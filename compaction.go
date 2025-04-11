package bitcask

import (
	"errors"
	"sort"
)

type Compaction struct {
	inputs []*Wal

	output     *Wal
	outputHint *HintWal

	edit *ManifestEdit
}

func NewCompaction(inputs []*Wal, outputFid uint64) (*Compaction, error) {
	dir := inputs[0].Dir()
	baseTime := inputs[0].BaseTime()
	deleteFiles := make([]LogFile, len(inputs))
	for idx := range inputs {
		inputs[idx].Ref()
		baseTime = min(baseTime, inputs[idx].BaseTime())
		deleteFiles[idx] = LogFile{
			wal: inputs[idx],
			fid: inputs[idx].Fid(),
		}
	}

	outputWal, err := NewWal(MergePath(dir, outputFid), outputFid, int64(baseTime))
	if err != nil {
		return nil, err
	}

	outputHint, err := NewHint(TmpPath(dir, outputFid), outputFid, int64(baseTime))
	if err != nil {
		return nil, err
	}

	edit := &ManifestEdit{
		addFiles:    []LogFile{{outputWal, outputFid}},
		deleteFiles: deleteFiles,
		hasNextFid:  true,
		nextFid:     outputFid + 1,
	}

	return &Compaction{
		inputs:     inputs,
		output:     outputWal,
		outputHint: outputHint,
		edit:       edit,
	}, nil
}

func (c *Compaction) Finalize() error {
	walName := WalFilename(c.output.Fid())
	hintName := HintFilename(c.output.Fid())

	if err := c.output.Rename(walName); err != nil {
		return err
	}

	return c.outputHint.wal.Rename(hintName)
}

func (c *Compaction) Destroy() {
	c.output.Unref()

	for idx := range c.inputs {
		c.inputs[idx].Unref()
	}
}

func DefaultCompactionPicker(wals []PickerWalInfo) []uint64 {
	// reverse order
	sort.Slice(wals, func(i, j int) bool {
		return wals[i].FreeBytes > wals[j].FreeBytes
	})

	var res []uint64
	for idx := range wals {
		size := float64(wals[idx].WalSize)
		free := float64(wals[idx].FreeBytes)

		if free/size < CompactionPickerRatio {
			break
		}

		res = append(res, wals[idx].Fid)
		if len(res) >= 2 {
			break
		}
	}

	return res
}

func (db *DBImpl) maybeScheduleCompaction() {
	if db.bgErr != nil {
		return
	}

	if !db.compacting.CompareAndSwap(false, true) {
		return
	}

	// only one reach here

	db.mu.Lock()
	defer db.mu.Unlock()

	candidateWals := make([]PickerWalInfo, 0, len(db.manifest.wals))
	for fid := range db.manifest.wals {
		// skip the active wal
		if fid == db.manifest.active.fid {
			continue
		}

		candidateWals = append(candidateWals, PickerWalInfo{
			Fid:        fid,
			WalSize:    db.manifest.wals[fid].wal.Size(),
			CreateTime: db.manifest.wals[fid].wal.CreateTime(),
			FreeBytes:  db.manifest.wals[fid].freeBytes + db.manifest.wals[fid].deltaFreeBytes,
		})
	}

	filterdWals := db.opts.CompactionPicker(candidateWals)
	if len(filterdWals) == 0 {
		db.compacting.Store(false)
		return
	}

	db.backgroundCompaction(filterdWals)
}

func (db *DBImpl) backgroundCompaction(wals []uint64) {
	inputs := make([]*Wal, len(wals))
	for idx := range wals {
		inputs[idx] = db.manifest.wals[wals[idx]].wal
	}

	compaction, err := NewCompaction(inputs, db.manifest.nextFid)
	if err != nil {
		db.bgErr = err
		db.compacting.Store(false)
		return
	}

	// run compaction without any lock
	go db.doCompactionWork(compaction)
}

func (db *DBImpl) doCompactionWork(compaction *Compaction) {
	defer func() {
		db.compacting.Store(false)
		compaction.Destroy()
	}()

	for idx := range compaction.inputs {
		if err := db.compactOneWal(
			compaction.output, compaction.outputHint, compaction.inputs[idx],
		); err != nil {
			db.bgErr = err
			return
		}
	}

	if err := compaction.Finalize(); err != nil {
		db.bgErr = err
		return
	}

	// apply the edit
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.manifest.LogAndApply(compaction.edit); err != nil {
		db.bgErr = err
		return
	}

	// update the index and ignore any error
	_ = IterateHint(compaction.outputHint, func(ns, key []byte, fid, off, sz uint64) error {
		return db.index.Put(ns, key, fid, off, sz, nil)
	})
}

func (db *DBImpl) compactOneWal(dst *Wal, dstHint *HintWal, src *Wal) error {
	it := NewWalIterator(src)
	defer it.Close()

	for {
		foff, recordBytes, err := it.Next()
		if err != nil {
			if errors.Is(err, ErrWalIteratorEOF) {
				break
			}
			return err
		}

		record, err := RecordFromBytes(recordBytes, src.CreateTime())
		if err != nil {
			return err
		}

		if db.doFilter(record, src.fid, foff) {
			continue
		}

		newRecord, err := record.Encode(dst.CreateTime())
		if err != nil {
			return err
		}

		// write dst wal
		foff, err = dst.WriteRecord(newRecord)
		if err != nil {
			return err
		}

		// write dst hint wal
		hintRecord := &HintRecord{
			ns:   record.Ns,
			key:  record.Key,
			fid:  dst.Fid(),
			off:  foff,
			size: uint64(len(newRecord)),
		}
		if err = dstHint.AppendRecord(hintRecord); err != nil {
			return err
		}
	}

	return nil
}

func (db *DBImpl) doFilter(srcRecord *Record, srcFid, srcOff uint64) bool {
	fid, off, _, err := db.index.Get(srcRecord.Ns, srcRecord.Key)
	if err != nil { // the key has been deleted or evicted
		return true
	}

	if fid != srcFid || off != srcOff { // the key has been updated
		return true
	}

	if db.opts.CompactionFilter != nil {
		if db.opts.CompactionFilter(srcRecord.Ns, srcRecord.Key, srcRecord.Value, srcRecord.Meta) {
			// compaction filter failed, the key should be deleted
			return true
		}
	}

	// the key should be retained
	return false
}
