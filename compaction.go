package bitcask

import (
	"errors"
	"os"
	"slices"
	"sort"
	"sync"
)

type Compaction struct {
	inputs []*Wal

	output     *Wal
	writer     *WalRewriter
	hintWriter *HintWriter

	edit *ManifestEdit

	mu sync.RWMutex
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

	writer, err := NewHintWriter(TmpPath(dir, outputFid), outputFid, int64(baseTime))
	if err != nil {
		return nil, err
	}

	// the compaction may generate an empty wal, which don't have to keep it
	edit := &ManifestEdit{
		addFiles:    nil,
		deleteFiles: deleteFiles,
		hasNextFid:  true,
		nextFid:     outputFid + 1,
	}

	return &Compaction{
		inputs:     inputs,
		output:     outputWal,
		hintWriter: writer,
		writer:     NewWalRewriter(outputWal, 1024*1024), // 1MB
		edit:       edit,
	}, nil
}

func (c *Compaction) Finalize() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error
	if err = c.writer.Flush(); err != nil {
		return err
	}

	if err = c.hintWriter.Flush(); err != nil {
		return err
	}

	// corner case: empty output wal
	// otherwise, we should add the output wal to manifest
	if c.output.Empty() {
		return nil
	}

	c.edit.addFiles = append(c.edit.addFiles, LogFile{
		wal: c.output,
		fid: c.output.Fid(),
	})

	walName := WalFilename(c.output.Fid())
	hintName := HintFilename(c.output.Fid())

	if err = c.output.Rename(walName); err != nil {
		return err
	}

	return c.hintWriter.Wal().Rename(hintName)
}

func (c *Compaction) Destroy() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// corner case: empty hint file
	if c.hintWriter.Wal().Empty() {
		c.hintWriter.Wal().Unref()
	}

	_ = c.hintWriter.Close()
	_ = c.writer.Close()

	c.output.Unref()

	for idx := range c.inputs {
		c.inputs[idx].Unref()
	}
}

func (db *DBImpl) maybeScheduleCompaction() {
	if db.reclaiming.Load() {
		return
	}

	if !db.compacting.CompareAndSwap(false, true) {
		return
	}

	// only one reach here

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.bgErr != nil {
		db.compacting.Store(false)
		return
	}

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

	db.backgroundCompactionLocked(filterdWals)
}

func (db *DBImpl) backgroundCompactionLocked(wals []uint64) {
	inputs := make([]*Wal, len(wals))
	for idx := range wals {
		inputs[idx] = db.manifest.wals[wals[idx]].wal
	}

	fid := db.manifest.GenFid()
	compaction, err := NewCompaction(inputs, fid)
	if err != nil {
		db.bgErr = err
		db.compacting.Store(false)
		return
	}

	db.compaction = compaction

	db.logger.Info().Uints64("input wals", wals).Uint64("output wal", fid).Msg("new compaction")

	// run compaction without any lock
	go db.doCompaction(compaction)
}

func (db *DBImpl) doCompaction(compaction *Compaction) {
	var err error

	defer func() {
		db.compacting.Store(false)
		compaction.Destroy()
	}()

	if err = db.doCompactionWork(compaction); err == nil {
		db.logger.Info().Msg("compaction finished")
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	db.bgErr = err

	db.logger.Err(err).Msg("failed compaction")
}

func (db *DBImpl) doCompactionWork(compaction *Compaction) error {
	var err error
	for idx := range compaction.inputs {
		if err = db.compactOneWal(
			compaction.writer, compaction.hintWriter, compaction.inputs[idx],
		); err != nil {
			return err
		}

		db.logger.Info().Uint64("wal", compaction.inputs[idx].Fid()).Msg("part compaction")
	}

	if err = compaction.Finalize(); err != nil {
		return err
	}

	db.logger.Info().Msg("prepare to submit the compaction")

	// here, we should update the manifest and index synchronously and atomicly
	// otherwise, whether the index or manifest update first, the query will not find the
	// related wal, and return key not found
	//
	// at the same time, we don't want to hold the mutex for a long time, especially updating
	// the index via hint wal, which maybe time consuming
	var txn *ManifestTxn
	onePhase := func() error {
		var err error

		db.mu.Lock()
		txn, err = db.manifest.NewTxn()
		db.mu.Unlock()

		if err != nil {
			return err
		}

		// let the edit visible
		edit := &ManifestEdit{
			addFiles:   compaction.edit.addFiles,
			hasNextFid: compaction.edit.hasNextFid,
			nextFid:    compaction.edit.nextFid,
		}
		txn.Apply(edit)
		db.logger.Info().Msg("one phase: apply the edit")

		// update the index without any lock and ignore any error
		// FIXME: put operations may evict some keys, we should put it into an edit
		_ = IterateHint(compaction.hintWriter.Wal(), func(record *HintRecord) error {
			_ = db.index.Put(record.ns, record.key, record.fid, record.off, record.size, nil)
			return nil
		})
		db.logger.Info().Msg("one phase: gen hint wal")
		return nil
	}

	twoPhase := func() error {
		db.mu.Lock()
		defer db.mu.Unlock()

		// commit the txn
		edit := &ManifestEdit{
			deleteFiles: compaction.edit.deleteFiles,
		}
		if err := txn.Commit(edit); err != nil {
			return err
		}

		db.logger.Info().Msg("two phase: commit the txn")

		// clean un-used files
		_ = db.manifest.CleanFiles(false)

		db.compaction = nil

		// cache the hint file size
		hintWal := compaction.hintWriter.Wal()
		db.hintSizeCache[hintWal.Fid()] = int64(hintWal.Size())

		for idx := range compaction.edit.deleteFiles {
			logFile := compaction.edit.deleteFiles[idx]
			delete(db.hintSizeCache, logFile.fid)
		}

		return nil
	}

	if err = onePhase(); err != nil {
		return err
	}

	return twoPhase()
}

func (db *DBImpl) compactOneWal(dst *WalRewriter, hintWriter *HintWriter, src *Wal) error {
	bufPtr, _ := db.recordPool.Get().(*[]byte)
	defer db.recordPool.Put(bufPtr)

	var hintRecord HintRecord
	return IterateRecord(src, func(record *Record, foff, _ uint64) error {
		// the foff points to the start offset of data in the wal
		// however, the offset used by ReadRecord of wal expects the start offset of data header
		foff -= RecordHeaderSize

		if db.doFilter(record, src.fid, foff) {
			return nil
		}

		recordBytes, err := record.Encode(*bufPtr, dst.Wal().BaseTime())
		if err != nil {
			return err
		}

		// write dst wal
		if foff, err = dst.AppendRecord(recordBytes); err != nil {
			return err
		}

		// write dst hint wal
		hintRecord.ns = record.Ns
		hintRecord.key = record.Key
		hintRecord.fid = dst.Wal().Fid()
		hintRecord.off = foff
		hintRecord.size = uint64(len(recordBytes))

		return hintWriter.AppendRecord(&hintRecord)
	})
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

func (db *DBImpl) getCompactionWalsLocked() []uint64 {
	if !db.compacting.Load() || db.compaction == nil {
		return nil
	}

	c := db.compaction
	c.mu.RLock()
	defer c.mu.RUnlock()

	wals := make([]uint64, 0, len(c.inputs)+1)
	wals = append(wals, c.output.Fid())

	for idx := range c.inputs {
		wals = append(wals, c.inputs[idx].Fid())
	}

	return wals
}

func (db *DBImpl) reclaimDiskUsage(expect int64) {
	if !db.reclaiming.CompareAndSwap(false, true) {
		return
	}

	// only one reach here

	defer func() {
		db.reclaiming.Store(false)
	}()

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.bgErr != nil {
		return
	}

	usage, err := db.approximateDiskUsageLocked()
	if err != nil {
		db.bgErr = errors.Join(err, ErrDiskOutOfLimit)
		return
	}

	db.logger.Info().Int64("expect", expect).Int64("usage", usage).Msg("reclaim disk usage")
	if usage <= expect {
		return
	}

	compactionWals := db.getCompactionWalsLocked()
	files := make([]LogFile, 0, len(db.manifest.wals))
	for fid := range db.manifest.wals {
		// exclude the compation wals
		if slices.Contains(compactionWals, fid) {
			continue
		}

		// skip the active wal
		if fid == db.manifest.ActiveWal().Fid() {
			continue
		}

		files = append(files, LogFile{
			fid: fid,
			wal: db.manifest.wals[fid].wal,
		})
	}

	db.mu.Unlock()

	// sort by create time in positive order
	sort.Slice(files, func(i, j int) bool {
		return files[i].wal.CreateTime() < files[j].wal.CreateTime()
	})

	idx := 0
	deleteFiles := make([]LogFile, 0, 3)

	// reclaim the old wals
	for usage > expect && idx < len(files) {
		usage -= int64(files[idx].wal.Size())
		deleteFiles = append(deleteFiles, files[idx])

		idx++
	}

	db.logger.Info().Uints64("wals", Map(deleteFiles, func(f LogFile) uint64 {
		return f.fid
	})).Msg("prepare to reclaim wals")

	db.mu.Lock()

	if len(deleteFiles) == 0 {
		db.bgErr = ErrDiskOutOfLimit
		db.logger.Err(db.bgErr).Msg("failed to reclaim disk usage")
		return
	}

	// apply the edit
	edit := &ManifestEdit{
		deleteFiles: deleteFiles,
	}

	if err = db.manifest.LogAndApply(edit); err != nil {
		db.bgErr = errors.Join(err, ErrDiskOutOfLimit)
		db.logger.Err(db.bgErr).Msg("failed to apply")
	}

	db.logger.Info().Msg("reclaim successfully")

	// delete the related hint wals
	for idx := range deleteFiles {
		// ignore errors
		delete(db.hintSizeCache, deleteFiles[idx].fid)
		_ = os.Remove(HintPath(db.opts.Dir, deleteFiles[idx].fid))
	}
}

// the method estimates total size of database
// warning: the return size includes the total size of database reference files
func (db *DBImpl) approximateDiskUsageLocked() (int64, error) {
	var usage int64

	// manifest file size
	usage += int64(db.manifest.FileSize())

	// hint and wal file size
	for fid, info := range db.manifest.wals {
		usage += int64(info.wal.Size())
		usage += db.hintSizeCache[fid]
	}

	// remove the un-used hint cache items
	for fid := range db.hintSizeCache {
		if _, exists := db.manifest.wals[fid]; !exists {
			delete(db.hintSizeCache, fid)
		}
	}

	return usage, nil
}
