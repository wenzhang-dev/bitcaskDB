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
	if c.output.Size() == SuperBlockSize {
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
	if c.hintWriter.Wal().Size() == SuperBlockSize {
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

	db.backgroundCompactionLocked(filterdWals)
}

func (db *DBImpl) backgroundCompactionLocked(wals []uint64) {
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

	db.compaction = compaction

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
			compaction.writer, compaction.hintWriter, compaction.inputs[idx],
		); err != nil {
			db.bgErr = err
			return
		}
	}

	if err := compaction.Finalize(); err != nil {
		db.bgErr = err
		return
	}

	func() {
		db.mu.Lock()
		defer db.mu.Unlock()

		// apply the edit
		if err := db.manifest.LogAndApply(compaction.edit); err != nil {
			db.bgErr = err
		}

		// clean un-used files
		_ = db.manifest.CleanFiles(false)

		db.compaction = nil
	}()

	if db.bgErr != nil {
		return
	}

	// reach here: execute without any lock
	// the compaction hold the wal reference until the function exits

	// update the index and ignore any error
	_ = IterateHint(compaction.hintWriter.Wal(), func(record *HintRecord) error {
		_ = db.index.Put(record.ns, record.key, record.fid, record.off, record.size, nil)
		return nil
	})
}

func (db *DBImpl) compactOneWal(dst *WalRewriter, hintWriter *HintWriter, src *Wal) error {
	return IterateRecord(src, func(record *Record, foff, _ uint64) error {
		// the foff points to the start offset of data in the wal
		// however, the offset used by ReadRecord of wal expects the start offset of data header
		foff -= RecordHeaderSize

		if db.doFilter(record, src.fid, foff) {
			return nil
		}

		recordBytes, err := record.Encode(dst.Wal().BaseTime())
		if err != nil {
			return err
		}

		// write dst wal
		if foff, err = dst.AppendRecord(recordBytes); err != nil {
			return err
		}

		// write dst hint wal
		hintRecord := &HintRecord{
			ns:   record.Ns,
			key:  record.Key,
			fid:  dst.Wal().Fid(),
			off:  foff,
			size: uint64(len(recordBytes)),
		}
		return hintWriter.AppendRecord(hintRecord)
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
	db.mu.Lock()
	defer db.mu.Unlock()

	usage, err := db.approximateDiskUsageLocked()
	if err != nil {
		db.bgErr = errors.Join(err, ErrDiskOutOfLimit)
		return
	}

	if usage <= expect {
		return
	}

	compactionWals := db.getCompactionWalsLocked()
	files := make([]LogFile, len(db.manifest.wals))
	for fid := range db.manifest.wals {
		// exclude the compation wals
		if slices.Contains(compactionWals, fid) {
			continue
		}

		files = append(files, LogFile{
			fid: fid,
			wal: db.manifest.wals[fid].wal,
		})
	}

	// sort by create time in positive order
	sort.Slice(files, func(i int, j int) bool {
		return files[i].wal.CreateTime() < files[j].wal.CreateTime()
	})

	idx := 0
	var deleteFiles []LogFile

	// reclaim the old wals
	for usage > expect && idx < len(files) {
		usage -= int64(files[idx].wal.Size())
		deleteFiles = append(deleteFiles, files[idx])

		idx++
	}

	if len(deleteFiles) == 0 {
		db.bgErr = ErrDiskOutOfLimit
		return
	}

	// apply the edit
	edit := &ManifestEdit{
		deleteFiles: deleteFiles,
	}

	if err = db.manifest.LogAndApply(edit); err != nil {
		db.bgErr = errors.Join(err, ErrDiskOutOfLimit)
	}
}

// the method estimates total size of database
// warning: the return size includes the total size of database reference files
func (db *DBImpl) approximateDiskUsageLocked() (int64, error) {
	var usage int64

	fileSize := func(path string) int64 {
		fi, err := os.Stat(path)
		if err != nil {
			return 0
		}
		return fi.Size()
	}

	// manifest file size
	usage += int64(db.manifest.FileSize())

	// hint and wal file size
	for fid, info := range db.manifest.wals {
		usage += int64(info.wal.Size())

		hintSize, exists := db.hintSizeCache[fid]
		if !exists {
			hintSize = fileSize(HintPath(db.manifest.dir, fid))
			if hintSize != 0 {
				db.hintSizeCache[fid] = hintSize
			}
		}
		usage += hintSize
	}

	// remove the un-used hint cache items
	for fid := range db.hintSizeCache {
		if _, exists := db.manifest.wals[fid]; !exists {
			delete(db.hintSizeCache, fid)
		}
	}

	return usage, nil
}
