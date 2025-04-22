package bitcask

import (
	"errors"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/flock"
)

var (
	ErrLockDB         = errors.New("lock database")
	ErrLoadManifest   = errors.New("load manifest file")
	ErrCleanDB        = errors.New("clean database")
	ErrRecoverDB      = errors.New("recover database")
	ErrNewIndex       = errors.New("new index")
	ErrDiskOutOfLimit = errors.New("disk out of limit")
)

type writer struct {
	cond  *sync.Cond
	batch *Batch

	err   error
	done  bool
	flush bool
}

type DBImpl struct {
	fileLock *flock.Flock
	mu       sync.Mutex

	opts *Options
	stop chan bool

	randor  *rand.Rand
	index   *Index
	writers *Deque[*writer]

	manifest *Manifest
	bgErr    error

	compacting *atomic.Bool
	compaction *Compaction

	reclaiming *atomic.Bool

	wallTime *atomic.Int64

	// hint wal file is immutable
	// the cache used to estimate total size of database
	hintSizeCache map[uint64]int64

	// the record usually a short lived object. so here a pool is used
	// to ease the pressure on the golang garbage collection
	recordPool sync.Pool

	// the writer usually a short lived object. its lifetime is as long
	// as the write request
	writerPool sync.Pool
}

func NewDB(opts *Options) (*DBImpl, error) {
	opts.Init()

	fileLock := flock.New(LockPath(opts.Dir))
	hold, err := fileLock.TryLock()
	if err != nil || !hold {
		return nil, ErrLockDB
	}

	manifest, err := NewManifestIfNotExists(opts.Dir)
	if err != nil {
		return nil, errors.Join(err, ErrLoadManifest)
	}

	if err = manifest.CleanFiles(true); err != nil {
		return nil, errors.Join(err, ErrCleanDB)
	}

	randor := rand.New(rand.NewSource(time.Now().Unix()))

	dbImpl := &DBImpl{
		opts:          opts,
		fileLock:      fileLock,
		randor:        randor,
		stop:          make(chan bool),
		writers:       NewDeque[*writer](),
		manifest:      manifest,
		bgErr:         nil,
		compacting:    new(atomic.Bool),
		reclaiming:    new(atomic.Bool),
		wallTime:      new(atomic.Int64),
		hintSizeCache: make(map[uint64]int64),
		recordPool: sync.Pool{
			New: func() any {
				b := make([]byte, opts.RecordBufferSize)
				return &b
			},
		},
		writerPool: sync.Pool{
			New: func() any {
				return &writer{}
			},
		},
	}

	dbImpl.compacting.Store(false)
	dbImpl.reclaiming.Store(false)
	dbImpl.wallTime.Store(time.Now().Unix())

	indexOpts := &IndexOptions{
		Capacity:             opts.IndexCapacity,
		Limited:              opts.IndexLimited,
		EvictionPoolCapacity: opts.IndexEvictionPoolCapacity,
		SampleKeys:           opts.IndexSampleKeys,
		Helper:               dbImpl,
	}
	if dbImpl.index, err = NewIndex(indexOpts); err != nil {
		return nil, errors.Join(err, ErrNewIndex)
	}

	if err = dbImpl.recoverFromWals(); err != nil {
		return nil, errors.Join(err, ErrRecoverDB)
	}

	go dbImpl.doBackgroundTask()

	return dbImpl, nil
}

func (db *DBImpl) newWriter(batch *Batch, flush bool) *writer {
	writer, _ := db.writerPool.Get().(*writer)

	writer.cond = sync.NewCond(&db.mu)
	writer.batch = batch
	writer.err = nil
	writer.done = false
	writer.flush = flush

	return writer
}

func (db *DBImpl) recoverFromWals() error {
	wals := make([]uint64, 0, len(db.manifest.wals))
	for fid := range db.manifest.wals {
		wals = append(wals, fid)
	}

	slices.Sort(wals)

	// recover from older wals to newest wals
	for _, fid := range wals {
		if err := db.recoverFromWal(fid); err != nil {
			return err
		}
	}

	return nil
}

func (db *DBImpl) recoverFromWal(fid uint64) error {
	// prefer hint wal
	hintPath := HintPath(db.opts.Dir, fid)
	hint, err := LoadWal(hintPath, fid)
	if err == nil {
		defer hint.Close()

		// cache hint file size
		db.hintSizeCache[hint.Fid()] = int64(hint.Size())

		err = IterateHint(hint, func(record *HintRecord) error {
			return db.index.Put(record.ns, record.key, fid, record.off, record.size, nil)
		})
	}

	if err == nil {
		return nil
	}

	// use the original wal
	wal := db.manifest.wals[fid].wal
	return IterateRecord(wal, func(record *Record, foff, size uint64) error {
		// the foff points to the start offset of data in the wal
		// however, the offset used by ReadRecord of wal expects the start offset of data header
		foff -= RecordHeaderSize

		return db.index.Put(record.Ns, record.Key, wal.Fid(), foff, size, nil)
	})
}

func (db *DBImpl) doBackgroundTask() {
	checkDiskUsageInterval := int(GetOptions().CheckDiskUsageInterval)
	compactionTriggerInterval := int(GetOptions().CompactionTriggerInterval)

	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	tickNum := 0
	for {
		select {
		case <-db.stop:
			return
		case <-tick.C:
			tickNum++
			db.wallTime.Store(time.Now().Unix())
		}

		if tickNum%checkDiskUsageInterval == 0 && db.opts.DiskUsageLimited > 0 {
			db.reclaimDiskUsage(int64(db.opts.DiskUsageLimited))
		}

		if tickNum%compactionTriggerInterval == 0 && !db.opts.DisableCompaction {
			db.maybeScheduleCompaction()
		}
	}
}

func (db *DBImpl) Write(batch *Batch, opts *WriteOptions) error {
	if opts == nil {
		opts = &WriteOptions{Sync: false}
	}
	w := db.newWriter(batch, opts.Sync)
	defer db.writerPool.Put(w)

	db.mu.Lock()
	defer db.mu.Unlock()

	db.writers.PushBack(w)

	for !w.done {
		head, _ := db.writers.Front()
		if w == *head {
			break
		}

		w.cond.Wait()
	}

	if w.done {
		return w.err
	}

	// reach here: the writer is front, and other writers should wait

	lastWriter := w
	err := db.ensureRoomForWrite()
	if err == nil {
		var syncErr error
		batch := db.buildBatchGroup(&lastWriter)

		// release the lock
		db.mu.Unlock()

		// no race condition. the wal file only is written by front writer
		active := db.manifest.active
		locs, err := db.writeWal(active, batch)
		if err == nil && opts.Sync {
			if err = active.Sync(); err != nil {
				syncErr = err
			}
		}

		// index is thread-safe
		var writeStats map[uint64]uint64
		if err == nil {
			writeStats = db.writeIndex(batch, active.Fid(), locs)
		}

		db.mu.Lock()
		if syncErr != nil {
			db.bgErr = syncErr
		}

		if err == nil {
			// apply the manifest edit but don't persist
			if err = db.manifest.Apply(&ManifestEdit{freeBytes: writeStats}); err != nil {
				db.bgErr = err
			}
		}
	}

	for {
		front, _ := db.writers.Front()
		_ = db.writers.PopFront()

		if *front != w {
			(*front).err = err
			(*front).done = true
			(*front).cond.Signal()
		}

		if *front == lastWriter {
			break
		}
	}

	// notify new head of write queue
	if !db.writers.Empty() {
		front, _ := db.writers.Front()
		(*front).cond.Signal()
	}

	return err
}

// the index is always writable
func (db *DBImpl) writeIndex(batch *Batch, fid uint64, locs [][2]uint64) map[uint64]uint64 {
	writeStats := make(map[uint64]uint64)

	for idx := range batch.records {
		stat := &WriteStat{}
		record := batch.records[idx]

		switch {
		case record.Deleted:
			_ = db.index.Delete(record.Ns, record.Key, stat)
		case record.Meta.IsTombstone():
			_ = db.index.SoftDelete(record.Ns, record.Key, stat)
		default:
			_ = db.index.Put(record.Ns, record.Key, fid, locs[idx][0], locs[idx][1], stat)
		}

		writeStats[stat.FreeWalFid] += stat.FreeBytes
	}

	return writeStats
}

func (db *DBImpl) writeWal(active *Wal, batch *Batch) (locs [][2]uint64, err error) {
	var bin []byte
	var off uint64
	locs = make([][2]uint64, len(batch.records))

	bufPtr, _ := db.recordPool.Get().(*[]byte)
	defer db.recordPool.Put(bufPtr)

	for idx := range batch.records {
		if bin, err = batch.records[idx].Encode(*bufPtr, active.BaseTime()); err != nil {
			active.ResetBuffer()
			return
		}

		if off, err = active.WriteRecord(bin); err != nil {
			active.ResetBuffer()
			return
		}

		locs[idx][0] = off
		locs[idx][1] = uint64(len(bin))
	}

	return locs, active.Flush()
}

func (db *DBImpl) buildBatchGroup(lastWriter **writer) *Batch {
	first, _ := db.writers.Front()
	result := (*first).batch

	// allow the group to grow up to a maximum size, but if the
	// original write is small, limit the growth so we do not slow
	// down the small write too much
	size := result.ByteSize()
	maxSize := 1 << 20       // 1 MB
	if size <= (128 << 10) { // 128 KB
		maxSize = size + (128 << 10)
	}

	*lastWriter = *first
	tmpBatch := NewBatch()
	// advance past "first"
	for i := 1; i < db.writers.Len(); i++ {
		w, _ := db.writers.At(i)
		if (*w).flush && !(*first).flush {
			// don't include a sync write into a batch handled by a non-sync write
			break
		}

		if (*w).batch != nil {
			size += (*w).batch.ByteSize()
			if size > maxSize {
				// don't make batch too big
				break
			}

			// append to *result
			if result == (*first).batch {
				// switch to temporary batch instead of disturbing caller's batch
				result = tmpBatch
				result.Append((*first).batch)
			}

			result.Append((*w).batch)
		}
		*lastWriter = *w
	}

	return result
}

func (db *DBImpl) ensureRoomForWrite() error {
	if db.bgErr != nil {
		return db.bgErr
	}

	if db.manifest.active.Size() >= db.opts.WalMaxSize {
		old, err := db.manifest.RotateWal()
		if err != nil {
			db.bgErr = err
			return err
		}

		// when the wal rotates, the hint file is generated in background
		// don't need to care if it succeeds or not
		// if it's unsuccessful, it will be cleaned up automatically
		go func() {
			fileSize, err := NewHintByWal(old)
			if err == nil && fileSize != 0 {
				db.mu.Lock()
				db.hintSizeCache[old.Fid()] = int64(fileSize)
				db.mu.Unlock()
			}
		}()
	}

	if db.manifest.FileSize() >= db.opts.ManifestMaxSize {
		if err := db.manifest.RotateManifest(); err != nil {
			db.bgErr = err
			return err
		}
	}

	return nil
}

func (db *DBImpl) Get(ns, key []byte, opts *ReadOptions) (val []byte, meta *Meta, err error) {
	var wal *Wal
	var record *Record
	var recordBytes []byte

	fid, off, sz, err := db.index.Get(ns, key)
	if err != nil {
		return nil, nil, err
	}

	func() {
		db.mu.Lock()
		defer db.mu.Unlock()

		// increase the reference to avoid deletion
		wal = db.manifest.ToWalWithRef(fid)
	}()

	if wal == nil {
		return nil, nil, ErrKeyNotFound
	}

	defer wal.Unref()

	// reach here: read the wal file without any lock
	if opts == nil {
		opts = &ReadOptions{VerifyChecksum: true}
	}
	if recordBytes, err = wal.ReadRecord(off, sz, opts.VerifyChecksum); err != nil {
		return nil, nil, errors.Join(err, ErrKeyNotFound)
	}

	if record, err = RecordFromBytes(recordBytes, wal.BaseTime()); err != nil {
		return nil, nil, errors.Join(err, ErrKeyNotFound)
	}

	val = record.Value
	meta = record.Meta

	return
}

func (db *DBImpl) Put(ns, key, val []byte, meta *Meta, opts *WriteOptions) error {
	batch := NewBatch()
	batch.Put(ns, key, val, meta)
	return db.Write(batch, opts)
}

func (db *DBImpl) Delete(ns, key []byte, opts *WriteOptions) error {
	batch := NewBatch()
	batch.Delete(ns, key)
	return db.Write(batch, opts)
}

func (db *DBImpl) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()

	close(db.stop)
	db.manifest.Close()

	_ = db.fileLock.Unlock()
	db.fileLock.Close()
}

func (db *DBImpl) Rand(upper uint64) uint64 {
	return uint64(db.randor.Int63n(int64(upper)))
}

func (db *DBImpl) WallTime() time.Time {
	return time.Unix(db.wallTime.Load(), 0)
}
