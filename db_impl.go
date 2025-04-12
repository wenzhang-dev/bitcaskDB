package bitcask

import (
	"errors"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/flock"
)

var (
	ErrLockDB       = errors.New("lock database")
	ErrLoadManifest = errors.New("load manifest file")
	ErrCleanDB      = errors.New("clean database")
	ErrRecoverDB    = errors.New("recover database")
	ErrNewIndex     = errors.New("new index")
)

type writer struct {
	cond  *sync.Cond
	batch *Batch

	err   error
	done  bool
	flush bool
}

func newWriter(batch *Batch, mux *sync.Mutex, flush bool) *writer {
	return &writer{
		cond:  sync.NewCond(mux),
		batch: batch,
		err:   nil,
		done:  false,
		flush: flush,
	}
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
	wallTime   *atomic.Int64
}

func NewDB(opts *Options) (*DBImpl, error) {
	fileLock := flock.New(LockPath(opts.Dir))
	hold, err := fileLock.TryLock()
	if err != nil || !hold {
		return nil, ErrLockDB
	}

	manifest, err := NewManifestIfNotExists(opts.Dir)
	if err != nil {
		return nil, errors.Join(err, ErrLoadManifest)
	}

	if err = manifest.CleanFiles(); err != nil {
		return nil, errors.Join(err, ErrCleanDB)
	}

	randor := rand.New(rand.NewSource(time.Now().Unix()))

	if opts.CompactionPicker == nil {
		opts.CompactionPicker = DefaultCompactionPicker
	}

	dbImpl := &DBImpl{
		opts:       opts,
		fileLock:   fileLock,
		randor:     randor,
		stop:       make(chan bool),
		writers:    NewDeque[*writer](),
		manifest:   manifest,
		bgErr:      nil,
		compacting: new(atomic.Bool),
		wallTime:   new(atomic.Int64),
	}

	dbImpl.compacting.Store(false)
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

func (db *DBImpl) recoverFromWals() error {
	wals := make([]uint64, 0, len(db.manifest.wals))
	for fid := range db.manifest.wals {
		wals = append(wals, fid)
	}

	// positive order
	sort.Slice(wals, func(i int, j int) bool {
		return wals[i] < wals[j]
	})

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
	err := RecoverFromHint(hintPath, fid, func(ns, key []byte, fid, off, sz uint64) error {
		return db.index.Put(ns, key, fid, off, sz, nil)
	})

	if err == nil {
		return nil
	}

	// use wal
	wal := db.manifest.wals[fid].wal
	it := NewWalIterator(wal)
	defer it.Close()

	for {
		foff, recordBytes, err := it.Next()
		if err != nil {
			if errors.Is(err, ErrWalIteratorEOF) {
				break
			}
			return err
		}

		record, err := RecordFromBytes(recordBytes, wal.BaseTime())
		if err != nil {
			return err
		}

		// the offset returned by the Next method of wal iterator points to the actual start of the data
		// however, the offset used by ReadRecord of wal is the start of the data header
		// therefore, the offset is corrected here
		foff -= RecordHeaderSize
		err = db.index.Put(record.Ns, record.Key, wal.Fid(), foff, uint64(len(recordBytes)), nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DBImpl) doBackgroundTask() {
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

		if tickNum%CompactionTriggerInterval == 0 {
			db.maybeScheduleCompaction()
		}
	}
}

func (db *DBImpl) Write(batch *Batch, opts *WriteOptions) error {
	w := newWriter(batch, &db.mu, opts.Sync)

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
		offs, sizes, err := db.writeWal(active, batch)
		if err == nil && opts.Sync {
			if err = active.Sync(); err != nil {
				syncErr = err
			}
		}

		// index is thread-safe
		var writeStats map[uint64]uint64
		if err == nil {
			writeStats = db.writeIndex(batch, active.Fid(), offs, sizes)
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
func (db *DBImpl) writeIndex(batch *Batch, fid uint64, offs []uint64, sizes []uint64) map[uint64]uint64 {
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
			_ = db.index.Put(record.Ns, record.Key, fid, offs[idx], sizes[idx], stat)
		}

		writeStats[stat.FreeWalFid] += stat.FreeBytes
	}

	return writeStats
}

func (db *DBImpl) writeWal(active *Wal, batch *Batch) (offs []uint64, sizes []uint64, err error) {
	var bin []byte
	var off uint64
	offs = make([]uint64, len(batch.records))
	sizes = make([]uint64, len(batch.records))

	for idx := range batch.records {
		bin, err = batch.records[idx].Encode(active.BaseTime())
		if err != nil {
			active.ResetBuffer()
			return
		}

		sizes[idx] = uint64(len(bin))
		off, err = active.WriteRecord(bin)
		if err != nil {
			active.ResetBuffer()
			return
		}

		offs[idx] = off
	}

	return offs, sizes, active.Flush()
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
			_ = NewHintByWal(old)
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
	var record *Record
	var recordBytes []byte

	fid, off, sz, err := db.index.Get(ns, key)
	if err != nil {
		return nil, nil, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	wal := db.manifest.ToWal(fid)
	if wal == nil {
		return nil, nil, ErrKeyNotFound
	}

	// increase the reference to avoid deletion
	wal.Ref()
	db.mu.Unlock()

	// reach here: read the wal file without any lock
	recordBytes, err = wal.ReadRecord(off, sz)
	if err == nil {
		record, err = RecordFromBytes(recordBytes, wal.BaseTime())
		if err == nil {
			val = record.Value
			meta = record.Meta
		}
	}

	if err != nil {
		err = errors.Join(err, ErrKeyNotFound)
	}

	db.mu.Lock()
	wal.Unref()

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
