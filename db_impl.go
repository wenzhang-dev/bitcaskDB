package bitcask

import (
	"errors"
	"math/rand"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/flock"

	"github.com/rs/zerolog"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/wenzhang-dev/bitcaskDB/block_reader"
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

type reader struct {
	cond *sync.Cond

	// file descriptor number
	fd int

	// wal file id
	fid uint64

	off  uint64
	size uint64

	err  error
	done bool

	val  []byte
	meta *Meta

	firstBlkIdx    uint64
	firstBlkOffset uint64
	blks           [][]byte
}

type DBImpl struct {
	fileLock *flock.Flock
	mu       sync.Mutex

	opts *Options
	stop chan bool

	randor  *rand.Rand
	index   *Index
	writers *Deque[*writer]

	rdMu          sync.Mutex
	readers       *Deque[*reader]
	blockCache    *BlockCache
	blockReader   block_reader.BlockReader
	blockRequests block_reader.Requests

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
	readerPool sync.Pool

	logger *zerolog.Logger
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
		readers:       NewDeque[*reader](),
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
		readerPool: sync.Pool{
			New: func() any {
				return &reader{}
			},
		},
	}

	dbImpl.compacting.Store(false)
	dbImpl.reclaiming.Store(false)
	dbImpl.wallTime.Store(time.Now().Unix())
	dbImpl.initLogger()

	dbImpl.logger.Info().Msg("database bootstrap")

	indexOpts := &IndexOptions{
		Capacity:             opts.IndexCapacity,
		Limited:              opts.IndexLimited,
		EvictionPoolCapacity: opts.IndexEvictionPoolCapacity,
		SampleKeys:           opts.IndexSampleKeys,
		Helper:               dbImpl,
	}

	dbImpl.logger.Info().Msg("init index")
	if dbImpl.index, err = NewIndex(indexOpts); err != nil {
		dbImpl.logger.Err(err).Msg("failed to init index")
		return nil, errors.Join(err, ErrNewIndex)
	}

	dbImpl.logger.Info().Msg("init block reader")
	if dbImpl.blockReader, err = block_reader.NewDefaultBlockReader(opts.BlockReaderConcurrent); err != nil {
		dbImpl.logger.Err(err).Msg("failed to init block reader")
		return nil, err
	}
	dbImpl.blockRequests = make(block_reader.Requests, opts.BlockReaderConcurrent)

	blockCacheOpt := &BlockCacheOptions{
		Capacity:             opts.BlockCacheCapacity,
		Limited:              opts.BlockCacheLimited,
		EvictionPoolCapacity: opts.BlockCacheEvictionPoolCapacity,
		SampleKeys:           opts.BlockCacheSampleKeys,
		Helper:               dbImpl,
	}
	dbImpl.logger.Info().Msg("init block cache")
	if dbImpl.blockCache, err = NewBlockCache(blockCacheOpt); err != nil {
		dbImpl.logger.Err(err).Msg("failed to init block cache")
		return nil, err
	}

	dbImpl.logger.Info().Msg("recover wals")
	if err = dbImpl.recoverFromWals(); err != nil {
		dbImpl.logger.Err(err).Msg("failed to recover wals")
		return nil, errors.Join(err, ErrRecoverDB)
	}

	dbImpl.logger.Info().Msg("start background task")
	go dbImpl.doBackgroundTask()

	return dbImpl, nil
}

func (db *DBImpl) initLogger() {
	file := &lumberjack.Logger{
		Filename:   filepath.Join(db.opts.LogDir, db.opts.LogFile),
		MaxSize:    int(db.opts.LogMaxSize),
		MaxBackups: int(db.opts.LogMaxBackups),
		Compress:   false,
	}

	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		return filepath.Base(file) + ":" + strconv.Itoa(line)
	}

	w := zerolog.ConsoleWriter{
		NoColor:    true,
		Out:        file,
		TimeFormat: "2006-01-02 15:04:05",
	}

	logger := zerolog.New(w).With().Timestamp().Caller().Logger().Level(
		zerolog.Level(db.opts.LogLevel),
	)

	db.logger = &logger
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

func (db *DBImpl) newReader(fd int, fid, off, size, blkNum, firstBlkIdx, firstBlkOff uint64) *reader {
	reader, _ := db.readerPool.Get().(*reader)

	reader.cond = sync.NewCond(&db.rdMu)
	reader.fd = fd
	reader.fid = fid
	reader.off = off
	reader.size = size

	reader.done = false
	reader.err = nil

	reader.val = nil
	reader.meta = nil

	reader.firstBlkIdx = firstBlkIdx
	reader.firstBlkOffset = firstBlkOff
	reader.blks = make([][]byte, blkNum)

	return reader
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
	checkDiskUsageInterval := int(db.opts.CheckDiskUsageInterval)
	compactionTriggerInterval := int(db.opts.CompactionTriggerInterval)

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
			go db.reclaimDiskUsage(int64(db.opts.DiskUsageLimited))
		}

		if tickNum%compactionTriggerInterval == 0 && !db.opts.DisableCompaction {
			go db.maybeScheduleCompaction()
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
				db.logger.Err(err).Msg("failed to sync wal")
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
				db.logger.Err(err).Msg("failed to apply the write stats")
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
			db.logger.Err(err).Msg("failed to rotate wal")
			return err
		}

		db.logger.Info().Uint64("new", db.manifest.active.Fid()).Msg("rotate wal")

		// when the wal rotates, the hint file is generated in background
		// don't need to care if it succeeds or not
		// if it's unsuccessful, it will be cleaned up automatically
		go func() {
			fileSize, err := NewHintByWal(old)
			if err == nil && fileSize > SuperBlockSize {
				db.mu.Lock()
				db.hintSizeCache[old.Fid()] = int64(fileSize)
				db.mu.Unlock()
			}
		}()
	}

	if db.manifest.FileSize() >= db.opts.ManifestMaxSize {
		if err := db.manifest.RotateManifest(); err != nil {
			db.bgErr = err
			db.logger.Err(err).Msg("failed to rotate manifest")
			return err
		}
		db.logger.Info().Uint64("new", db.manifest.fid).Msg("rotate manifest")
	}

	return nil
}

func (db *DBImpl) Get(ns, key []byte, opts *ReadOptions) ([]byte, *Meta, error) {
	fid, off, sz, err := db.index.Get(ns, key)
	if err != nil {
		return nil, nil, err
	}

	// increase the reference to avoid deletion
	wal := db.getWalRef(fid)
	if wal == nil {
		return nil, nil, ErrKeyNotFound
	}
	defer wal.Unref()

	// reach here: read the wal file without any lock
	verifyChecksum := false
	if opts != nil && opts.VerifyChecksum {
		verifyChecksum = true
	}

	return db.readRecord(wal, off, sz, verifyChecksum)
}

func (db *DBImpl) getWalRef(fid uint64) *Wal {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.manifest.ToWalWithRef(fid)
}

func (db *DBImpl) parseRecord(wal *Wal, size uint64, blkOff uint64, blks [][]byte, verifyChecksum bool) ([]byte, *Meta, error) {
	recordBytes, err := WalParseRecord(size, blkOff, blks, verifyChecksum)
	if err != nil {
		return nil, nil, err
	}

	if record, err := RecordFromBytes(recordBytes, wal.BaseTime()); err != nil {
		return nil, nil, err
	} else {
		return record.Value, record.Meta, nil
	}
}

func (db *DBImpl) readRecord(wal *Wal, offset, size uint64, verifyChecksum bool) ([]byte, *Meta, error) {
	recordBytes, err := wal.ReadRecord(offset, size, verifyChecksum)
	if err != nil {
		return nil, nil, errors.Join(err, ErrKeyNotFound)
	}

	record, err := RecordFromBytes(recordBytes, wal.BaseTime())
	if err != nil {
		return nil, nil, errors.Join(err, ErrKeyNotFound)
	}

	return record.Value, record.Meta, nil
}

func (db *DBImpl) setCacheBlocks(reader *reader) int {
	missBlks := 0

	for idx := range reader.blks {
		blk, err := db.blockCache.Get(reader.fid, reader.firstBlkIdx+uint64(idx))
		if err != nil {
			missBlks++
		} else {
			reader.blks[idx] = blk
		}
	}

	return missBlks
}

func (db *DBImpl) buildBlockRequests(lastReader **reader) block_reader.Requests {
	curReqNum := 0
	*lastReader = nil

	for i := 0; i < db.readers.Len(); i++ {
		r, _ := db.readers.At(i)

		missBlks := db.setCacheBlocks(*r)
		if missBlks == 0 {
			*lastReader = *r
			continue
		}

		if missBlks+curReqNum > int(db.opts.BlockReaderConcurrent) {
			break
		}

		deltaReqNum := 0
		for idx := range (*r).blks {
			if (*r).blks[idx] != nil {
				continue
			}

			fd := (*r).fd
			fid := (*r).fid
			offset := WalBlockOffset((*r).firstBlkIdx + uint64(idx))
			if _, found := db.blockRequests[:curReqNum].BinarySearch((*r).fid, offset); !found {
				req := db.blockReader.NewRequest(fd, fid, offset, db.blockCache.NewBlock())
				db.blockRequests[curReqNum+deltaReqNum] = req
				deltaReqNum++
			}
		}

		curReqNum += deltaReqNum

		db.blockRequests[:curReqNum].Sort()
		*lastReader = *r
	}

	return db.blockRequests[:curReqNum]
}

func (db *DBImpl) setReaderResult(wal *Wal, reader *reader, reqs block_reader.Requests, verifyChecksum bool) {
	for idx := range reader.blks {
		// hit the block cache
		if reader.blks[idx] != nil {
			continue
		}

		offset := WalBlockOffset(reader.firstBlkIdx + uint64(idx))
		req, found := reqs.BinarySearch(reader.fid, offset)
		if !found {
			reader.err = errors.Join(ErrIncompleteRead, ErrKeyNotFound)
			return
		}

		if err := req.Err(); err != nil {
			reader.err = err
			return
		}

		reader.blks[idx] = req.Blk
	}

	reader.val, reader.meta, reader.err = db.parseRecord(wal, reader.size, reader.off-reader.firstBlkOffset, reader.blks, verifyChecksum)
}

func (db *DBImpl) finalizeBlockRequests(
	wal *Wal, reqs block_reader.Requests, headReader *reader, lastReader *reader, last_err error, verifyChecksum bool,
) {
	for {
		front, _ := db.readers.Front()
		_ = db.readers.PopFront()

		(*front).done = true
		if last_err != nil {
			(*front).err = last_err
		} else {
			db.setReaderResult(wal, *front, reqs, verifyChecksum)
		}

		if *front != headReader {
			(*front).cond.Signal()
		}

		if *front == lastReader {
			break
		}
	}

	// release block cache
	for _, req := range reqs {
		db.blockCache.Put(req.Fid, WalBlockIdx(req.Off), uint64(req.NBytes()), req.Blk)
	}
}

func (db *DBImpl) GetV2(ns, key []byte, opts *ReadOptions) ([]byte, *Meta, error) {
	fid, off, sz, err := db.index.Get(ns, key)
	if err != nil {
		return nil, nil, err
	}

	wal := db.getWalRef(fid)
	if wal == nil {
		return nil, nil, ErrKeyNotFound
	}
	defer wal.Unref()

	verifyChecksum := false
	if opts != nil && opts.VerifyChecksum {
		verifyChecksum = true
	}

	// fast path: read from block cache
	firstBlkIdx, firstBlkOff, blkNum := WalBlockIndexRange(off, sz)
	if blks, err := db.blockCache.BatchGet(fid, firstBlkIdx, blkNum); err == nil {
		return db.parseRecord(wal, sz, off-firstBlkOff, blks, verifyChecksum)
	}

	// slow path: read from disk

	// if the value size is large, splitting into small blocks will not improve performance,
	// but will increase the latency of other small IO reads. therefore, when the number of
	// data blocks split by value size exceeds IO concurrent, the pread is directly used to
	// read the data at once.
	//
	// at the same time, the block cache is avoided, and the large value temporarily used
	// too many cache blocks, resulting in a lower cache hit rate for the more common small
	// value.

	// large value
	if blkNum > db.opts.BlockReaderConcurrent {
		return db.readRecord(wal, off, sz, verifyChecksum)
	}

    // small value
    if sz <= BlockSize / 8 {
        return db.readRecord(wal, off, sz, verifyChecksum)
    }

	// medium value
	reader := db.newReader(wal.Fd(), fid, off, sz, blkNum, firstBlkIdx, firstBlkOff)
	defer db.readerPool.Put(reader)

	db.rdMu.Lock()
	defer db.rdMu.Unlock()

	db.readers.PushBack(reader)

	for !reader.done {
		head, _ := db.readers.Front()
		if reader == *head {
			break
		}

		reader.cond.Wait()
	}

	if reader.done {
		return reader.val, reader.meta, reader.err
	}

	// reach here: the reader is front, and other writers should wait
	lastReader := reader
	reqs := db.buildBlockRequests(&lastReader)

	// submit io without lock and wait the io to complete
	db.rdMu.Unlock()
	if len(reqs) > 0 {
		err = db.blockReader.Submit(reqs)
	}
	db.rdMu.Lock()

	db.finalizeBlockRequests(wal, reqs, reader, lastReader, err, verifyChecksum)

	// notify new head of reader queue
	if !db.readers.Empty() {
		front, _ := db.readers.Front()
		(*front).cond.Signal()
	}

	return reader.val, reader.meta, reader.err
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

	db.logger.Info().Msg("db closed")
}

func (db *DBImpl) Rand(upper uint64) uint64 {
	return uint64(db.randor.Int63n(int64(upper)))
}

func (db *DBImpl) WallTime() time.Time {
	return time.Unix(db.wallTime.Load(), 0)
}
