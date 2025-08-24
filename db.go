package bitcask

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
)

const (
	WalFileSuffix      = ".wal"
	HintFileSuffix     = ".hint"
	MergeFileSuffix    = ".merge"
	TmpFileSuffix      = ".tmp"
	LockFile           = "LOCK"
	ManifestFilePrefix = "MANIFEST"
	CurrentFile        = "CURRENT"
)

const (
	UnknownFileType = iota

	WalFileType
	HintFileType
	MergeFileType
	TmpFileType
	LockFileType
	ManifestFileType
	CurrentFileType
)

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrIncompleteRead = errors.New("incomplete read")
	ErrKeySoftDeleted = errors.New("key soft delete")
)

type WriteOptions struct {
	Sync bool
}

type ReadOptions struct {
	VerifyChecksum bool
}

type PickerWalInfo struct {
	CreateTime uint64
	FreeBytes  uint64
	WalSize    uint64
	Fid        uint64
}

type (
	CompactionPicker func([]PickerWalInfo) []uint64
	CompactionFilter func(ns, key, val []byte, meta *Meta) bool
)

type Options struct {
	Dir string

	LogFile       string
	LogDir        string
	LogLevel      int8
	LogMaxSize    uint64
	LogMaxBackups uint64

	WalMaxSize      uint64
	ManifestMaxSize uint64

	IndexCapacity             uint64
	IndexLimited              uint64
	IndexEvictionPoolCapacity uint64
	IndexSampleKeys           uint64

	BlockCacheCapacity             uint64
	BlockCacheLimited              uint64
	BlockCacheEvictionPoolCapacity uint64
	BlockCacheSampleKeys           uint64

	BlockReaderConcurrent uint64

	CompactionPicker CompactionPicker
	CompactionFilter CompactionFilter

	DiskUsageLimited uint64

	NsSize   uint64
	EtagSize uint64

	CompactionTriggerInterval uint64
	CheckDiskUsageInterval    uint64

	CompactionPickerRatio float64

	DisableCompaction bool

	RecordBufferSize uint64
}

func (o *Options) Init() {
	if o.LogDir == "" {
		o.LogDir = o.Dir
	}

	if o.LogFile == "" {
		o.LogFile = DefaultLogFile
	}

	if o.LogMaxSize == 0 {
		o.LogMaxSize = DefaultLogMaxSize
	}

	if o.CompactionPicker == nil {
		o.CompactionPicker = DefaultCompactionPicker
	}

	if o.CompactionTriggerInterval <= 0 {
		o.CompactionTriggerInterval = DefaultCompactionTriggerInterval
	}

	if o.CheckDiskUsageInterval <= 0 {
		o.CheckDiskUsageInterval = DefaultCheckDiskUsageInterval
	}

	if o.CompactionPickerRatio <= 0 {
		o.CompactionPickerRatio = DefaultCompactionPickerRatio
	}

	if o.RecordBufferSize <= 0 {
		o.RecordBufferSize = DefaultRecordBufferSize
	}

	gOpts = o
}

var gOpts *Options

// read-only
func GetOptions() *Options {
	return gOpts
}

type DB interface {
	Get(ns, key []byte, opts *ReadOptions) (val []byte, meta *Meta, err error)
	Put(ns, key, val []byte, meta *Meta, opts *WriteOptions) error

	Write(batch *Batch, opts *WriteOptions) error
	Delete(ns, key []byte, opts *WriteOptions) error
	Close()
}

func TmpFilename(fid uint64) string {
	return fmt.Sprintf("%06d%s", fid, TmpFileSuffix)
}

func WalFilename(fid uint64) string {
	return fmt.Sprintf("%06d%s", fid, WalFileSuffix)
}

func HintFilename(fid uint64) string {
	return fmt.Sprintf("%06d%s", fid, HintFileSuffix)
}

func MergeFilename(fid uint64) string {
	return fmt.Sprintf("%06d%s", fid, MergeFileSuffix)
}

func ManifestFilename(fid uint64) string {
	return fmt.Sprintf("%s-%06d", ManifestFilePrefix, fid)
}

func TmpPath(dir string, fid uint64) string {
	return filepath.Join(dir, TmpFilename(fid))
}

func WalPath(dir string, fid uint64) string {
	return filepath.Join(dir, WalFilename(fid))
}

func HintPath(dir string, fid uint64) string {
	return filepath.Join(dir, HintFilename(fid))
}

func ManifestPath(dir string, fid uint64) string {
	return filepath.Join(dir, ManifestFilename(fid))
}

func MergePath(dir string, fid uint64) string {
	return filepath.Join(dir, MergeFilename(fid))
}

func LockPath(dir string) string {
	return filepath.Join(dir, LockFile)
}

func CurrentPath(dir string) string {
	return filepath.Join(dir, CurrentFile)
}

func DefaultCompactionPicker(wals []PickerWalInfo) []uint64 {
	compactionPickerRatio := GetOptions().CompactionPickerRatio

	// reverse order
	sort.Slice(wals, func(i, j int) bool {
		return wals[i].FreeBytes > wals[j].FreeBytes
	})

	var res []uint64
	for idx := range wals {
		size := float64(wals[idx].WalSize)
		free := float64(wals[idx].FreeBytes)

		if free/size < compactionPickerRatio {
			break
		}

		res = append(res, wals[idx].Fid)
		if len(res) >= 2 {
			break
		}
	}

	return res
}
