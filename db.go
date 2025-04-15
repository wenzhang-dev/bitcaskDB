package bitcask

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
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
	ErrKeySoftDeleted = errors.New("key soft delete")
)

// namespace is fixed size
func MergedKey(ns, key []byte) []byte {
	mergedKey := make([]byte, len(ns)+len(key))
	copy(mergedKey, ns)
	copy(mergedKey[len(ns):], key)

	return mergedKey
}

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
	Dir             string
	WalMaxSize      uint64
	ManifestMaxSize uint64

	IndexCapacity             uint64
	IndexLimited              uint64
	IndexEvictionPoolCapacity uint64
	IndexSampleKeys           uint64

	CompactionPicker CompactionPicker
	CompactionFilter CompactionFilter

	DiskUsageLimited int64
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

func PathExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

func ComputeCRC32(data []byte) uint32 {
	const castagnoliPoly = 0x82f63b78
	table := crc32.MakeTable(castagnoliPoly)
	checksum := crc32.Checksum(data, table)
	return (checksum>>15 | checksum<<17) + 0xa282ead8
}

// pread does not modify the file pointer, so it has no effect on append write
func PreadFull(fd int, buf []byte, offset int64) error {
	totalRead, expectRead := 0, len(buf)
	for totalRead < expectRead {
		// pread syscall try to read data with the specific buffer length
		n, err := unix.Pread(fd, buf[totalRead:], offset+int64(totalRead))
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		totalRead += n
	}

	return nil
}

// return 0, 0 for all exceptions
func DecodeUvarint(data []byte) (uint64, int) {
	v, size := binary.Uvarint(data)
	if size <= 0 {
		return 0, 0
	}
	return v, size
}

type Runners struct {
	functors  []func()
	committed bool
}

func NewRunner() *Runners {
	return &Runners{
		committed: true,
	}
}

func NewReverseRunner() *Runners {
	return &Runners{
		committed: false,
	}
}

func (r *Runners) Post(f func()) {
	r.functors = append(r.functors, f)
}

func (r *Runners) Do() {
	if !r.committed {
		return
	}

	for idx := range r.functors {
		r.functors[idx]()
	}
}

func (r *Runners) Rollback() {
	r.committed = false
}

func (r *Runners) Commit() {
	r.committed = true
}

func ParseFilename(name string) (fileType int, fid uint64, err error) {
	ext := filepath.Ext(name)

	switch ext {
	case WalFileSuffix:
		fid, err := strconv.Atoi(name[:len(name)-len(ext)])
		return WalFileType, uint64(fid), err
	case HintFileSuffix:
		fid, err := strconv.Atoi(name[:len(name)-len(ext)])
		return HintFileType, uint64(fid), err
	case MergeFileSuffix:
		fid, err := strconv.Atoi(name[:len(name)-len(ext)])
		return MergeFileType, uint64(fid), err
	case TmpFileSuffix:
		fid, err := strconv.Atoi(name[:len(name)-len(ext)])
		return TmpFileType, uint64(fid), err
	}

	if name == CurrentFile {
		return CurrentFileType, 0, nil
	}

	if name == LockFile {
		return LockFileType, 0, nil
	}

	if strings.HasPrefix(name, ManifestFilePrefix) {
		fid, err := strconv.Atoi(name[len(ManifestFilePrefix)+1:])
		return ManifestFileType, uint64(fid), err
	}

	return UnknownFileType, 0, nil
}

func GenSha1NS(ns string) []byte {
	hash := sha1.Sum([]byte(ns))
	return hash[:]
}

func GenSha1Etag(data []byte) []byte {
	hash := sha1.Sum(data)
	return hash[:]
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
