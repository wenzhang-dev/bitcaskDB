package bitcask

import (
	"crypto/sha1"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

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

// namespace is fixed size
func MergedKey(ns, key []byte) []byte {
	mergedKey := make([]byte, len(ns)+len(key))
	copy(mergedKey, ns)
	copy(mergedKey[len(ns):], key)

	return mergedKey
}

func GenSha1NS(ns string) []byte {
	hash := sha1.Sum([]byte(ns))
	return hash[:]
}

func GenSha1Etag(data []byte) []byte {
	hash := sha1.Sum(data)
	return hash[:]
}

func Gen1KBytes() []byte {
	buf := make([]byte, 1024)
	for i := 0; i < 128; i++ {
		copy(buf[i*8:], []byte("01234567"))
	}
	return buf
}

func GenNKBytes(n int) []byte {
	bytes1KB := Gen1KBytes()
	buf := make([]byte, 1024*n)
	for i := 0; i < n; i++ {
		copy(buf[i*1024:], bytes1KB)
	}
	return buf
}
