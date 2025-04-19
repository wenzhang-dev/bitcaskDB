package bitcask

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrWalIteratorEOF       = errors.New("eof")
	ErrWalMismatchCRC       = errors.New("CRC mismatch, corrupted data")
	ErrWalMismatchMagic     = errors.New("magic number mismatch")
	ErrWalMismatchBlockSize = errors.New("block size mismatch")
	ErrWalUnknownRecordType = errors.New("invalid record type")
	ErrWalUnavailable       = errors.New("wal unavailable")
	ErrWalFrozen            = errors.New("wal frozen")
)

// wal super block at head
type superBlock struct {
	magic     uint64
	blockSize uint64

	// start position of actual data
	// that points the next byte of checksum
	startOff uint32

	// maybe have other fields
	createTime uint64
	baseTime   uint64

	// checksum as footer
	crc32 uint32
}

const (
	BlockSize = 32 * 1024 // 32KB per block

	RecordFull   = 1 // Record fits entirely in a single block
	RecordFirst  = 2 // First chunk of a record
	RecordMiddle = 3 // Middle chunk of a record
	RecordLast   = 4 // Last chunk of a record

	RecordHeaderSize = 7 // 4 bytes CRC + 2 bytes Length + 1 byte Type

	MagicNumber           = 0x77616C64 // magic number
	SuperBlockSize        = 40         // 8 + 8 + 4 + 8 + 8 + 4 bytes
	SuperBlockCRC32Offset = SuperBlockSize - 4
)

// it's not thread safe
// usually, only one writer can operate the Wal. no race condition
type Wal struct {
	fp    *os.File
	super *superBlock

	// reference count
	refs        *atomic.Int64
	deleterOnce sync.Once

	// internal buffer
	buf bytes.Buffer

	// file name
	path string

	// file id
	fid uint64

	// file size
	size uint64

	// the data start position
	offset uint32

	// when the wal is marked immutable, which is not writable
	immutable bool

	// indicate whether the wal can operate
	invalid bool
}

func (wal *Wal) Rename(newName string) error {
	newPath := filepath.Join(wal.Dir(), newName)
	if err := os.Rename(wal.path, newPath); err != nil {
		return err
	}

	wal.path = newPath
	return nil
}

// load the existed wal file
func LoadWal(path string, fid uint64) (*Wal, error) {
	runners := NewRunner()
	defer runners.Do()

	file, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	runners.Post(func() {
		file.Close()
	})

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	wal := &Wal{
		fp:        file,
		path:      path,
		fid:       fid,
		size:      uint64(stat.Size()),
		immutable: false,
		invalid:   false,
		refs:      new(atomic.Int64),
	}

	wal.refs.Store(1)

	wal.super, err = wal.loadSuperBlock()
	if err != nil {
		return nil, err
	}

	wal.offset = wal.super.startOff

	// abort all functors
	runners.Rollback()

	return wal, nil
}

// create the wal with specific path
func NewWal(path string, fid uint64, baseTime int64) (*Wal, error) {
	runners := NewRunner()
	defer runners.Do()

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	runners.Post(func() {
		file.Close()
	})

	if baseTime < 0 {
		baseTime = time.Now().Unix()
	}

	wal := &Wal{
		fp:        file,
		path:      path,
		fid:       fid,
		size:      0,
		immutable: false,
		invalid:   false,
		refs:      new(atomic.Int64),
	}

	wal.refs.Store(1)

	wal.super, err = wal.writeSuperBlock(uint64(baseTime))
	if err != nil {
		return nil, err
	}

	wal.size += SuperBlockSize
	wal.offset = wal.super.startOff

	// abort all functors
	runners.Rollback()

	return wal, nil
}

func (wal *Wal) deleteSelf() {
	wal.invalid = true
	_ = wal.fp.Close()
	_ = os.Remove(wal.path)
}

// thread-safe
// decrease the reference count
// delete self when reference count equals zero
func (wal *Wal) Unref() {
	if wal.refs.Add(-1) == 0 {
		wal.deleterOnce.Do(wal.deleteSelf)
	}
}

// thread-safe
// increase the reference count
func (wal *Wal) Ref() {
	wal.refs.Add(1)
}

// before read and write wal, usually incr the reference count
// it makes sure the wal is always valid
func (wal *Wal) Valid() bool {
	return !wal.invalid
}

func (wal *Wal) writeSuperBlock(baseTime uint64) (*superBlock, error) {
	super := &superBlock{
		magic:      MagicNumber,
		blockSize:  BlockSize,
		startOff:   SuperBlockSize,
		createTime: uint64(time.Now().Unix()),
		baseTime:   baseTime,
	}

	buf := make([]byte, SuperBlockSize)
	binary.LittleEndian.PutUint64(buf[0:], super.magic)
	binary.LittleEndian.PutUint64(buf[8:], super.blockSize)
	binary.LittleEndian.PutUint32(buf[16:], super.startOff)
	binary.LittleEndian.PutUint64(buf[20:], super.createTime)
	binary.LittleEndian.PutUint64(buf[28:], super.baseTime)

	crc := ComputeCRC32(buf[:SuperBlockCRC32Offset])
	binary.LittleEndian.PutUint32(buf[SuperBlockCRC32Offset:], crc)

	if _, err := wal.fp.Write(buf); err != nil {
		return nil, err
	}

	if err := wal.fp.Sync(); err != nil {
		return nil, err
	}

	return super, nil
}

func (wal *Wal) loadSuperBlock() (*superBlock, error) {
	buf := make([]byte, SuperBlockSize)
	_, err := wal.fp.ReadAt(buf, 0)
	if err != nil {
		return nil, err
	}

	crc := ComputeCRC32(buf[:SuperBlockCRC32Offset])
	crcExpect := binary.LittleEndian.Uint32(buf[SuperBlockCRC32Offset:])
	if crc != crcExpect {
		return nil, ErrWalMismatchCRC
	}

	magic := binary.LittleEndian.Uint64(buf[0:])
	if magic != MagicNumber {
		return nil, ErrWalMismatchMagic
	}

	blockSize := binary.LittleEndian.Uint64(buf[8:])
	startOff := binary.LittleEndian.Uint32(buf[16:])

	if blockSize != BlockSize {
		return nil, ErrWalMismatchBlockSize
	}

	createTime := binary.LittleEndian.Uint64(buf[20:])
	baseTime := binary.LittleEndian.Uint64(buf[28:])

	return &superBlock{
		magic:      magic,
		blockSize:  blockSize,
		startOff:   startOff,
		createTime: createTime,
		baseTime:   baseTime,
		crc32:      crc,
	}, nil
}

func (wal *Wal) CreateTime() uint64 {
	return wal.super.createTime
}

func (wal *Wal) BaseTime() uint64 {
	return wal.super.baseTime
}

func (wal *Wal) Sync() error {
	return wal.fp.Sync()
}

func (wal *Wal) Freeze() {
	wal.immutable = true
}

func (wal *Wal) Immutable() bool {
	return !wal.immutable
}

func (wal *Wal) Close() error {
	wal.Flush()
	return wal.fp.Close()
}

func (wal *Wal) Path() string {
	return wal.path
}

func (wal *Wal) Dir() string {
	return path.Dir(wal.path)
}

func (wal *Wal) Fid() uint64 {
	return wal.fid
}

// return the current file size
func (wal *Wal) Size() uint64 {
	return wal.size
}

func (wal *Wal) Empty() bool {
	return wal.size == SuperBlockSize && wal.buf.Len() == 0
}

// flush the internal buffer
func (wal *Wal) Flush() error {
	if wal.buf.Len() == 0 {
		return nil
	}

	data := wal.buf.Bytes()
	defer wal.buf.Reset()

	n, err := wal.fp.Write(data)

	// FIXME: maybe parital write
	wal.size += uint64(n)

	return err
}

// clean the internal buffer
func (wal *Wal) ResetBuffer() {
	wal.buf.Reset()
}

// append data to internal buffer
func (wal *Wal) appendFile(data []byte) error {
	_, err := wal.buf.Write(data)
	return err
}

// the record offset should include super block, which is the pysical offset of wal file
// but the splitted blocks should exclude the super block. in other words, the file layout:
// |  super block  |  block  |  block  | ... |
// |<-    40B    ->|<- 32K ->|<- 32K ->| ... |
func (wal *Wal) writeOffset(skipSuperBlock bool) uint64 {
	if !skipSuperBlock {
		return wal.size + uint64(wal.buf.Len())
	}
	return wal.size + uint64(wal.buf.Len()) - SuperBlockSize
}

// write one record, and return the start offset of the record in wal file
func (wal *Wal) WriteRecord(record []byte) (uint64, error) {
	if !wal.Valid() {
		return 0, ErrWalUnavailable
	}

	if !wal.Immutable() {
		return 0, ErrWalFrozen
	}

	var err error
	var offset uint64
	begin := true
	left := uint64(len(record))
	padding := []byte{0, 0, 0, 0, 0, 0}

	for left > 0 {
		leftover := BlockSize - (wal.writeOffset(true) % BlockSize)
		if leftover < RecordHeaderSize {
			if err = wal.appendFile(padding[:leftover]); err != nil {
				return 0, err
			}
			leftover = BlockSize
		}

		if begin {
			offset = wal.writeOffset(false)
		}

		avail := leftover - RecordHeaderSize
		fragmentLength := min(left, avail)

		var recordType byte
		end := (left == fragmentLength)
		switch {
		case begin && end:
			recordType = RecordFull
		case begin:
			recordType = RecordFirst
		case end:
			recordType = RecordLast
		default:
			recordType = RecordMiddle
		}

		header := make([]byte, RecordHeaderSize)
		binary.LittleEndian.PutUint32(header[0:], ComputeCRC32(record[:fragmentLength]))
		binary.LittleEndian.PutUint16(header[4:], uint16(fragmentLength))
		header[6] = recordType

		if err = wal.appendFile(header); err != nil {
			return 0, err
		}

		if err = wal.appendFile(record[:fragmentLength]); err != nil {
			return 0, err
		}

		record = record[fragmentLength:]
		left -= fragmentLength
		begin = false
	}

	return offset, nil
}

// calculate the physical footprint of the record based on its actual size and offset
func (wal *Wal) recordPhysicalSize(offset, size uint64) uint64 {
	left := size
	phySize := uint64(0)

	// skip the super block
	offset -= SuperBlockSize

	for left > 0 {
		leftover := BlockSize - (offset % BlockSize)
		if leftover < RecordHeaderSize {
			phySize += leftover
			offset += leftover
			leftover = BlockSize
		}

		avail := leftover - RecordHeaderSize
		fragmentLength := min(left, avail)

		phySize += (RecordHeaderSize + fragmentLength)
		offset += (RecordHeaderSize + fragmentLength)

		left -= fragmentLength
	}

	return phySize
}

// read one record from specifc offset and size
func (wal *Wal) ReadRecord(offset, size uint64, verifyChecksum bool) (record []byte, err error) {
	if !wal.Valid() {
		return nil, ErrWalUnavailable
	}

	phySize := wal.recordPhysicalSize(offset, size)
	if offset+phySize > wal.size {
		return nil, errors.New("read beyond file size")
	}

	buffer := make([]byte, phySize)
	// read all related data using only one disk read operation
	if err = PreadFull(int(wal.fp.Fd()), buffer, int64(offset)); err != nil {
		return
	}

	recordOff := 0
	expectedOff := 0
	record = make([]byte, size)
	for {
		header := buffer[expectedOff : expectedOff+RecordHeaderSize]
		expectedOff += RecordHeaderSize

		crc := binary.LittleEndian.Uint32(header[0:])
		length := int(binary.LittleEndian.Uint16(header[4:]))
		recordType := header[6]

		// avoid the the corrupted data triggering out of range
		// small probability: use crc32 checksum
		length = min(length, int(phySize)-expectedOff)

		data := buffer[expectedOff : expectedOff+length]
		expectedOff += length

		if verifyChecksum && ComputeCRC32(data) != crc {
			return nil, ErrWalMismatchCRC
		}

		recordOff += copy(record[recordOff:], data)

		switch recordType {
		case RecordFull:
			if recordOff != int(size) {
				return nil, errors.New("size mismatch, corrupted data")
			}
			return
		case RecordFirst, RecordMiddle:
			// Continue reading next chunk
		case RecordLast:
			return
		default:
			return nil, ErrWalUnknownRecordType
		}
	}
}
