package bitcask

import (
	"encoding/binary"
)

type WalIterator struct {
	wal     *Wal
	fileOff int

	bufOff  int
	bufSize int
	buf     []byte

	err error
}

func NewWalIterator(wal *Wal) *WalIterator {
	wal.Ref()

	return &WalIterator{
		wal:     wal,
		fileOff: int(wal.offset),
		bufOff:  0,
		bufSize: 0,
		buf:     make([]byte, BlockSize),
	}
}

func (i *WalIterator) Close() {
	i.wal.Unref()
}

func (i *WalIterator) fd() int {
	return int(i.wal.fp.Fd())
}

// try to read a block unless there is less than one block left
// the Next method will return the start offset of data in wal file, and the data itself
func (i *WalIterator) Next() (uint64, []byte, error) {
	var off uint64
	var record []byte

	for i.err == nil {
		if i.bufOff+RecordHeaderSize > i.bufSize {
			i.fileOff += i.bufSize

			// skip the padding
			i.bufSize = min(BlockSize, int(i.wal.Size())-i.fileOff)
			if i.bufSize == 0 {
				i.err = ErrWalIteratorEOF
				return 0, nil, i.err
			}

			if i.err = PreadFull(i.fd(), i.buf[:i.bufSize], int64(i.fileOff)); i.err != nil {
				return 0, nil, i.err
			}

			i.bufOff = 0
		}

		header := i.buf[i.bufOff : i.bufOff+RecordHeaderSize]
		i.bufOff += RecordHeaderSize

		crc := binary.LittleEndian.Uint32(header[0:])
		length := int(binary.LittleEndian.Uint16(header[4:]))
		recordType := header[6]

		// record the file offset
		if len(record) == 0 {
			off = uint64(i.fileOff + i.bufOff)
		}

		// avoid the corrupted data
		length = min(length, i.bufSize-i.bufOff)
		data := i.buf[i.bufOff : i.bufOff+length]
		i.bufOff += length

		if ComputeCRC32(data) != crc {
			i.err = ErrWalMismatchCRC
			return 0, nil, i.err
		}

		switch recordType {
		case RecordFull:
			// reference the backing store of slice
			return off, data, nil
		case RecordFirst, RecordMiddle:
			// Continue reading next chunk
			record = append(record, data...)
		case RecordLast:
			record = append(record, data...)
			return off, record, nil
		default:
			i.err = ErrWalUnknownRecordType
		}
	}

	return 0, nil, i.err
}

// the functionality is same to Next, except that the offset does not contain wal header
// it's useful for the hint generation
func (i *WalIterator) NextWithoutHeaderOffset() (uint64, []byte, error) {
	off, data, err := i.Next()
	if err != nil {
		return 0, nil, err
	}

	off -= RecordHeaderSize
	return off, data, nil
}
