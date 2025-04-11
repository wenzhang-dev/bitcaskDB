package bitcask

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type HintRecord struct {
	ns  []byte
	key []byte

	fid  uint64
	off  uint64
	size uint64
}

const (
	minHintRecordSize = NsSize + 1 + 1 + 1*3
)

var ErrCorruptedHintRecord = errors.New("corrupted hint record")

// format:
// | ns | key-size | key | fid | off | size |
//
// ns: fixed-size hex string
// key-size: varint64
// fid: varint64
// off: varint64
// size: varint64
func (r *HintRecord) Encode() ([]byte, error) {
	var buf bytes.Buffer
	encodeVarint := func(v uint64) {
		var tmp [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(tmp[:], v)
		buf.Write(tmp[:n])
	}

	buf.Write(r.ns)
	encodeVarint(uint64(len(r.key)))
	buf.Write(r.key)
	encodeVarint(r.fid)
	encodeVarint(r.off)
	encodeVarint(r.size)

	return buf.Bytes(), nil
}

func (r *HintRecord) Decode(data []byte) error {
	if len(data) < minHintRecordSize {
		return ErrCorruptedHintRecord
	}

	offset := 0

	r.ns = data[:NsSize]
	offset += NsSize

	keyLen, nbytes := DecodeUvarint(data[offset:])
	offset += nbytes

	keyOffset := offset
	offset += int(keyLen)

	r.fid, nbytes = DecodeUvarint(data[offset:])
	offset += nbytes

	r.off, nbytes = DecodeUvarint(data[offset:])
	offset += nbytes

	r.size, nbytes = DecodeUvarint(data[offset:])
	offset += nbytes

	if offset != len(data) {
		return ErrCorruptedHintRecord
	}

	r.key = data[keyOffset : keyOffset+int(keyLen)]

	return nil
}

type HintWal struct {
	wal *Wal

	bufLen int
}

func LoadHint(path string, fid uint64) (*HintWal, error) {
	hint, err := LoadWal(path, fid)
	if err != nil {
		return nil, err
	}

	return &HintWal{
		wal:    hint,
		bufLen: 0,
	}, nil
}

func NewHint(path string, fid uint64, baseTime int64) (*HintWal, error) {
	hint, err := NewWal(path, fid, baseTime)
	if err != nil {
		return nil, err
	}

	return &HintWal{
		wal:    hint,
		bufLen: 0,
	}, nil
}

func (h *HintWal) AppendRecord(record *HintRecord) error {
	recordBytes, err := record.Encode()
	if err != nil {
		return err
	}

	_, err = h.wal.WriteRecord(recordBytes)
	if err != nil {
		return err
	}

	h.bufLen += len(recordBytes)
	if h.bufLen >= (1 << 20) { // 1 MB
		if err = h.wal.Flush(); err != nil {
			return err
		}
		h.bufLen = 0
	}

	return nil
}

func (h *HintWal) Flush() error {
	if h.bufLen == 0 {
		return nil
	}

	return h.wal.Flush()
}

func (h *HintWal) Close() error {
	return h.wal.Close()
}

func NewHintByWal(wal *Wal) error {
	// hint wal use the same fid and base time
	hintPath := TmpPath(wal.Dir(), wal.fid)
	hint, err := NewHint(hintPath, wal.fid, int64(wal.BaseTime()))
	if err != nil {
		return err
	}

	defer hint.Close()

	it := NewWalIterator(wal)
	defer it.Close()

	var foff uint64
	var recordBytes []byte
	var record *Record

	for {
		if foff, recordBytes, err = it.Next(); err != nil {
			if errors.Is(err, ErrWalIteratorEOF) {
				break
			}
			return err
		}

		if record, err = RecordFromBytes(recordBytes, wal.BaseTime()); err != nil {
			return err
		}

		hintRecord := &HintRecord{
			ns:   record.Ns,
			key:  record.Key,
			fid:  wal.fid,
			off:  foff,
			size: uint64(len(recordBytes)),
		}

		if err = hint.AppendRecord(hintRecord); err != nil {
			return err
		}
	}

	// rename hint file
	return hint.wal.Rename(HintFilename(wal.fid))
}

func IterateHint(hint *HintWal, cb func(ns, key []byte, fid, off, sz uint64) error) error {
	it := NewWalIterator(hint.wal)
	defer it.Close()

	var err error
	var recordBytes []byte
	record := &HintRecord{}
	for {
		if _, recordBytes, err = it.Next(); err != nil {
			if errors.Is(err, ErrWalIteratorEOF) {
				break
			}
			return err
		}

		if err = record.Decode(recordBytes); err != nil {
			return err
		}

		if err = cb(record.ns, record.key, record.fid, record.off, record.size); err != nil {
			return err
		}
	}

	return nil
}

func RecoverFromHint(path string, fid uint64, cb func(ns, key []byte, fid, off, sz uint64) error) error {
	hint, err := LoadHint(path, fid)
	if err != nil {
		return err
	}

	defer hint.Close()

	return IterateHint(hint, cb)
}
