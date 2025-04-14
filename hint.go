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
	minHintRecordSize       = NsSize + 1 + 1 + 1*3
	hintWalRewriterThrehold = 1024 * 1024 // 1MB
)

var ErrCorruptedHintRecord = errors.New("corrupted hint record")

// format:
// | ns | key-size | key | fid | off | size |
//
// ns: fixed-size string
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

type HintWriter struct {
	rewriter *WalRewriter
}

func NewHintWriter(path string, fid uint64, baseTime int64) (*HintWriter, error) {
	hint, err := NewWal(path, fid, baseTime)
	if err != nil {
		return nil, err
	}

	return &HintWriter{
		rewriter: NewWalRewriter(hint, hintWalRewriterThrehold),
	}, nil
}

func (w *HintWriter) AppendRecord(record *HintRecord) error {
	recordBytes, err := record.Encode()
	if err != nil {
		return err
	}

	_, err = w.rewriter.AppendRecord(recordBytes)
	return err
}

func (w *HintWriter) Wal() *Wal {
	return w.rewriter.wal
}

func (w *HintWriter) Close() error {
	return w.rewriter.Close()
}

func (w *HintWriter) Flush() error {
	return w.rewriter.Flush()
}

func NewHintByWal(wal *Wal) error {
	// hint wal use the same fid and base time
	hintPath := TmpPath(wal.Dir(), wal.fid)
	writer, err := NewHintWriter(hintPath, wal.fid, int64(wal.BaseTime()))
	if err != nil {
		return err
	}

	defer writer.Close()

	it := NewWalIterator(wal)
	defer it.Close()

	var foff uint64
	var recordBytes []byte
	var record *Record

	for {
		// hint record should point to the start offset of data header
		if foff, recordBytes, err = it.NextWithoutHeaderOffset(); err != nil {
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

		if err = writer.AppendRecord(hintRecord); err != nil {
			return err
		}
	}

	// rename hint file
	return writer.Wal().Rename(HintFilename(wal.fid))
}

func IterateHint(hint *Wal, cb func(record *HintRecord) error) error {
	it := NewWalIterator(hint)
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

		if err = cb(record); err != nil {
			return err
		}
	}

	return nil
}

func RecoverFromHint(path string, fid uint64, cb func(record *HintRecord) error) error {
	hint, err := LoadWal(path, fid)
	if err != nil {
		return err
	}

	defer hint.Close()

	return IterateHint(hint, cb)
}
