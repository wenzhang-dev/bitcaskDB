package bitcask

import (
	"encoding/binary"
	"errors"

	"github.com/vmihailenco/msgpack/v5"
)

type Record struct {
	Ns    []byte
	Key   []byte
	Value []byte
	Meta  *Meta

	// mark whether it's a delete operation
	// it's fundamentally different from the meta tombstone.
	// the tombstone indicates s soft deletion, but in fact, the key still exists in database
	// the deleted tag indicates that the key will be removed directly in the database
	// this deleted tag would not be serialized
	Deleted bool
}

// serialization format:
// | header size | header | key | value | meta |
//
// the header including:
// - header size: 1B
// - ns: fixed size
// - flags: 1B
// - key size: varint32 1~5B
// - value size: varint32 1~5B
// - meta size: varint32 1~5B
// - etag: optional, fixed size
// - expire: optional, varint32 1~5B
// - other optional fields if need
//
// for small record:
// if key=16B, value=128B, meta=64B, its header is about 50B, and effective space usage is about 80%
//
// for medium record:
// if key=64B, value=128KB, meta=1KB, it's header is about 50B, and effective space usage is abort 99%

const (
	noEtagFieldBit    = 0
	noExpireFieldBit  = 1
	tombstoneFieldBit = 2
)

func (r *Record) ApproximateSize() int {
	// 1B header size + len(ns) + 1B flags + 2B varint * 3 + len(meta) + 2B expire
	approximateRecordHeaderSize := 1 + len(r.Ns) + 1 + 2*3 + len(r.Meta.Etag) + 2

	return approximateRecordHeaderSize + len(r.Key) + len(r.Value) + r.Meta.AppMetaApproximateSize()
}

func (r *Record) Encode(backStore []byte, baseTime uint64) ([]byte, error) {
	flag := byte(0)
	if len(r.Meta.Etag) == 0 {
		flag |= byte(1 << noEtagFieldBit)
	}

	if r.Meta.IsTombstone() {
		flag |= byte(1 << tombstoneFieldBit)
	}

	expireSize := 0
	var expireBytes [binary.MaxVarintLen32]byte
	switch {
	case r.Meta.Expire == MetaNoExpire:
		flag |= byte(1 << noExpireFieldBit)

	case r.Meta.Expire < baseTime:
		return nil, errors.New("invalid expire")

	default: // expire > base time
		expireSize = binary.PutUvarint(expireBytes[:], r.Meta.Expire-baseTime)
	}

	var err error
	var metaEncoded []byte
	if r.Meta.AppMetaSize != 0 {
		if metaEncoded, err = msgpack.Marshal(r.Meta.AppMeta); err != nil {
			return nil, err
		}
	}

	// try to encode the varint32 fields
	offset := 0
	var tmp [3 * binary.MaxVarintLen32]byte
	offset += binary.PutUvarint(tmp[offset:], uint64(len(r.Key)))
	offset += binary.PutUvarint(tmp[offset:], uint64(len(r.Value)))
	offset += binary.PutUvarint(tmp[offset:], uint64(len(metaEncoded)))
	tmpSize := offset

	// plus 2 bytes: flag and header size
	headerSize := offset + expireSize + len(r.Ns) + len(r.Meta.Etag) + 2
	totalSize := headerSize + len(r.Key) + len(r.Value) + len(metaEncoded)

	// prefer use the backing store
	buf := backStore[0:]
	if totalSize > cap(backStore) {
		buf = make([]byte, totalSize)
	}

	offset = 0

	// header size
	buf[0] = byte(headerSize)
	offset++

	// namespace
	offset += copy(buf[offset:], r.Ns)

	// flag
	buf[offset] = flag
	offset++

	// varint
	offset += copy(buf[offset:], tmp[:tmpSize])

	// optional etag
	offset += copy(buf[offset:], r.Meta.Etag)

	// optional ttl
	offset += copy(buf[offset:], expireBytes[:expireSize])

	// key
	offset += copy(buf[offset:], r.Key)

	// value
	offset += copy(buf[offset:], r.Value)

	// meta
	offset += copy(buf[offset:], metaEncoded)

	return buf[:offset], nil
}

func RecordFromBytes(data []byte, baseTime uint64) (*Record, error) {
	nsSize := int(GetOptions().NsSize)

	// 1B header size + len(ns) + 1B flags + 1B variant * 3
	minRecordHeaderSize := 1 + nsSize + 1 + 1*3

	if len(data) < minRecordHeaderSize {
		return nil, errors.New("invalid data")
	}

	offset := 0

	// header size
	headerSize := int(data[0])
	offset++

	// namespace
	ns := data[offset : offset+nsSize]
	offset += nsSize

	// flag
	flag := data[offset]
	offset++

	// key size
	keyLen, keySize := DecodeUvarint(data[offset:])
	offset += keySize

	// value size
	valLen, valSize := DecodeUvarint(data[offset:])
	offset += valSize

	// meta size
	metaLen, metaSize := DecodeUvarint(data[offset:])
	offset += metaSize

	// validation
	// avoid out of range of data buffer
	etagLen := int(GetOptions().EtagSize)
	if flag&(1<<noEtagFieldBit) != 0 {
		etagLen = 0
	}

	expireSize := 0
	expire := uint64(MetaNoExpire)
	if flag&(1<<noExpireFieldBit) == 0 {
		expire, expireSize = DecodeUvarint(data[offset+etagLen:])
		expire += baseTime
	}

	currentHeaderSize := offset + etagLen + expireSize
	currentTotalSize := currentHeaderSize + int(keyLen+valLen+metaLen)
	if currentHeaderSize != headerSize || currentTotalSize != len(data) {
		return nil, errors.New("invalid data")
	}

	// etag
	etag := data[offset : offset+etagLen]
	offset += etagLen

	// ttl
	offset += expireSize

	// key
	key := data[offset : offset+int(keyLen)]
	offset += int(keyLen)

	// value
	value := data[offset : offset+int(valLen)]
	offset += int(valLen)

	// meta
	meta := data[offset : offset+int(metaLen)]
	// offset += int(metaLen)

	// app meta deserialization
	serverMeta := NewMeta(nil)
	if metaLen > 0 {
		var appMeta map[string]string
		if err := msgpack.Unmarshal(meta, &appMeta); err != nil {
			return nil, err
		}
		serverMeta.SetAppMeta(appMeta)
	}

	serverMeta.SetEtag(
		etag,
	).SetTombstone(
		flag&(1<<tombstoneFieldBit) != 0,
	).SetExpire(
		expire,
	)

	return &Record{
		Ns:    ns,
		Key:   key,
		Meta:  serverMeta,
		Value: value,
	}, nil
}

// Iterate the specific wal and the callback will pass record, offset and data size
func IterateRecord(wal *Wal, cb func(record *Record, foff, size uint64) error) error {
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

		if err = cb(record, foff, uint64(len(recordBytes))); err != nil {
			return err
		}
	}

	return nil
}
