package bitcask

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHintEncodeAndDecode(t *testing.T) {
	ns := sha1Bytes("namespace")
	hintRecord := &HintRecord{
		ns:   ns[:],
		key:  []byte("test-key"),
		fid:  2,
		off:  123,
		size: 100,
	}

	bytes, err := hintRecord.Encode()
	assert.Nil(t, err)

	decodeRecord := &HintRecord{}
	err = decodeRecord.Decode(bytes)
	assert.Nil(t, err)

	assert.Equal(t, hintRecord, decodeRecord)
}

func TestHint_NewHintByWal(t *testing.T) {
	wal := setupWal("new_hint_by_wal", t)
	defer wal.Unref()

	ns1 := sha1Bytes("namespace")
	backStore := make([]byte, DefaultRecordBufferSize)
	baseTime := uint64(time.Now().Unix())
	record := &Record{
		Ns:    ns1[:],
		Key:   []byte("test-key"),
		Meta:  NewMeta(nil),
		Value: []byte("hello world"),
	}

	for i := 0; i < 1000; i++ {
		key := []byte("test-key" + strconv.Itoa(i))
		record.Key = key

		bytes, err := record.Encode(backStore, baseTime)
		assert.Nil(t, err)

		_, err = wal.WriteRecord(bytes)
		assert.Nil(t, err)
	}

	wal.Flush()

	// test hint
	fileSize, err := NewHintByWal(wal)
	assert.Nil(t, err)
	assert.True(t, fileSize > 0)

	hintPath := HintPath(wal.Dir(), wal.Fid())
	hint, err := LoadWal(hintPath, wal.Fid())
	assert.Nil(t, err)
	defer hint.Close()
	defer os.Remove(hintPath)

	itNum := 0
	err = IterateHint(hint, func(hintRecord *HintRecord) error {
		assert.Equal(t, hintRecord.ns, ns1[:])
		assert.Equal(t, hintRecord.key, []byte("test-key"+strconv.Itoa(itNum)))

		recordBytes, err := wal.ReadRecord(hintRecord.off, hintRecord.size, true)
		assert.Nil(t, err)

		readRecord, err := RecordFromBytes(recordBytes, wal.BaseTime())
		assert.Nil(t, err)
		assert.Equal(t, readRecord.Ns, hintRecord.ns)
		assert.Equal(t, readRecord.Key, hintRecord.key)
		assert.Equal(t, readRecord.Value, record.Value)

		itNum++
		return nil
	})
	assert.Nil(t, err)
	assert.Equal(t, itNum, 1000)
}
