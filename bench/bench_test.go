package bench

import (
	"crypto/sha1"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wenzhang-dev/bitcaskDB"
)

var db bitcask.DB

func newDB(b *testing.B) {
	dir := "./bitcaskDB"
	_ = os.RemoveAll(dir)
	_ = os.MkdirAll(dir, os.ModePerm)

	opts := &bitcask.Options{
		Dir:                       dir,
		WalMaxSize:                1024 * 1024 * 1024, // 1GB
		ManifestMaxSize:           10 * 1024 * 1024,   // 10MB
		IndexCapacity:             1000000,            // 1 million
		IndexLimited:              80000,
		IndexEvictionPoolCapacity: 64,
		IndexSampleKeys:           5,
		CompactionPicker:          nil, // default picker
		CompactionFilter:          nil, // default filter
	}

	var err error
	db, err = bitcask.NewDB(opts)
	assert.Nil(b, err)
}

func BenchmarkPutGet(b *testing.B) {
	newDB(b)
	defer db.Close()

	b.Run("put", benchmarkPut)
	b.Run("batchPut", benchmarkBatchPut)
	b.Run("get", benchmarkGet)
}

func benchmarkPut(b *testing.B) {
	ns := sha1.Sum([]byte("test"))
	getTestBytes := func(i int) []byte {
		return []byte(strconv.Itoa(i))
	}
	meta := bitcask.NewMeta(nil)
	opts := &bitcask.WriteOptions{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(ns[:], getTestBytes(i), getTestBytes(i), meta, opts)
		assert.Nil(b, err)
	}
}

func benchmarkBatchPut(b *testing.B) {
	ns := sha1.Sum([]byte("test"))
	getTestBytes := func(i int) []byte {
		return []byte(strconv.Itoa(i))
	}
	meta := bitcask.NewMeta(nil)
	opts := &bitcask.WriteOptions{}
	const batchSize = 50

	b.ResetTimer()
	b.ReportAllocs()

	batch := bitcask.NewBatch()
	for i := 0; i < b.N; i++ {
		batch.Put(ns[:], getTestBytes(i), getTestBytes(i), meta)

		if i%batchSize == 0 {
			err := db.Write(batch, opts)
			assert.Nil(b, err)
			batch.Clear()
		}
	}

	if batch.Size() != 0 {
		err := db.Write(batch, opts)
		assert.Nil(b, err)
	}
}

func benchmarkGet(b *testing.B) {
	ns := sha1.Sum([]byte("test"))
	getTestBytes := func(i int) []byte {
		return []byte(strconv.Itoa(i))
	}
	meta := bitcask.NewMeta(nil)
	wOpts := &bitcask.WriteOptions{}

	const batchSize = 50
	batch := bitcask.NewBatch()
	for i := 0; i < 100001; i++ {
		batch.Put(ns[:], getTestBytes(i), getTestBytes(i), meta)

		if i%batchSize == 0 {
			err := db.Write(batch, wOpts)
			assert.Nil(b, err)
			batch.Clear()
		}
	}

	rOpts := &bitcask.ReadOptions{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, _ = db.Get(ns[:], getTestBytes(i), rOpts)
	}
}
