package bench

import (
	"crypto/sha1"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wenzhang-dev/bitcaskDB"
)

var (
	dir string
	db  *bitcask.DBImpl
)

const BatchSize = 50

func genTestKey(i int) []byte {
	return []byte(strconv.Itoa(i))
}

var (
	bin4KB = bitcask.GenNKBytes(4)
	ns     = sha1.Sum([]byte("benchmark"))
)

func newDB(b *testing.B) {
	dir = "./bitcaskDB"
	_ = os.RemoveAll(dir)
	_ = os.MkdirAll(dir, os.ModePerm)

	opts := &bitcask.Options{
		Dir:                       dir,
		WalMaxSize:                1024 * 1024 * 1024, // 1GB
		ManifestMaxSize:           10 * 1024 * 1024,   // 10MB
		IndexCapacity:             10000000,           // 10 million
		IndexLimited:              8000000,
		IndexEvictionPoolCapacity: 64,
		IndexSampleKeys:           5,
		CompactionPicker:          nil, // default picker
		CompactionFilter:          nil, // default filter
		NsSize:                    bitcask.DefaultNsSize,
		EtagSize:                  bitcask.DefaultEtagSize,
		DisableCompaction:         true,
	}

	var err error
	db, err = bitcask.NewDB(opts)
	assert.Nil(b, err)
}

func BenchmarkPutGet(b *testing.B) {
	newDB(b)
	defer db.Close()

	b.Run("put4K", benchmarkPut)
	b.Run("batchPut4K", benchmarkBatchPut)
	b.Run("get4K", benchmarkGet)

	b.Run("concurrentGet4K", benchmarkConcurrentGet)
	b.Run("concurrentPut4K", benchmarkConcurrentPut)
}

func benchmarkPut(b *testing.B) {
	meta := bitcask.NewMeta(nil)
	opts := &bitcask.WriteOptions{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(ns[:], genTestKey(i), bin4KB, meta, opts)
		assert.Nil(b, err)
	}
}

func benchmarkConcurrentPut(b *testing.B) {
	meta := bitcask.NewMeta(nil)
	opts := &bitcask.WriteOptions{}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		iteration := 0
		for pb.Next() {
			err := db.Put(ns[:], genTestKey(iteration), bin4KB, meta, opts)
			assert.Nil(b, err)

			iteration++
		}
	})
}

func benchmarkBatchPut(b *testing.B) {
	meta := bitcask.NewMeta(nil)
	opts := &bitcask.WriteOptions{}

	b.ResetTimer()
	b.ReportAllocs()

	batch := bitcask.NewBatch()
	for i := 0; i < b.N; i++ {
		batch.Put(ns[:], genTestKey(i), bin4KB, meta)

		if i%BatchSize == 0 {
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

func getPrepare(b *testing.B) {
	meta := bitcask.NewMeta(nil)
	wOpts := &bitcask.WriteOptions{}

	batch := bitcask.NewBatch()
	for i := 0; i < 100001; i++ {
		batch.Put(ns[:], genTestKey(i), bin4KB, meta)

		if i%BatchSize == 0 {
			err := db.Write(batch, wOpts)
			assert.Nil(b, err)
			batch.Clear()
		}
	}
}

func benchmarkGet(b *testing.B) {
	getPrepare(b)

	rOpts := &bitcask.ReadOptions{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, err := db.Get(ns[:], genTestKey(i%100000), rOpts)
		assert.Nilf(b, err, "i: %v", i)
	}
}

func benchmarkConcurrentGet(b *testing.B) {
	getPrepare(b)

	rOpts := &bitcask.ReadOptions{}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		iteration := 0
		for pb.Next() {
			_, _, err := db.Get(ns[:], genTestKey(iteration%100000), rOpts)
			assert.Nil(b, err)

			iteration++
		}
	})
}
