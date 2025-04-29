package bench

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wenzhang-dev/bitcaskDB"
)

func newCompactionDB(b *testing.B) {
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
		NsSize:                    0,
		EtagSize:                  0,
		DisableCompaction:         false,
		CompactionTriggerInterval: 10, // 10 seconds
	}

	var err error
	db, err = bitcask.NewDB(opts)
	assert.Nil(b, err)
}

func BenchmarkCompaction(b *testing.B) {
	newCompactionDB(b)
	defer db.Close()

	b.Run("compaction", func(b *testing.B) {
		benchmarkCompaction(b, db)
	})
}

func benchmarkCompaction(b *testing.B, db bitcask.DB) {
	threshold := 10000000
	meta := bitcask.NewMeta(nil)
	value4KB := bitcask.GenNKBytes(4)
	opts := &bitcask.WriteOptions{}
	batchSize := 50

	newKey := func(hint, threshold int) []byte {
		hint %= threshold
		key := fmt.Sprintf("key=%10d,%10d", hint, hint) // 25 bytes
		return []byte(key)
	}

	// repeat write 10 million keys
	b.RunParallel(func(pb *testing.PB) {
		iteration := 1
		batch := bitcask.NewBatch()
		for pb.Next() {
			batch.Put(nil, newKey(iteration, threshold), value4KB, meta)

			if iteration%batchSize == 0 {
				err := db.Write(batch, opts)
				assert.Nil(b, err)
				batch.Clear()
			}

			iteration++
		}
	})
}
