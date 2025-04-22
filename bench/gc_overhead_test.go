package bench

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wenzhang-dev/bitcaskDB"
)

func newGcOverheadDB(b *testing.B) {
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
		DisableCompaction:         false,
		DiskUsageLimited:          10 * 1024 * 1024 * 1024, // 10GB
	}

	var err error
	db, err = bitcask.NewDB(opts)
	assert.Nil(b, err)
}

var previousPause time.Duration

func gcPause() time.Duration {
	runtime.GC()

	var stats debug.GCStats
	debug.ReadGCStats(&stats)

	pause := stats.PauseTotal - previousPause
	previousPause = stats.PauseTotal

	return pause
}

func BenchmarkGcOverhead(b *testing.B) {
	newGcOverheadDB(b)
	defer db.Close()

	meta := bitcask.NewMeta(nil)
	opts := &bitcask.WriteOptions{}

	b.ResetTimer()
	b.ReportAllocs()

	startTime := time.Now()
	fmt.Printf("GC pause for startup: gc=%s\n", gcPause())

	var totalIteration int64
	b.RunParallel(func(pb *testing.PB) {
		var err error
		iteration := 0
		batch := bitcask.NewBatch()
		for pb.Next() {
			batch.Put(ns[:], genTestKey(iteration), bin4KB, meta)

			if iteration%BatchSize == 0 {
				err = db.Write(batch, opts)
				assert.Nil(b, err)
				batch.Clear()
			}

			iteration++
		}

		atomic.AddInt64(&totalIteration, int64(iteration))
	})

	diff := time.Since(startTime)
	fmt.Printf("GC pause for test: total=%s, gc=%s, iter=%d\n", diff, gcPause(), totalIteration)
}
