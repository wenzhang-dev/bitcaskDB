package bench

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wenzhang-dev/bitcaskDB"
)

func newDiskUsageDB(b *testing.B) {
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

func BenchmarkDiskUsage(b *testing.B) {
	b.Run("batchPut4K", benchmarkDiskUsageBatchPut)

	b.Run("concurrentBatchPut4K", benchmarkDiskUsageConcurrentBatchPut)
}

func getActualDiskUsage(path string) int64 {
	cmd := exec.Command("du", "-sb", path)

	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return 0
	}

	parts := strings.Fields(out.String())
	if len(parts) < 1 {
		return 0
	}

	size, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0
	}

	return size
}

// print disk usgae per three seconds
var (
	totalBytesWritten int64
	stopCh            chan struct{}
)

func printDiskUsageStat() {
	fmt.Printf("\n")

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	var lastTotal int64
	for {
		select {
		case <-ticker.C:
			current := atomic.LoadInt64(&totalBytesWritten)
			speed := current - lastTotal
			lastTotal = current
			fmt.Printf(
				"Write Speed %.2f MB/s | Write Total: %.2f GB | Disk Usage: %.2f GB\n",
				float64(speed)/1024/1024/3,
				float64(current)/1024/1024/1024,
				float64(getActualDiskUsage(dir))/1024/1024/1024,
			)
		case <-stopCh:
			return
		}
	}
}

func benchmarkDiskUsageBatchPut(b *testing.B) {
	newDiskUsageDB(b)
	defer db.Close()

	totalBytesWritten = 0
	stopCh = make(chan struct{})
	defer close(stopCh)

	go printDiskUsageStat()

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
			atomic.AddInt64(&totalBytesWritten, int64(batch.ByteSize()))

			batch.Clear()
		}
	}
}

func benchmarkDiskUsageConcurrentBatchPut(b *testing.B) {
	newDiskUsageDB(b)
	defer db.Close()

	totalBytesWritten = 0
	stopCh = make(chan struct{})
	defer close(stopCh)

	go printDiskUsageStat()

	meta := bitcask.NewMeta(nil)
	opts := &bitcask.WriteOptions{}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		var err error
		iteration := 0
		batch := bitcask.NewBatch()
		for pb.Next() {
			batch.Put(ns[:], genTestKey(iteration), bin4KB, meta)

			if iteration%BatchSize == 0 {
				err = db.Write(batch, opts)
				assert.Nil(b, err)
				atomic.AddInt64(&totalBytesWritten, int64(batch.ByteSize()))

				batch.Clear()
			}

			iteration++
		}
	})
}
