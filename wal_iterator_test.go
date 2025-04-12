package bitcask

import (
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWalIterator_Basic(t *testing.T) {
	wal := setupWal("test_wal_it_basic.wal", t)
	defer wal.Unref()

	for i := 0; i < 1000; i++ {
		data := []byte(strconv.Itoa(i))
		_, err := wal.WriteRecord(data)
		assert.Nil(t, err)
	}

	wal.Flush()

	// iterate
	it := NewWalIterator(wal)
	defer it.Close()

	itNum := 0
	for {
		_, readData, err := it.Next()
		if errors.Is(err, ErrWalIteratorEOF) {
			break
		}
		assert.Nil(t, err)
		assert.Equal(t, readData, []byte(strconv.Itoa(itNum)))

		itNum++
	}

	assert.Equal(t, itNum, 1000)
}

func TestWalIterator_LargeData(t *testing.T) {
	wal := setupWal("test_wal_it_large_data.wal", t)
	defer wal.Unref()

	data5KB := genNKBytes(5)

	// total 5MB = 4KB * 1024
	for i := 0; i < 1024; i++ {
		_, err := wal.WriteRecord(data5KB)
		assert.Nil(t, err)
		assert.Nil(t, wal.Flush())
	}

	// iterate
	it := NewWalIterator(wal)
	defer it.Close()

	itNum := 0
	var err error
	var readData []byte
	for {
		_, readData, err = it.Next()
		if err != nil {
			break
		}
		assert.Equal(t, data5KB, readData)

		itNum++
	}

	assert.True(t, errors.Is(err, ErrWalIteratorEOF))
	assert.Equal(t, 1024, itNum)
}
