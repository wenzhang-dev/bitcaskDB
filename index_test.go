package bitcask

import (
	"errors"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type mockIndexHelper struct{}

func (mock *mockIndexHelper) Rand(upper uint64) uint64 {
	return uint64(rand.Int63n(int64(upper)))
}

func (mock *mockIndexHelper) WallTime() time.Time {
	return time.Now()
}

func setupIndex(t *testing.T) *Index {
	index, err := NewIndex(&IndexOptions{
		Capacity:             1000,
		Limited:              800,
		EvictionPoolCapacity: 32,
		SampleKeys:           5,
		Helper:               &mockIndexHelper{},
	})

	assert.Nil(t, err)
	return index
}

func TestIndexBasicOperations(t *testing.T) {
	index := setupIndex(t)

	ns1 := []byte("ns1")
	key1, key2 := []byte("key1"), []byte("key2")

	// Insert values
	err := index.Put(ns1, key1, 1, 100, 100, nil)
	assert.NoError(t, err)

	err = index.Put(ns1, key2, 2, 200, 100, nil)
	assert.NoError(t, err)

	// Retrieve values
	fid, off, _, err := index.Get(ns1, key1)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), fid)
	assert.Equal(t, uint64(100), off)

	fid, off, _, err = index.Get(ns1, key2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), fid)
	assert.Equal(t, uint64(200), off)

	// Update an existing key
	err = index.Put(ns1, key1, 3, 300, 100, nil)
	assert.NoError(t, err)

	fid, off, _, err = index.Get(ns1, key1)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), fid)
	assert.Equal(t, uint64(300), off)
}

func TestIndexDeleteOperations(t *testing.T) {
	index := setupIndex(t)

	ns1 := []byte("ns1")
	key1, key2 := []byte("key1"), []byte("key2")

	// Insert and delete key
	err := index.Put(ns1, key1, 1, 100, 100, nil)
	assert.NoError(t, err)

	err = index.Delete(ns1, key1, nil)
	assert.NoError(t, err)

	_, _, _, err = index.Get(ns1, key1)
	assert.Error(t, err) // Should return error since key is deleted

	// Soft delete (overwrite with invalid offset)
	err = index.Put(ns1, key2, 2, 200, 100, nil)
	assert.NoError(t, err)

	err = index.SoftDelete(ns1, key2, nil)
	assert.NoError(t, err)

	_, _, _, err = index.Get(ns1, key2)
	assert.NotNil(t, err)
	assert.True(t, errors.Is(err, ErrKeySoftDeleted))
}

func TestIndexEviction(t *testing.T) {
	index := setupIndex(t)

	ns1 := []byte("ns1")
	totalFreeBytes := uint64(0)

	// the Limited is 800, but 1 million keys has been written
	// the value size is 100 bytes, so the free bytes should equal to 100 * N
	for i := 1; i <= 1000000; i++ {
		key := []byte("key" + strconv.Itoa(i))
		stat := &WriteStat{}
		err := index.Put(ns1, key, 1, uint64(i*100), 100, stat)
		assert.Nil(t, err)

		totalFreeBytes += stat.FreeBytes
	}

	assert.Equal(t, totalFreeBytes, uint64(100*(1000000-800)))
}
