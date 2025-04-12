package bitcask

import (
	"bytes"
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/spaolacci/murmur3"

	"github.com/stretchr/testify/assert"
)

type mockSimpleMapOperator struct {
	fixedValues []uint64
	index       int
}

func (op *mockSimpleMapOperator) Hash(key *uint64) uint64 {
	return *key
}

func (op *mockSimpleMapOperator) Equals(lhs, rhs *uint64) bool {
	return *lhs == *rhs
}

func (op *mockSimpleMapOperator) Rand(n uint64) uint64 {
	if len(op.fixedValues) == 0 {
		return 0 // Default to first bucket if no fixed values provided
	}
	if op.index >= len(op.fixedValues) {
		op.index = 0
	}
	val := op.fixedValues[op.index] % n
	op.index++
	return val
}

func (op *mockSimpleMapOperator) WallTime() time.Time {
	return time.Now()
}

func TestMap_SimpleMapBasicOperations(t *testing.T) {
	evictionOrder := []uint64{1, 2, 3} // Define a fixed eviction order
	optr := &mockSimpleMapOperator{fixedValues: evictionOrder}
	opts := &MapOptions{
		Capacity:             100,
		Limited:              80,
		EvictionPoolCapacity: 16,
		SampleKeys:           3,
	}

	m, err := NewMap[uint64, uint64](optr, opts)
	assert.Nil(t, err)

	key1, val1 := uint64(1), uint64(1)
	key2, val2 := uint64(2), uint64(2)
	key3, val3 := uint64(3), uint64(3)

	// Test Set and Get
	_, _ = m.Set(&key1, &val1)
	_, _ = m.Set(&key2, &val2)
	_, _ = m.Set(&key3, &val3)

	res, err := m.Get(&key1)
	assert.Nil(t, err)
	assert.Equal(t, val1, *res)

	res, err = m.Get(&key2)
	assert.Nil(t, err)
	assert.Equal(t, val2, *res)

	// Update existing element
	val1Updated := uint64(11)
	old, err := m.Set(&key1, &val1Updated)
	assert.Nil(t, err)
	assert.Equal(t, *old, val1)

	res, err = m.Get(&key1)
	assert.Nil(t, err)
	assert.Equal(t, val1Updated, *res)

	// Exceed Limited and trigger eviction
	for i := 4; i <= 81; i++ {
		key, val := uint64(i), uint64(i)
		_, err = m.Set(&key, &val)
		assert.Nil(t, err)
	}

	_, err = m.Get(&evictionOrder[0])
	assert.NotNil(t, err) // Since eviction occurs, the first evicted key should be removed

	// Test Delete
	old, err = m.Delete(&key2)
	assert.Nil(t, err)
	assert.Equal(t, *old, val2)

	_, err = m.Get(&key2)
	assert.NotNil(t, err)
	assert.True(t, errors.Is(err, ErrKeyNotFound))
}

func TestMap_SimpleMapEvictionOrder(t *testing.T) {
	evictionOrder := []uint64{1, 2, 3, 4, 5, 6} // Fixed eviction order
	optr := &mockSimpleMapOperator{fixedValues: evictionOrder}
	opts := &MapOptions{
		Capacity:             100,
		Limited:              80,
		EvictionPoolCapacity: 16,
		SampleKeys:           3,
	}

	m, err := NewMap[uint64, uint64](optr, opts)
	assert.Nil(t, err)

	// reach the limit
	for i := 1; i <= 80; i++ {
		key, val := uint64(i), uint64(i)

		old, err := m.Set(&key, &val)
		assert.Nil(t, err)
		assert.Nil(t, old)
	}

	// insert a new key, triggering eviction
	key81, val81 := uint64(81), uint64(81)
	old, err := m.Set(&key81, &val81)
	assert.Nil(t, err)
	assert.Equal(t, *old, uint64(1))

	// the first evicted key should be 1
	key1 := uint64(1)
	_, err = m.Get(&key1)
	assert.NotNil(t, err)
	assert.True(t, errors.Is(err, ErrKeyNotFound))

	// the remaining keys should still exist
	for i := 2; i <= 81; i++ {
		key, val := uint64(i), uint64(i)
		res, err := m.Get(&key)
		assert.Nil(t, err)
		assert.Equal(t, *res, val)
	}

	// insert more keys to trigger further eviction
	key82, val82 := uint64(82), uint64(82)
	old, err = m.Set(&key82, &val82)
	assert.Nil(t, err)
	assert.Equal(t, *old, uint64(2))

	// the second evicted key should be 2
	key2 := uint64(2)
	_, err = m.Get(&key2)
	assert.NotNil(t, err)
	assert.True(t, errors.Is(err, ErrKeyNotFound))

	// ensure remaining keys are still available
	for i := 3; i <= 82; i++ {
		key, val := uint64(i), uint64(i)
		res, err := m.Get(&key)
		assert.Nil(t, err)
		assert.Equal(t, *res, val)
	}
}

type mockShardMapOperator struct{}

func (op *mockShardMapOperator) Hash(key *[]byte) uint64 {
	hasher := murmur3.New64()
	hasher.Write(*key)
	return hasher.Sum64()
}

func (op *mockShardMapOperator) Equals(lhs, rhs *[]byte) bool {
	return bytes.Equal(*lhs, *rhs)
}

func (op *mockShardMapOperator) Rand(n uint64) uint64 {
	return uint64(rand.Int63n(int64(n)))
}

func (op *mockShardMapOperator) WallTime() time.Time {
	return time.Now()
}

func TestMap_ShardMapBasic(t *testing.T) {
	opts := &MapOptions{
		Capacity:             1000,
		Limited:              800,
		EvictionPoolCapacity: 16,
		SampleKeys:           3,
	}

	m, err := NewShardMap[[]byte, []byte](&mockShardMapOperator{}, opts)
	assert.Nil(t, err)

	key1, val1 := []byte("123"), []byte("123")
	key2, val2 := []byte("456"), []byte("456")
	key3, val3 := []byte("789"), []byte("789")

	// Test Set and Get
	_, _ = m.Set(&key1, &val1)
	_, _ = m.Set(&key2, &val2)
	_, _ = m.Set(&key3, &val3)

	res, err := m.Get(&key1)
	assert.Nil(t, err)
	assert.Equal(t, val1, *res)

	res, err = m.Get(&key2)
	assert.Nil(t, err)
	assert.Equal(t, val2, *res)

	// Update existing element
	val1Updated := []byte("111")
	old, err := m.Set(&key1, &val1Updated)
	assert.Nil(t, err)
	assert.Equal(t, *old, val1)

	res, err = m.Get(&key1)
	assert.Nil(t, err)
	assert.Equal(t, val1Updated, *res)

	// Test Delete
	old, err = m.Delete(&key2)
	assert.Nil(t, err)
	assert.Equal(t, *old, val2)

	_, err = m.Get(&key2)
	assert.NotNil(t, err)
	assert.True(t, errors.Is(err, ErrKeyNotFound))
}

func TestMap_ShardMapLRUEviction(t *testing.T) {
	opts := &MapOptions{
		Capacity:             1000000,
		Limited:              800000,
		EvictionPoolCapacity: 32,
		SampleKeys:           5,
	}

	m, err := NewShardMap[[]byte, []byte](&mockShardMapOperator{}, opts)
	assert.Nil(t, err)

	for i := 1; i < 1000000; i++ {
		numStr := strconv.Itoa(i)
		key, val := []byte(numStr), []byte(numStr)

		_, err := m.Set(&key, &val)
		assert.Nil(t, err)
	}

	// the first half of the elements are evicted more
	num := 0
	for i := 1; i <= 500000; i++ {
		numStr := strconv.Itoa(i)
		key, val := []byte(numStr), []byte(numStr)
		res, err := m.Get(&key)
		if err != nil && errors.Is(err, ErrKeyNotFound) {
			num++
		}

		if err == nil {
			assert.Equal(t, *res, val)
		}
	}

	// total eviction elements should be 200000 (capacity - limited)
	assert.True(t, num > 100000)
}

func TestMap_ShardMapConcurrentReadAndWrite(t *testing.T) {
	opts := &MapOptions{
		Capacity:             1000000,
		Limited:              800000,
		EvictionPoolCapacity: 32,
		SampleKeys:           5,
	}

	genBytes := func(i, j int) []byte {
		return []byte("i" + strconv.Itoa(i) + "j" + strconv.Itoa(j))
	}

	m, err := NewShardMap[[]byte, []byte](&mockShardMapOperator{}, opts)
	assert.Nil(t, err)

	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				key := genBytes(i, j)
				val := genBytes(i, j)

				_, err := m.Set(&key, &val)
				assert.Nil(t, err)
			}

			for j := 0; j < 1000; j++ {
				key := genBytes(i, j)
				val, err := m.Get(&key)
				assert.Nil(t, err)
				assert.Equal(t, key, *val)
			}
		}(i)
	}

	wg.Wait()
}
