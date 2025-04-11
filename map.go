package bitcask

import (
	"errors"
	"sort"
	"sync"
	"time"
)

var ErrMapOptions = errors.New("invalid map options")

// the reason why we use the specific hashtable implementation:
// - it's hard to control golang map capacity and disable autoscale
// - support any type as hashable key
// - support the customized hash function
//
// comparation:
// - for open addressing hashtable, delete marker makes query performance decrease.
//   so it's not suitable for frequent deletion
// - for linked-base hashtable, memory overhead is larger because of more pointers,
//   which is less cpu cache-friendly

type MapOperator[K any] interface {
	// used to map key to slot
	Hash(key *K) uint64

	// used to compare key equalization
	Equals(lhs, rhs *K) bool

	// used to generate random slot
	Rand(uint64) uint64

	// second level clock
	WallTime() time.Time
}

type Bucket[K any, V any] struct {
	key    *K
	val    *V
	next   *Bucket[K, V]
	expire uint32
}

// the eviction poll is fixed size, which store some keys of random buckets
// when the number of entries in map reaches the limit, it will trigger eviction
// the sample random keys will be added to this eviction pool, and evict the
// entry of minimum expire
//
// notes: the eviction is based on the expire value. but the cached key of
// pool maybe be updated, the expire maybe not accurate
// we allow this scenario, and the map only is an approximate LRU
type EvictionPoolEntry[K any] struct {
	slot   uint64
	key    *K
	expire uint32
}

// it's thread-safe
type SimpleMap[K any, V any] struct {
	optr     MapOperator[K]
	capacity uint64

	// the length and capacity of this slice are fixed
	// it reduce the overhead of rehash
	buckets []Bucket[K, V]

	// the actual used number of buckets
	used uint64

	// the maximum number of used buckets
	// the prefer limited is less than capacity * 0.75, and the reservation
	// enhance hashtable performance when hash collision
	limited uint64

	// the eviction pool for fixed size
	evictPoolSize     uint64
	evictPoolCapacity uint64
	sampleKeys        uint64
	evictPool         []EvictionPoolEntry[K]

	// all expire is relative time of initial time
	initTime time.Time

	mu sync.Mutex
}

type MapOptions struct {
	// the number of bucket
	Capacity uint64

	// the number of elements
	Limited uint64

	EvictionPoolCapacity uint64
	SampleKeys           uint64
}

func (opt *MapOptions) validate() error {
	if opt.Limited > opt.Capacity {
		return ErrMapOptions
	}

	if opt.EvictionPoolCapacity > opt.Limited {
		return ErrMapOptions
	}

	if opt.SampleKeys < 1 {
		return ErrMapOptions
	}

	if opt.EvictionPoolCapacity < 16 {
		return ErrMapOptions
	}

	return nil
}

func NewMap[K any, V any](optr MapOperator[K], opts *MapOptions) (*SimpleMap[K, V], error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}

	return &SimpleMap[K, V]{
		optr:              optr,
		capacity:          opts.Capacity,
		buckets:           make([]Bucket[K, V], opts.Capacity),
		used:              0,
		limited:           opts.Limited,
		evictPoolCapacity: opts.EvictionPoolCapacity,
		evictPool:         make([]EvictionPoolEntry[K], opts.EvictionPoolCapacity),
		evictPoolSize:     0,
		sampleKeys:        opts.SampleKeys,
		initTime:          optr.WallTime(),
	}, nil
}

func (m *SimpleMap[K, V]) Size() uint64 {
	return m.used
}

func (m *SimpleMap[K, V]) Capacity() uint64 {
	return m.capacity
}

func (m *SimpleMap[K, V]) genExpire() uint32 {
	now := m.optr.WallTime()
	if now.Before(m.initTime) {
		return 0
	}

	return uint32(now.Sub(m.initTime).Seconds())
}

// the Set method should always work and return nil
// and it will return the previous value
func (m *SimpleMap[K, V]) Set(key *K, value *V) (*V, error) {
	slot := m.optr.Hash(key)
	return m.setWithSlot(key, value, slot)
}

func (m *SimpleMap[K, V]) setWithSlot(key *K, value *V, slot uint64) (*V, error) {
	var old *V
	var err error
	var entry *Bucket[K, V]

	slot %= m.capacity

	m.mu.Lock()
	defer m.mu.Unlock()

	entry, err = m.getEntryWithSlot(key, slot)
	if err == nil {
		// found the key
		old = entry.val

		entry.val = value
		entry.expire = m.genExpire()
		return old, nil
	}

	if m.used+1 > m.limited {
		old = m.evict()
	}

	// insert always wokrs
	m.used++

	// empty slot
	if m.buckets[slot].key == nil {
		m.buckets[slot].key = key
		m.buckets[slot].val = value
		m.buckets[slot].expire = m.genExpire()
		return old, nil
	}

	// insert slot at head
	entry = &Bucket[K, V]{
		key:    key,
		val:    value,
		next:   m.buckets[slot].next,
		expire: m.genExpire(),
	}

	m.buckets[slot].next = entry
	return old, nil
}

func (m *SimpleMap[K, V]) Get(key *K) (*V, error) {
	slot := m.optr.Hash(key)
	return m.getWithSlot(key, slot)
}

func (m *SimpleMap[K, V]) getWithSlot(key *K, slot uint64) (*V, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, err := m.getEntryWithSlot(key, slot)
	if err != nil {
		return nil, err
	}

	return entry.val, nil
}

func (m *SimpleMap[K, V]) getEntryWithSlot(key *K, slot uint64) (*Bucket[K, V], error) {
	slot %= m.capacity
	if m.buckets[slot].key == nil {
		return nil, ErrKeyNotFound
	}

	entry := &m.buckets[slot]
	for entry != nil {
		if m.optr.Equals(key, entry.key) {
			return entry, nil
		}

		entry = entry.next
	}

	return nil, ErrKeyNotFound
}

// delete the key and return the previous value
func (m *SimpleMap[K, V]) Delete(key *K) (*V, error) {
	slot := m.optr.Hash(key)
	return m.deleteWithSlot(key, slot)
}

func (m *SimpleMap[K, V]) deleteWithSlot(key *K, slot uint64) (*V, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.deleteWithSlotInternal(key, slot)
}

func (m *SimpleMap[K, V]) deleteWithSlotInternal(key *K, slot uint64) (old *V, err error) {
	slot %= m.capacity
	if m.buckets[slot].key == nil {
		return nil, ErrKeyNotFound
	}

	if m.optr.Equals(m.buckets[slot].key, key) {
		old = m.buckets[slot].val

		if m.buckets[slot].next != nil {
			m.buckets[slot] = *m.buckets[slot].next
		} else {
			m.buckets[slot].key = nil
			m.buckets[slot].val = nil
		}
		m.used--
		return
	}

	entry := &m.buckets[slot]
	for entry.next != nil {
		if m.optr.Equals(entry.next.key, key) {
			old = entry.next.val

			entry.next = entry.next.next
			m.used--
			return
		}
		entry = entry.next
	}

	return nil, ErrKeyNotFound
}

func (m *SimpleMap[K, V]) insertEvictionEntry(entry EvictionPoolEntry[K]) {
	// ascending order by expire
	// find the upper bound position
	idx := sort.Search(int(m.evictPoolSize), func(i int) bool {
		return entry.expire < m.evictPool[i].expire
	})

	// not found
	if idx == int(m.evictPoolSize) {
		idx = int(m.evictPoolSize - 1)
		if m.evictPoolSize != m.evictPoolCapacity {
			idx = int(m.evictPoolSize)
		}
	}

	if m.evictPoolSize != m.evictPoolCapacity {
		m.evictPoolSize++
	}

	// move [idx, size-1) to [idx+1, size)
	copy(m.evictPool[idx+1:], m.evictPool[idx:m.evictPoolSize-1])
	m.evictPool[idx] = entry
}

// return the eviction entry
func (m *SimpleMap[K, V]) evictMinExpireEntry() *V {
	var err error
	var old *V
	var pos uint64

	for pos < m.evictPoolSize {
		key := m.evictPool[pos].key
		slot := m.evictPool[pos].slot

		// ignore the deletion error
		if old, err = m.deleteWithSlotInternal(key, slot); err == nil {
			break
		}

		pos++
	}

	// remove the unused entries
	// the range [0, pos] will be removed
	copy(m.evictPool, m.evictPool[pos+1:m.evictPoolSize])
	m.evictPoolSize -= pos

	return old
}

// the eviction should always work and return the eviction value
//
// since each eviction adds up to the sample keys, at least one of the sample keys
// is guaranteed to be evicted, even if all the keys in previous eviction pool are
// removed
func (m *SimpleMap[K, V]) evict() *V {
	sampleKeys := m.sampleKeys
	for sampleKeys > 0 {
		slot := m.optr.Rand(m.capacity)
		entry := &m.buckets[slot]
		if entry.key == nil {
			continue
		}

		for sampleKeys > 0 && entry != nil {
			m.insertEvictionEntry(EvictionPoolEntry[K]{
				expire: entry.expire,
				key:    entry.key,
				slot:   slot,
			})

			entry = entry.next
			sampleKeys--
		}
	}

	return m.evictMinExpireEntry()
}

const (
	MapShardNum = 16
)

// it's thread-safe
type ShardMap[K any, V any] struct {
	opts   *MapOptions
	optr   MapOperator[K]
	shards [MapShardNum]*SimpleMap[K, V]
}

func NewShardMap[K any, V any](optr MapOperator[K], opts *MapOptions) (*ShardMap[K, V], error) {
	shardOpts := &MapOptions{
		Capacity:             opts.Capacity / MapShardNum,
		Limited:              opts.Limited / MapShardNum,
		EvictionPoolCapacity: opts.EvictionPoolCapacity,
		SampleKeys:           opts.SampleKeys,
	}

	shardMap := &ShardMap[K, V]{
		opts: opts,
		optr: optr,
	}

	var err error
	for idx := range shardMap.shards {
		shardMap.shards[idx], err = NewMap[K, V](optr, shardOpts)
		if err != nil {
			return nil, err
		}
	}

	return shardMap, nil
}

func (s *ShardMap[K, V]) Get(key *K) (*V, error) {
	slot := s.optr.Hash(key)
	shard := s.shards[slot%MapShardNum]
	return shard.getWithSlot(key, slot)
}

func (s *ShardMap[K, V]) Set(key *K, value *V) (*V, error) {
	slot := s.optr.Hash(key)
	shard := s.shards[slot%MapShardNum]
	return shard.setWithSlot(key, value, slot)
}

func (s *ShardMap[K, V]) Delete(key *K) (*V, error) {
	slot := s.optr.Hash(key)
	shard := s.shards[slot%MapShardNum]
	return shard.deleteWithSlot(key, slot)
}

func (s *ShardMap[K, V]) Capacity() uint64 {
	return s.opts.Capacity
}
