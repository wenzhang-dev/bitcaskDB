package bitcask

import (
	"bytes"
	"time"

	"github.com/spaolacci/murmur3"
)

type IndexHelper interface {
	Rand(uint64) uint64
	WallTime() time.Time
}

type IndexOperator struct {
	helper IndexHelper
}

func (optr *IndexOperator) Hash(key *[]byte) uint64 {
	hasher := murmur3.New64()
	hasher.Write(*key)
	return hasher.Sum64()
}

func (optr *IndexOperator) Equals(lhs, rhs *[]byte) bool {
	return bytes.Equal(*lhs, *rhs)
}

func (optr *IndexOperator) Rand(upper uint64) uint64 {
	return optr.helper.Rand(upper)
}

func (optr *IndexOperator) WallTime() time.Time {
	return optr.helper.WallTime()
}

type IndexValue struct {
	fid       uint64
	valueOff  uint64
	valueSize uint64
}

type Index struct {
	maps *ShardMap[[]byte, IndexValue]
}

type IndexOptions struct {
	Capacity             uint64
	Limited              uint64
	EvictionPoolCapacity uint64
	SampleKeys           uint64

	Helper IndexHelper
}

// TODO: batch optimization
func NewIndex(opts *IndexOptions) (*Index, error) {
	mapOpts := &MapOptions{
		Capacity:             opts.Capacity,
		Limited:              opts.Limited,
		EvictionPoolCapacity: opts.EvictionPoolCapacity,
		SampleKeys:           opts.SampleKeys,
	}

	optr := &IndexOperator{
		helper: opts.Helper,
	}

	maps, err := NewShardMap[[]byte, IndexValue](optr, mapOpts)
	if err != nil {
		return nil, err
	}

	return &Index{maps}, nil
}

func (i *Index) Get(ns, key []byte) (fid uint64, off uint64, sz uint64, err error) {
	var val *IndexValue
	mergedKey := MergedKey(ns, key)

	if val, err = i.maps.Get(&mergedKey); err != nil {
		return
	}

	fid = val.fid
	off = val.valueOff
	sz = val.valueSize

	if off == 0 { // invalid offset
		err = ErrKeySoftDeleted
	}

	return
}

type WriteStat struct {
	// how much disk space is freed by this write
	FreeBytes uint64

	// which wal is affected
	FreeWalFid uint64
}

func (i *Index) Delete(ns, key []byte, stat *WriteStat) error {
	mergedKey := MergedKey(ns, key)
	old, err := i.maps.Delete(&mergedKey)
	if err != nil {
		return err
	}

	if stat != nil && old != nil {
		stat.FreeBytes = old.valueSize
		stat.FreeWalFid = old.fid
	}

	return nil
}

func (i *Index) SoftDelete(ns, key []byte, stat *WriteStat) error {
	mergedKey := MergedKey(ns, key)
	old, err := i.maps.Set(&mergedKey, &IndexValue{
		valueOff: 0, // invalid offset
	})
	if err != nil {
		return err
	}

	if stat != nil && old != nil {
		stat.FreeBytes = old.valueSize
		stat.FreeWalFid = old.fid
	}

	return nil
}

func (i *Index) Put(ns, key []byte, fid uint64, off uint64, sz uint64, stat *WriteStat) error {
	mergedKey := MergedKey(ns, key)
	old, err := i.maps.Set(&mergedKey, &IndexValue{
		valueOff:  off,
		valueSize: sz,
		fid:       fid,
	})
	if err != nil {
		return err
	}

	if stat != nil && old != nil {
		stat.FreeBytes = old.valueSize
		stat.FreeWalFid = old.fid
	}

	return nil
}

func (i *Index) Capacity() uint64 {
	return i.maps.Capacity()
}
