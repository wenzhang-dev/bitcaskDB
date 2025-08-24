package bitcask

import (
	"errors"
	"sync"
	"time"
)

const (
	// fid use 5 bytes
	BlockCacheFidBits  = 40
	BlockCacheFidMask  = (1 << BlockCacheFidBits) - 1
	BlockCacheFidShift = (64 - 40) // high 40-bits

	// each data block is 32KB, and 22 bits can locate 128 GB
	BlockCacheIdxBits  = 22
	BlockCacheIdxMask  = (1 << BlockCacheIdxBits) - 1
	BlockCacheIdxShift = 0 // low 22-bits

	// the extra 2 bits reserved
)

var (
	ErrBlockCacheMiss          = errors.New("block cache miss")
	ErrBlockCacheFidOutOfRange = errors.New("block cache fid out of range")
	ErrBlockCacheIdxOutOfRange = errors.New("block cache idx out of range")
)

func BlockCacheKey(fid, blkIdx uint64) (uint64, error) {
	if fid > BlockCacheFidMask {
		return 0, ErrBlockCacheFidOutOfRange
	}

	if blkIdx > BlockCacheIdxMask {
		return 0, ErrBlockCacheIdxOutOfRange
	}

	return (fid << BlockCacheFidShift) | blkIdx, nil
}

type BlockCacheOperator struct {
	helper MapOperatorBase
}

func (optr *BlockCacheOperator) Hash(key *uint64) uint64 {
	return *key
}

func (optr *BlockCacheOperator) Equals(lhs, rhs *uint64) bool {
	return *lhs == *rhs
}

func (optr *BlockCacheOperator) Rand(upper uint64) uint64 {
	return optr.helper.Rand(upper)
}

func (optr *BlockCacheOperator) WallTime() time.Time {
	return optr.helper.WallTime()
}

type BlockCache struct {
	maps *ShardMap[uint64, []byte]

	blkPool *sync.Pool
}

type BlockCacheOptions struct {
	Capacity             uint64
	Limited              uint64
	EvictionPoolCapacity uint64
	SampleKeys           uint64

	Helper MapOperatorBase
}

func NewBlockCache(opts *BlockCacheOptions) (*BlockCache, error) {
	blkPool := &sync.Pool{
		New: func() any {
			b := make([]byte, BlockSize)
			return &b
		},
	}

	if opts == nil || opts.Capacity == 0 {
		return &BlockCache{
			maps:    nil,
			blkPool: blkPool,
		}, nil
	}

	mapOpts := &MapOptions{
		Capacity:             opts.Capacity,
		Limited:              opts.Limited,
		EvictionPoolCapacity: opts.EvictionPoolCapacity,
		SampleKeys:           opts.SampleKeys,
	}

	optr := &BlockCacheOperator{
		helper: opts.Helper,
	}

	maps, err := NewShardMap[uint64, []byte](optr, mapOpts)
	if err != nil {
		return nil, err
	}

	return &BlockCache{
		maps:    maps,
		blkPool: blkPool,
	}, nil
}

func (c *BlockCache) key(fid, blkIdx uint64) (uint64, error) {
	if fid > BlockCacheFidMask {
		return 0, ErrBlockCacheFidOutOfRange
	}

	if blkIdx > BlockCacheIdxMask {
		return 0, ErrBlockCacheIdxOutOfRange
	}

	return (fid << BlockCacheFidShift) | blkIdx, nil
}

func (c *BlockCache) Get(fid, blkIdx uint64) ([]byte, error) {
	if c.maps == nil {
		return nil, ErrBlockCacheMiss
	}

	key, err := c.key(fid, blkIdx)
	if err != nil {
		return nil, err
	}

	if blkPtr, err := c.maps.Get(&key); err == nil {
		return *blkPtr, nil
	}

	return nil, ErrBlockCacheMiss
}

func (c *BlockCache) BatchGet(fid, blkStartIdx, blkNum uint64) ([][]byte, error) {
	if c.maps == nil {
		return nil, ErrBlockCacheMiss
	}

	blks := make([][]byte, blkNum)
	for i := uint64(0); i < blkNum; i++ {
		key, err := c.key(fid, blkStartIdx+i)
		if err != nil {
			return nil, err
		}

		blkPtr, err := c.maps.Get(&key)
		if err != nil {
			return nil, err
		}

		blks[i] = *blkPtr
	}

	return blks, nil
}

func (c *BlockCache) Put(fid, blkIdx, length uint64, blk []byte) error {
	if c.maps == nil {
		c.blkPool.Put(&blk)
		return nil
	}

	// the actual length is less than len(blk), and we should not put it into cache
	if int(length) != len(blk) {
		c.blkPool.Put(&blk)
		return nil
	}

	key, err := c.key(fid, blkIdx)
	if err != nil {
		c.blkPool.Put(&blk)
		return err
	}

	oldBlkPtr, err := c.maps.Set(&key, &blk)
	if err != nil {
		c.blkPool.Put(&blk)
		return err
	}

	// maybe no eviction
	if oldBlkPtr != nil {
		c.blkPool.Put(oldBlkPtr)
	}

	return nil
}

func (c *BlockCache) BatchPut(fid, blkIdx, length uint64, blks [][]byte) error {
	if c.maps == nil {
		for idx := range blks {
			c.blkPool.Put(&blks[idx])
		}
		return nil
	}

	// blks must include at least one elements
	blkNum := uint64(len(blks))
	if length != blkNum*uint64(len(blks[0])) {
		blkNum--
		c.blkPool.Put(&blks[len(blks)-1])
	}

	for i := uint64(0); i < blkNum; i++ {
		key, err := c.key(fid, blkIdx+i)
		if err != nil {
			c.blkPool.Put(&blks[i])
			continue
		}

		oldBlkPtr, err := c.maps.Set(&key, &blks[i])
		if err != nil {
			c.blkPool.Put(&blks[i])
			continue
		}

		if oldBlkPtr != nil {
			c.blkPool.Put(oldBlkPtr)
		}
	}

	return nil
}

func (c *BlockCache) NewBlock() []byte {
	blkPtr := c.blkPool.Get().(*[]byte)
	return *blkPtr
}
