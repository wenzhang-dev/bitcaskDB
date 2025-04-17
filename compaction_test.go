package bitcask

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCompaction_OneFullRewriteWal(t *testing.T) {
	db := setupDB(t)
	defer teardownDB(db)

	// the wal capacity is 1MB
	// one wal can store up to 256 elements
	meta := NewMeta(nil)
	bin4K := GenNKBytes(4)
	ns := sha1Bytes("compaction")
	opts := &WriteOptions{}

	for i := 0; i < 100; i++ {
		key := sha1Bytes(strconv.Itoa(i))
		err := db.Put(ns[:], key[:], bin4K, meta, opts)
		assert.Nil(t, err)
	}

	// only one wal
	assert.Equal(t, len(db.manifest.wals), 1)

	// manual rotate wal
	_, err := db.manifest.RotateWal()
	assert.Nil(t, err)
	assert.Equal(t, len(db.manifest.wals), 2)

	// re-write the data in active
	// the data in the previous wal should be evicted
	for i := 0; i < 100; i++ {
		key := sha1Bytes(strconv.Itoa(i))
		err := db.Put(ns[:], key[:], bin4K, meta, opts)
		assert.Nil(t, err)
	}

	// reach here: only two wals
	// manual trigger compaction
	db.maybeScheduleCompaction()
	assert.True(t, db.compacting.Load())

	// wait up to 10 seconds
	waitTimes := 0
	for {
		if !db.compacting.Load() {
			break
		}

		waitTimes++
		if waitTimes > 10 {
			break
		}
		time.Sleep(time.Second)
	}

	// the compaction should be finished
	assert.True(t, waitTimes <= 10)
	assert.False(t, db.compacting.Load())

	// after compaction, only one wal lefts
	assert.Equal(t, len(db.manifest.wals), 1)

	// after compaction, we can get the data
	for i := 0; i < 100; i++ {
		key := sha1Bytes(strconv.Itoa(i))
		readVal, _, err := db.Get(ns[:], key[:], &ReadOptions{})

		assert.Nil(t, err)
		assert.Equal(t, bin4K, readVal)
	}
}

func TestCompaction_OneNonFullRewriteWal(t *testing.T) {
	db := setupDB(t)
	defer teardownDB(db)

	// the wal capacity is 1MB
	// one wal can store up to 256 elements
	meta := NewMeta(nil)
	bin4K := GenNKBytes(4)
	ns := sha1Bytes("compaction")
	opts := &WriteOptions{}

	for i := 0; i < 100; i++ {
		key := sha1Bytes(strconv.Itoa(i))
		err := db.Put(ns[:], key[:], bin4K, meta, opts)
		assert.Nil(t, err)
	}

	// only one wal
	assert.Equal(t, len(db.manifest.wals), 1)

	// manual rotate wal
	_, err := db.manifest.RotateWal()
	assert.Nil(t, err)
	assert.Equal(t, len(db.manifest.wals), 2)

	// re-write the data in active
	for i := 0; i < 90; i++ {
		key := sha1Bytes(strconv.Itoa(i))
		err := db.Put(ns[:], key[:], bin4K, meta, opts)
		assert.Nil(t, err)
	}

	// reach here: only two wals
	// manual trigger compaction
	db.maybeScheduleCompaction()
	assert.True(t, db.compacting.Load())

	// wait up to 10 seconds
	waitTimes := 0
	for {
		if !db.compacting.Load() {
			break
		}

		waitTimes++
		if waitTimes > 10 {
			break
		}
		time.Sleep(time.Second)
	}

	// the compaction should be finished
	assert.True(t, waitTimes <= 10)
	assert.False(t, db.compacting.Load())

	// after compaction, two wals left
	assert.Equal(t, len(db.manifest.wals), 2)

	// after compaction, we can get the data
	for i := 0; i < 100; i++ {
		key := sha1Bytes(strconv.Itoa(i))
		readVal, _, err := db.Get(ns[:], key[:], &ReadOptions{})

		assert.Nil(t, err)
		assert.Equal(t, bin4K, readVal)
	}
}

func TestCompaction_TwoFullRewriteWals(t *testing.T) {
	db := setupDB(t)
	defer teardownDB(db)

	// the wal capacity is 1MB
	// one wal can store up to 256 elements
	meta := NewMeta(nil)
	bin4K := GenNKBytes(4)
	ns := sha1Bytes("compaction")
	opts := &WriteOptions{}

	for i := 0; i < 100; i++ {
		key := sha1Bytes(strconv.Itoa(i))
		err := db.Put(ns[:], key[:], bin4K, meta, opts)
		assert.Nil(t, err)
	}

	// only one wal
	assert.Equal(t, len(db.manifest.wals), 1)

	// manual rotate wal
	_, err := db.manifest.RotateWal()
	assert.Nil(t, err)
	assert.Equal(t, len(db.manifest.wals), 2)

	// re-write the data in active
	for i := 0; i < 100; i++ {
		key := sha1Bytes(strconv.Itoa(i))
		err := db.Put(ns[:], key[:], bin4K, meta, opts)
		assert.Nil(t, err)
	}

	// manual rorate wal again
	_, err = db.manifest.RotateWal()
	assert.Nil(t, err)
	assert.Equal(t, len(db.manifest.wals), 3)

	// re-write the data in active
	for i := 0; i < 100; i++ {
		key := sha1Bytes(strconv.Itoa(i))
		err := db.Put(ns[:], key[:], bin4K, meta, opts)
		assert.Nil(t, err)
	}

	// reach here: only three wals
	// manual trigger compaction
	db.maybeScheduleCompaction()
	assert.True(t, db.compacting.Load())

	// wait up to 10 seconds
	waitTimes := 0
	for {
		if !db.compacting.Load() {
			break
		}

		waitTimes++
		if waitTimes > 10 {
			break
		}
		time.Sleep(time.Second)
	}

	// the compaction should be finished
	assert.True(t, waitTimes <= 10)
	assert.False(t, db.compacting.Load())

	// after compaction, only one wal lefts
	assert.Equal(t, len(db.manifest.wals), 1)

	// after compaction, we can get the data
	for i := 0; i < 100; i++ {
		key := sha1Bytes(strconv.Itoa(i))
		readVal, _, err := db.Get(ns[:], key[:], &ReadOptions{})

		assert.Nil(t, err)
		assert.Equal(t, bin4K, readVal)
	}
}

func TestCompaction_TwoNonFullRewriteWals(t *testing.T) {
	db := setupDB(t)
	defer teardownDB(db)

	// the wal capacity is 1MB
	// one wal can store up to 256 elements
	meta := NewMeta(nil)
	bin4K := GenNKBytes(4)
	ns := sha1Bytes("compaction")
	opts := &WriteOptions{}

	for i := 0; i < 100; i++ {
		key := sha1Bytes(strconv.Itoa(i))
		err := db.Put(ns[:], key[:], bin4K, meta, opts)
		assert.Nil(t, err)
	}

	// only one wal
	assert.Equal(t, len(db.manifest.wals), 1)

	// manual rotate wal
	_, err := db.manifest.RotateWal()
	assert.Nil(t, err)
	assert.Equal(t, len(db.manifest.wals), 2)

	// re-write the data in active
	for i := 0; i < 90; i++ {
		key := sha1Bytes(strconv.Itoa(i))
		err := db.Put(ns[:], key[:], bin4K, meta, opts)
		assert.Nil(t, err)
	}

	// manual rorate wal again
	_, err = db.manifest.RotateWal()
	assert.Nil(t, err)
	assert.Equal(t, len(db.manifest.wals), 3)

	// re-write the data in active
	for i := 0; i < 90; i++ {
		key := sha1Bytes(strconv.Itoa(i))
		err := db.Put(ns[:], key[:], bin4K, meta, opts)
		assert.Nil(t, err)
	}

	// reach here: only three wals
	// manual trigger compaction
	db.maybeScheduleCompaction()
	assert.True(t, db.compacting.Load())

	// wait up to 10 seconds
	waitTimes := 0
	for {
		if !db.compacting.Load() {
			break
		}

		waitTimes++
		if waitTimes > 10 {
			break
		}
		time.Sleep(time.Second)
	}

	// the compaction should be finished
	assert.True(t, waitTimes <= 10)
	assert.False(t, db.compacting.Load())

	// after compaction, two wals left
	assert.Equal(t, len(db.manifest.wals), 2)

	// after compaction, we can get the data
	for i := 0; i < 100; i++ {
		key := sha1Bytes(strconv.Itoa(i))
		readVal, _, err := db.Get(ns[:], key[:], &ReadOptions{})

		assert.Nil(t, err)
		assert.Equal(t, bin4K, readVal)
	}
}

func TestCompaction_TwoNonFullRewriteWals2(t *testing.T) {
	db := setupDB(t)
	defer teardownDB(db)

	// the wal capacity is 1MB
	// one wal can store up to 256 elements
	meta := NewMeta(nil)
	bin4K := GenNKBytes(4)
	ns := sha1Bytes("compaction")
	opts := &WriteOptions{}

	for i := 0; i < 100; i++ {
		key := sha1Bytes(strconv.Itoa(i))
		err := db.Put(ns[:], key[:], bin4K, meta, opts)
		assert.Nil(t, err)
	}

	// only one wal
	assert.Equal(t, len(db.manifest.wals), 1)

	// manual rotate wal
	_, err := db.manifest.RotateWal()
	assert.Nil(t, err)
	assert.Equal(t, len(db.manifest.wals), 2)

	// re-write the data in active
	bin2K := GenNKBytes(2)
	for i := 0; i < 100; i++ {
		// skip half elements
		if i%2 == 0 {
			continue
		}

		key := sha1Bytes(strconv.Itoa(i))
		err := db.Put(ns[:], key[:], bin2K, meta, opts)
		assert.Nil(t, err)
	}

	// manual rorate wal again
	_, err = db.manifest.RotateWal()
	assert.Nil(t, err)
	assert.Equal(t, len(db.manifest.wals), 3)

	// reach here: only three wals
	// manual trigger compaction
	db.maybeScheduleCompaction()
	assert.True(t, db.compacting.Load())

	// wait up to 10 seconds
	waitTimes := 0
	for {
		if !db.compacting.Load() {
			break
		}

		waitTimes++
		if waitTimes > 10 {
			break
		}
		time.Sleep(time.Second)
	}

	// the compaction should be finished
	assert.True(t, waitTimes <= 10)
	assert.False(t, db.compacting.Load())

	// after compaction, three wals left
	assert.Equal(t, len(db.manifest.wals), 3)

	// check the data
	for i := 0; i < 100; i++ {
		key := sha1Bytes(strconv.Itoa(i))
		readVal, _, err := db.Get(ns[:], key[:], &ReadOptions{})
		assert.Nil(t, err)

		if i%2 == 0 {
			assert.Equal(t, bin4K, readVal)
		} else {
			assert.Equal(t, bin2K, readVal)
		}
	}
}
