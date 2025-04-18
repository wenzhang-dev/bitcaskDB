package bitcask

import (
	"crypto/sha1"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func sha1Bytes(input string) [20]byte {
	return sha1.Sum([]byte(input))
}

func setupDB(t *testing.T) *DBImpl {
	dir := "./test_bitcask_db"
	_ = os.RemoveAll(dir)

	assert.Nil(t, os.MkdirAll(dir, os.ModePerm))

	opts := &Options{
		Dir:                       dir,
		WalMaxSize:                1024 * 1024, // 1MB
		ManifestMaxSize:           1024 * 1024, // 1MB
		IndexCapacity:             1000000,
		IndexLimited:              800000,
		IndexEvictionPoolCapacity: 32,
		IndexSampleKeys:           5,
		NsSize:                    DefaultNsSize,
		EtagSize:                  DefaultEtagSize,
	}

	db, err := NewDB(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	return db
}

func loadDB(t *testing.T) *DBImpl {
	dir := "./test_bitcask_db"

	opts := &Options{
		Dir:                       dir,
		WalMaxSize:                1024 * 1024, // 1MB
		ManifestMaxSize:           1024 * 1024, // 1MB
		IndexCapacity:             1000000,
		IndexLimited:              800000,
		IndexEvictionPoolCapacity: 32,
		IndexSampleKeys:           5,
		NsSize:                    DefaultNsSize,
		EtagSize:                  DefaultEtagSize,
	}

	db, err := NewDB(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	return db
}

func teardownDB(db *DBImpl) {
	db.Close()
	_ = os.RemoveAll(db.opts.Dir)
}

func TestDBImplBasicWriteRead(t *testing.T) {
	db := setupDB(t)
	defer teardownDB(db)

	now := uint64(db.WallTime().Unix())
	ns := sha1Bytes("namespace")
	key := []byte("testKey")
	value := []byte("testValue")
	etag := sha1Bytes(string(value))
	meta := NewMeta(nil).SetExpire(now + 60).SetEtag(etag[:])

	err := db.Put(ns[:], key, value, meta, &WriteOptions{})
	assert.NoError(t, err)

	// write without options
	err = db.Put(ns[:], key, value, meta, nil)
	assert.Nil(t, err)

	readVal, readMeta, err := db.Get(ns[:], key, &ReadOptions{})
	assert.NoError(t, err)
	assert.Equal(t, value, readVal)
	assert.Equal(t, meta, readMeta)

	// read without options
	readVal, readMeta, err = db.Get(ns[:], key, nil)
	assert.NoError(t, err)
	assert.Equal(t, value, readVal)
	assert.Equal(t, meta, readMeta)
}

func TestDBImplWriteEmptyValue(t *testing.T) {
	db := setupDB(t)
	defer teardownDB(db)

	ns := sha1Bytes("namespace")
	key := []byte("testKey")
	meta := NewMeta(nil)

	// no etag, no expire and no value
	err := db.Put(ns[:], key, nil, meta, &WriteOptions{})
	assert.NoError(t, err)

	readVal, _, err := db.Get(ns[:], key, &ReadOptions{})
	assert.NoError(t, err)
	assert.Equal(t, len(readVal), 0)
}

func TestDBImplWriteDeleteRead(t *testing.T) {
	db := setupDB(t)
	defer teardownDB(db)

	ns := sha1Bytes("namespace")
	key := []byte("testKey")
	value := []byte("testValue")
	meta := NewMeta(nil)

	_ = db.Put(ns[:], key, value, meta, &WriteOptions{})
	_ = db.Delete(ns[:], key, &WriteOptions{})

	readVal, _, err := db.Get(ns[:], key, &ReadOptions{})
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.Nil(t, readVal)
}

func TestDBImplWALRotate(t *testing.T) {
	db := setupDB(t)
	defer teardownDB(db)

	ns := sha1Bytes("wal-rotation")
	meta := NewMeta(nil)
	opts := &WriteOptions{}

	initFid := db.manifest.active.Fid()

	// 50000 > (1MB Wal / min 50B per record = 20000)
	for i := 0; i < 50000; i++ {
		key := sha1Bytes("key" + strconv.Itoa(i))
		value := sha1Bytes("val" + strconv.Itoa(i))
		err := db.Put(ns[:], key[:], value[:], meta, opts)
		assert.Nil(t, err)
	}

	assert.NotEqual(t, initFid, db.manifest.active.Fid())
}

func TestDBImplPersistence(t *testing.T) {
	db := setupDB(t)

	bin4K := GenNKBytes(4)
	appMeta := make(map[string]string)
	appMeta["test"] = string(bin4K)

	ns := sha1Bytes("persistence")
	meta := NewMeta(appMeta)
	opts := &WriteOptions{}

	// write 10000 keys
	for i := 0; i < 10000; i++ {
		key := sha1Bytes("key" + strconv.Itoa(i))
		value := sha1Bytes("val" + strconv.Itoa(i))
		err := db.Put(ns[:], key[:], value[:], meta, opts)

		assert.Nil(t, err)
	}

	// check
	for i := 0; i < 10000; i++ {
		key := sha1Bytes("key" + strconv.Itoa(i))
		value := sha1Bytes("val" + strconv.Itoa(i))
		readVal, readMeta, err := db.Get(ns[:], key[:], &ReadOptions{})

		assert.Nil(t, err)
		assert.Equal(t, readVal, value[:])
		assert.Equal(t, readMeta.AppMeta, meta.AppMeta)
	}

	// re-open db
	db.Close()

	db = loadDB(t)
	defer teardownDB(db)

	// check again
	for i := 0; i < 10000; i++ {
		key := sha1Bytes("key" + strconv.Itoa(i))
		value := sha1Bytes("val" + strconv.Itoa(i))
		readVal, readMeta, err := db.Get(ns[:], key[:], &ReadOptions{})

		assert.Nil(t, err)
		assert.Equal(t, readVal, value[:])
		assert.Equal(t, readMeta.AppMeta, meta.AppMeta)
	}
}

/*
func TestDBImplBatchWrite(t *testing.T) {
}
*/

func TestDBImplConcurrentWriteAndRead(t *testing.T) {
	db := setupDB(t)
	defer teardownDB(db)

	var wg sync.WaitGroup
	ns := sha1Bytes("concurrent")
	meta := NewMeta(nil)

	genBytes := func(i, j int) []byte {
		return []byte("i" + strconv.Itoa(i) + "j" + strconv.Itoa(j))
	}

	// total: 25000 keys
	for i := 0; i < 50; i++ {
		wg.Add(1)
		// each goroutine writes 500 keys
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				// key equals it's value
				key := genBytes(i, j)
				value := genBytes(i, j)
				err := db.Put(ns[:], key, value, meta, &WriteOptions{})
				assert.Nil(t, err)
			}

			// check
			for j := 0; j < 500; j++ {
				key := genBytes(i, j)
				val, _, err := db.Get(ns[:], key, &ReadOptions{})
				if err != nil {
					print(err.Error())
				}
				assert.Nil(t, err)
				assert.Equal(t, val, key)
			}
		}(i)
	}

	wg.Wait()
}
