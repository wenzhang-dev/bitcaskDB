package bitcask

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setupManifestTxn(t *testing.T) *ManifestTxn {
	dir := "./test_bitcask_db"
	_ = os.RemoveAll(dir)

	assert.Nil(t, os.MkdirAll(dir, os.ModePerm))

	manifest, err := NewManifest(dir)
	assert.Nil(t, err)

	txn, err := manifest.NewTxn()
	assert.Nil(t, err)
	return txn
}

func TestManifestTxn_Commit(t *testing.T) {
	txn := setupManifestTxn(t)

	manifest := txn.manifest
	dir := manifest.dir

	wal3, _ := NewWal(WalPath(dir, 3), 3, -1)
	wal4, _ := NewWal(WalPath(dir, 4), 4, -1)
	wal5, _ := NewWal(WalPath(dir, 5), 5, -1)
	wal6, _ := NewWal(WalPath(dir, 6), 6, -1)
	wal7, _ := NewWal(WalPath(dir, 7), 7, -1)

	edit1 := &ManifestEdit{
		addFiles: []LogFile{
			{fid: 3, wal: wal3},
			{fid: 4, wal: wal4},
			{fid: 5, wal: wal5},
		},
		deleteFiles: nil,
		hasNextFid:  true,
		nextFid:     6,
		freeBytes:   make(map[uint64]uint64),
	}
	edit1.freeBytes[2] = 123

	// apply
	txn.Apply(edit1)

	// check
	assert.False(t, txn.aborted)
	assert.False(t, txn.committed)
	assert.False(t, txn.IsDone())
	assert.Equal(t, wal3, txn.ToWal(3))
	assert.Equal(t, wal4, txn.ToWal(4))
	assert.Equal(t, wal5, txn.ToWal(5))
	assert.Equal(t, txn.NextFid(), uint64(6))

	assert.Equal(t, wal3, manifest.ToWal(3))
	assert.Equal(t, wal4, manifest.ToWal(4))
	assert.Equal(t, wal5, manifest.ToWal(5))
	assert.Equal(t, manifest.NextFid(), uint64(6))

	// commit
	edit2 := &ManifestEdit{
		addFiles: []LogFile{
			{fid: 6, wal: wal6},
			{fid: 7, wal: wal7},
		},
		deleteFiles: []LogFile{
			{fid: 3, wal: wal3},
			{fid: 4, wal: wal4},
			{fid: 5, wal: wal5},
		},
		hasNextFid: true,
		nextFid:    8,
		freeBytes:  make(map[uint64]uint64),
	}

	edit2.freeBytes[2] = 123

	assert.Nil(t, txn.Commit(edit2))

	// check
	assert.True(t, txn.committed)
	assert.False(t, txn.aborted)
	assert.True(t, txn.IsDone())

	assert.Nil(t, manifest.ToWal(3))
	assert.Nil(t, manifest.ToWal(4))
	assert.Nil(t, manifest.ToWal(5))

	assert.Equal(t, wal6, manifest.ToWal(6))
	assert.Equal(t, wal7, manifest.ToWal(7))
	assert.Equal(t, manifest.NextFid(), uint64(8))

	// commit again
	err := txn.Commit(nil)
	assert.True(t, errors.Is(err, ErrCommittedManfiestTxn))
}

func TestManifestTxn_Abort(t *testing.T) {
	txn := setupManifestTxn(t)

	manifest := txn.manifest
	dir := manifest.dir

	wal3, _ := NewWal(WalPath(dir, 3), 3, -1)
	wal4, _ := NewWal(WalPath(dir, 4), 4, -1)
	wal5, _ := NewWal(WalPath(dir, 5), 5, -1)

	edit1 := &ManifestEdit{
		addFiles: []LogFile{
			{fid: 3, wal: wal3},
			{fid: 4, wal: wal4},
			{fid: 5, wal: wal5},
		},
		deleteFiles: nil,
		hasNextFid:  true,
		nextFid:     6,
		freeBytes:   make(map[uint64]uint64),
	}
	edit1.freeBytes[2] = 123

	// apply
	txn.Apply(edit1)

	// check
	assert.False(t, txn.aborted)
	assert.False(t, txn.committed)
	assert.False(t, txn.IsDone())
	assert.Equal(t, wal3, txn.ToWal(3))
	assert.Equal(t, wal4, txn.ToWal(4))
	assert.Equal(t, wal5, txn.ToWal(5))
	assert.Equal(t, txn.NextFid(), uint64(6))

	assert.Equal(t, wal3, manifest.ToWal(3))
	assert.Equal(t, wal4, manifest.ToWal(4))
	assert.Equal(t, wal5, manifest.ToWal(5))
	assert.Equal(t, manifest.NextFid(), uint64(6))

	// abort
	txn.Abort()

	// check
	assert.True(t, txn.aborted)
	assert.False(t, txn.committed)
	assert.True(t, txn.IsDone())

	assert.Nil(t, manifest.ToWal(3))
	assert.Nil(t, manifest.ToWal(4))
	assert.Nil(t, manifest.ToWal(5))
	assert.Equal(t, manifest.NextFid(), uint64(3))

	// abort again
	txn.Abort()
}
