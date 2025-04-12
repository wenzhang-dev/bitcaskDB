package bitcask

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_ParseFilename(t *testing.T) {
	hintName := "00001.hint"
	mergeName := "00002.merge"
	walName := "00003.wal"
	manifestName := "MANIFEST-000004"
	tmpName := "00005.tmp"
	lockName := "LOCK"
	currentName := "CURRENT"
	unknownName := "test"

	ft, fid, err := ParseFilename(hintName)
	assert.Nil(t, err)
	assert.Equal(t, ft, HintFileType)
	assert.Equal(t, fid, uint64(1))

	ft, fid, err = ParseFilename(mergeName)
	assert.Nil(t, err)
	assert.Equal(t, ft, MergeFileType)
	assert.Equal(t, fid, uint64(2))

	ft, fid, err = ParseFilename(walName)
	assert.Nil(t, err)
	assert.Equal(t, ft, WalFileType)
	assert.Equal(t, fid, uint64(3))

	ft, fid, err = ParseFilename(manifestName)
	assert.Nil(t, err)
	assert.Equal(t, ft, ManifestFileType)
	assert.Equal(t, fid, uint64(4))

	ft, fid, err = ParseFilename(tmpName)
	assert.Nil(t, err)
	assert.Equal(t, ft, TmpFileType)
	assert.Equal(t, fid, uint64(5))

	ft, fid, err = ParseFilename(lockName)
	assert.Nil(t, err)
	assert.Equal(t, ft, LockFileType)
	assert.Equal(t, fid, uint64(0))

	ft, fid, err = ParseFilename(currentName)
	assert.Nil(t, err)
	assert.Equal(t, ft, CurrentFileType)
	assert.Equal(t, fid, uint64(0))

	ft, fid, err = ParseFilename(unknownName)
	assert.Nil(t, err)
	assert.Equal(t, ft, UnknownFileType)
	assert.Equal(t, fid, uint64(0))
}
