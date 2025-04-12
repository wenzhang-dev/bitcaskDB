package bitcask

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func createTmpdir(t *testing.T) (dir string, closer func()) {
	dir = t.TempDir()
	closer = func() {
		os.RemoveAll(dir)
	}
	return
}

func TestManifest_NewManifest(t *testing.T) {
	dir, closer := createTmpdir(t)
	defer closer()

	manifest, err := NewManifest(dir)
	assert.Nil(t, err)

	defer manifest.Close()

	assert.Equal(t, manifest.fid, uint64(1))
	assert.Equal(t, manifest.nextFid, uint64(3))
	assert.NotNil(t, manifest.ActiveWal())
	assert.Equal(t, manifest.ActiveWal().Fid(), uint64(2))
	assert.True(t, manifest.FileSize() > 0)
}

func TestManifest_LoadManifest(t *testing.T) {
	dir, closer := createTmpdir(t)
	defer closer()

	manifest, err := NewManifest(dir)
	assert.Nil(t, err)

	manifest.Close()

	// load the previous manifest
	manifest, err = LoadManifest(dir)
	assert.Nil(t, err)

	defer manifest.Close()

	assert.Equal(t, manifest.fid, uint64(1))
	assert.Equal(t, manifest.nextFid, uint64(3))
	assert.NotNil(t, manifest.ActiveWal())
	assert.Equal(t, manifest.ActiveWal().Fid(), uint64(2))
	assert.True(t, manifest.FileSize() > 0)
}

func TestManifest_RotateWal(t *testing.T) {
	dir, closer := createTmpdir(t)
	defer closer()

	manifest, err := NewManifest(dir)
	assert.Nil(t, err)

	defer manifest.Close()

	old, err := manifest.RotateWal()
	assert.Nil(t, err)
	assert.Equal(t, old.Fid(), uint64(2))
	assert.Equal(t, manifest.nextFid, uint64(4))
}

func TestManifest_RotateManifest(t *testing.T) {
	dir, closer := createTmpdir(t)
	defer closer()

	manifest, err := NewManifest(dir)
	assert.Nil(t, err)

	defer manifest.Close()

	assert.Equal(t, manifest.fid, uint64(1))

	err = manifest.RotateManifest()
	assert.Nil(t, err)

	assert.Equal(t, manifest.fid, uint64(3))
	assert.Equal(t, manifest.nextFid, uint64(4))
}

func TestManifest_Apply(t *testing.T) {
	dir, closer := createTmpdir(t)
	defer closer()

	manifest, err := NewManifest(dir)
	assert.Nil(t, err)

	defer manifest.Close()

	wal3, err := NewWal(WalPath(dir, 3), 3, -1)
	assert.Nil(t, err)

	wal4, err := NewWal(WalPath(dir, 4), 4, -1)
	assert.Nil(t, err)

	wal5, err := NewWal(WalPath(dir, 5), 5, -1)
	assert.Nil(t, err)

	// apply
	assert.Equal(t, manifest.ActiveWal().Fid(), uint64(2))
	edit1 := &ManifestEdit{
		addFiles: []LogFile{
			{fid: 3, wal: wal3},
			{fid: 4, wal: wal4},
			{fid: 5, wal: wal5},
		},
		hasNextFid: true,
		nextFid:    6,
	}

	err = manifest.Apply(edit1)
	assert.Nil(t, err)

	edit2 := &ManifestEdit{
		deleteFiles: []LogFile{{fid: 4}, {fid: 5}},
	}

	err = manifest.Apply(edit2)
	assert.Nil(t, err)

	// check
	assert.Nil(t, manifest.ToWal(4))
	assert.Nil(t, manifest.ToWal(5))
	assert.NotNil(t, manifest.ToWal(3))
	assert.NotNil(t, manifest.ToWal(2))
	assert.Equal(t, manifest.nextFid, uint64(6))
}

func TestManifest_LogAndApply(t *testing.T) {
	dir, closer := createTmpdir(t)
	defer closer()

	manifest, err := NewManifest(dir)
	assert.Nil(t, err)

	wal3, err := NewWal(WalPath(dir, 3), 3, -1)
	assert.Nil(t, err)

	wal4, err := NewWal(WalPath(dir, 4), 4, -1)
	assert.Nil(t, err)

	wal5, err := NewWal(WalPath(dir, 5), 5, -1)
	assert.Nil(t, err)

	// apply
	assert.Equal(t, manifest.ActiveWal().Fid(), uint64(2))
	edit1 := &ManifestEdit{
		addFiles: []LogFile{
			{fid: 3, wal: wal3},
			{fid: 4, wal: wal4},
			{fid: 5, wal: wal5},
		},
		hasNextFid: true,
		nextFid:    6,
	}

	err = manifest.LogAndApply(edit1)
	assert.Nil(t, err)

	edit2 := &ManifestEdit{
		deleteFiles: []LogFile{{fid: 4}, {fid: 5}},
	}

	err = manifest.LogAndApply(edit2)
	assert.Nil(t, err)

	// re-open manifest
	manifest.Close()
	manifest, err = LoadManifest(dir)
	assert.Nil(t, err)

	// check
	assert.Nil(t, manifest.ToWal(4))
	assert.Nil(t, manifest.ToWal(5))
	assert.NotNil(t, manifest.ToWal(3))
	assert.NotNil(t, manifest.ToWal(2))
	assert.Equal(t, manifest.nextFid, uint64(6))
}
