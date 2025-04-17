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
	assert.Equal(t, manifest.NextFid(), uint64(3))

	active := manifest.ActiveWal()
	assert.NotNil(t, active)
	assert.Equal(t, active.Fid(), uint64(2))

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

	active := manifest.ActiveWal()
	assert.NotNil(t, active)
	assert.Equal(t, active.Fid(), uint64(2))

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
	assert.Equal(t, old.refs.Load(), int64(1))

	active := manifest.ActiveWal()
	assert.NotNil(t, active)
	assert.Equal(t, active.refs.Load(), int64(1))

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

	active := manifest.ActiveWal()
	assert.Equal(t, active.refs.Load(), int64(1))
	assert.Equal(t, active.Fid(), uint64(2))

	defer manifest.Close()

	wal3, err := NewWal(WalPath(dir, 3), 3, -1)
	assert.Nil(t, err)

	wal4, err := NewWal(WalPath(dir, 4), 4, -1)
	assert.Nil(t, err)

	wal5, err := NewWal(WalPath(dir, 5), 5, -1)
	assert.Nil(t, err)

	// apply
	edit1 := &ManifestEdit{
		addFiles: []LogFile{
			{fid: 3, wal: wal3},
			{fid: 4, wal: wal4},
			{fid: 5, wal: wal5},
		},
		hasNextFid: true,
		nextFid:    6,
	}

	assert.Nil(t, manifest.Apply(edit1))
	assert.Equal(t, wal3.refs.Load(), int64(2))
	assert.Equal(t, wal4.refs.Load(), int64(2))
	assert.Equal(t, wal5.refs.Load(), int64(2))

	edit2 := &ManifestEdit{
		deleteFiles: []LogFile{{fid: 4}, {fid: 5}},
	}

	assert.Nil(t, manifest.Apply(edit2))
	assert.Equal(t, wal3.refs.Load(), int64(2))
	assert.Equal(t, wal4.refs.Load(), int64(1))
	assert.Equal(t, wal5.refs.Load(), int64(1))

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
	edit1 := &ManifestEdit{
		addFiles: []LogFile{
			{fid: 3, wal: wal3},
			{fid: 4, wal: wal4},
			{fid: 5, wal: wal5},
		},
		hasNextFid: true,
		nextFid:    6,
	}

	assert.Nil(t, manifest.LogAndApply(edit1))
	assert.Equal(t, wal3.refs.Load(), int64(2))
	assert.Equal(t, wal4.refs.Load(), int64(2))
	assert.Equal(t, wal5.refs.Load(), int64(2))

	edit2 := &ManifestEdit{
		deleteFiles: []LogFile{{fid: 4}, {fid: 5}},
	}

	assert.Nil(t, manifest.LogAndApply(edit2))
	assert.Equal(t, wal3.refs.Load(), int64(2))
	assert.Equal(t, wal4.refs.Load(), int64(1))
	assert.Equal(t, wal5.refs.Load(), int64(1))

	// re-open manifest
	manifest.Close() // all referenced wals will be closed

	manifest, err = LoadManifest(dir)
	assert.Nil(t, err)

	// check
	assert.Nil(t, manifest.ToWal(4))
	assert.Nil(t, manifest.ToWal(5))

	wal2 := manifest.ToWal(2)
	assert.NotNil(t, wal2)
	assert.Equal(t, wal2.refs.Load(), int64(1))

	wal3 = manifest.ToWal(3)
	assert.NotNil(t, wal3)
	assert.Equal(t, wal3.refs.Load(), int64(1))

	assert.Equal(t, manifest.nextFid, uint64(6))
}
