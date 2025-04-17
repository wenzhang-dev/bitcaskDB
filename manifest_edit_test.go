package bitcask

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestManifestEdit_EncodeAndDecode(t *testing.T) {
	edit := &ManifestEdit{
		addFiles:    []LogFile{{fid: 5}},
		deleteFiles: []LogFile{{fid: 3}, {fid: 4}},
		hasNextFid:  true,
		nextFid:     6,
		freeBytes:   make(map[uint64]uint64),
	}

	edit.freeBytes[2] = 567

	bytes := edit.Encode()

	decodeEdit := &ManifestEdit{}
	err := decodeEdit.DecodeFrom(bytes)
	assert.Nil(t, err)
	assert.Equal(t, edit, decodeEdit)
}

func TestManifestEdit_DecodeCorruptData(t *testing.T) {
	edit := &ManifestEdit{}
	err := edit.DecodeFrom([]byte{255, 255, 255})
	assert.NotNil(t, err)
}

func TestManifestEdit_Merge(t *testing.T) {
	edit1 := &ManifestEdit{
		addFiles:    []LogFile{{fid: 5}},
		deleteFiles: []LogFile{{fid: 3}, {fid: 4}},
		hasNextFid:  true,
		nextFid:     6,
		freeBytes:   make(map[uint64]uint64),
	}

	edit1.freeBytes[2] = 123

	edit2 := &ManifestEdit{
		addFiles:    []LogFile{{fid: 6}, {fid: 7}},
		deleteFiles: []LogFile{{fid: 5}},
		hasNextFid:  true,
		nextFid:     8,
		freeBytes:   make(map[uint64]uint64),
	}

	edit2.freeBytes[2] = 123

	edit1.Merge(edit2)

	// check
	assert.Equal(t, edit1.addFiles, []LogFile{{fid: 5}, {fid: 6}, {fid: 7}})
	assert.Equal(t, edit1.deleteFiles, []LogFile{{fid: 3}, {fid: 4}, {fid: 5}})
	assert.Equal(t, edit1.nextFid, uint64(8))
	assert.Equal(t, edit1.freeBytes[2], uint64(246))
	assert.True(t, edit1.hasNextFid)
}
