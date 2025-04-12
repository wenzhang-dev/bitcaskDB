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
