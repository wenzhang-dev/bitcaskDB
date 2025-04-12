package bitcask

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const (
	manifestEditDeleteFileTag = 1
	manifestEditAddFileTag    = 2
	manifestEditNextFidTag    = 3
	manifestEditFreeBytesTag  = 4
)

var (
	ErrCorruptedManifest      = errors.New("corrupted manifest file")
	ErrUnknownManifestEditTag = errors.New("unknown manifest tag")
)

type LogFile struct {
	wal *Wal

	fid uint64
}

// the manifest edit will be persist one by one, and append to
// MANIFEST file
type ManifestEdit struct {
	// the delete list of wal file
	deleteFiles []LogFile

	// the add list of wal file
	addFiles []LogFile

	// free bytes for each wal file
	freeBytes map[uint64]uint64

	// the available file number
	nextFid    uint64
	hasNextFid bool
}

func (edit *ManifestEdit) Encode() []byte {
	var buf bytes.Buffer
	encodeVarint := func(v uint64) {
		var tmp [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(tmp[:], v)
		buf.Write(tmp[:n])
	}

	if edit.hasNextFid {
		encodeVarint(manifestEditNextFidTag)
		encodeVarint(edit.nextFid)
	}

	for _, file := range edit.addFiles {
		encodeVarint(manifestEditAddFileTag)
		encodeVarint(file.fid)
	}

	for _, file := range edit.deleteFiles {
		encodeVarint(manifestEditDeleteFileTag)
		encodeVarint(file.fid)
	}

	for fid, freeBytes := range edit.freeBytes {
		encodeVarint(manifestEditFreeBytesTag)
		encodeVarint(fid)
		encodeVarint(freeBytes)
	}

	return buf.Bytes()
}

func (edit *ManifestEdit) Clear() {
	edit.nextFid = 0
	edit.hasNextFid = false

	edit.addFiles = nil
	edit.deleteFiles = nil
	edit.freeBytes = make(map[uint64]uint64)
}

func (edit *ManifestEdit) DecodeFrom(data []byte) error {
	offset := 0
	edit.Clear()
	for offset < len(data) {
		tag, nbytes := binary.Uvarint(data[offset:])
		if nbytes <= 0 {
			return ErrCorruptedManifest
		}
		offset += nbytes

		switch tag {
		case manifestEditDeleteFileTag:
			fid, nbytes := binary.Uvarint(data[offset:])
			if nbytes <= 0 {
				return ErrCorruptedManifest
			}
			offset += nbytes
			edit.deleteFiles = append(edit.deleteFiles, LogFile{fid: fid})
		case manifestEditAddFileTag:
			fid, nbytes := binary.Uvarint(data[offset:])
			if nbytes <= 0 {
				return ErrCorruptedManifest
			}
			offset += nbytes
			edit.addFiles = append(edit.addFiles, LogFile{fid: fid})
		case manifestEditNextFidTag:
			fid, nbytes := binary.Uvarint(data[offset:])
			if nbytes <= 0 {
				return ErrCorruptedManifest
			}
			offset += nbytes
			edit.hasNextFid = true
			edit.nextFid = max(edit.nextFid, fid)
		case manifestEditFreeBytesTag:
			fid, nbytes := binary.Uvarint(data[offset:])
			if nbytes <= 0 {
				return ErrCorruptedManifest
			}
			offset += nbytes

			freeBytes, nbytes := binary.Uvarint(data[offset:])
			if nbytes <= 0 {
				return ErrCorruptedManifest
			}
			offset += nbytes
			edit.freeBytes[fid] += freeBytes
		default:
			return ErrUnknownManifestEditTag
		}
	}

	return nil
}
