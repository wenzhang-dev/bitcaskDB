package bitcask

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// it's not thread-safe
// the MANIFEST is append-only file
// it includes multiple edits, which record how the database changes
type Manifest struct {
	// the base directory
	dir string

	// manifest file
	fp *os.File

	// the manifest fid
	fid uint64

	// manifest file size
	size uint64

	// the active wal log
	// active and hint log file use the same log number
	// usually, the log number is largest
	active *Wal

	// the next allocatable file number
	nextFid uint64

	// all wal files
	// mapping from fid to walInfo
	wals map[uint64]*WalInfo
}

type WalInfo struct {
	wal *Wal

	// the total unuse size of wal file
	// if the original data is updated or deleted, the related wal
	// can free some disk space
	freeBytes uint64

	// delta free bytes of wal file
	// it indicates the unuse size of wal file, which is not persisted to manifest yet
	deltaFreeBytes uint64
}

func NewManifest(dir string) (*Manifest, error) {
	runners := NewRunner()
	defer runners.Do()

	manifestDir := ManifestPath(dir, 1)
	fp, err := os.OpenFile(manifestDir, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	runners.Post(func() {
		fp.Close()
		os.Remove(manifestDir)
	})

	active, err := NewWal(WalPath(dir, 2), 2, time.Now().Unix())
	if err != nil {
		return nil, err
	}

	runners.Post(func() {
		active.Unref()
	})

	manifest := &Manifest{
		dir:     dir,
		fp:      fp,
		nextFid: 3,
		wals:    make(map[uint64]*WalInfo),
		size:    0,
		active:  active,
	}

	edit := &ManifestEdit{
		addFiles:   []LogFile{{active, 2}},
		hasNextFid: true,
		nextFid:    3,
	}

	// write the MANIFEST file
	if err = manifest.LogAndApply(edit); err != nil {
		return nil, err
	}

	// write the CURRENT file
	if err = os.WriteFile(CurrentPath(dir), []byte(ManifestFilename(1)), 0o644); err != nil {
		return nil, err
	}

	// abort all functors
	runners.Rollback()

	return manifest, nil
}

func NewManifestIfNotExists(dir string) (*Manifest, error) {
	if PathExists(CurrentPath(dir)) {
		return LoadManifest(dir)
	}

	return NewManifest(dir)
}

// load the MANIFEST file according to CURRENT file
func LoadManifest(dir string) (*Manifest, error) {
	runners := NewRunner()
	defer runners.Do()

	data, err := os.ReadFile(CurrentPath(dir))
	if err != nil {
		return nil, fmt.Errorf("failed to read CURRENT file: %w", err)
	}

	manifestPath := filepath.Join(dir, strings.TrimSpace(string(data)))
	fp, err := os.OpenFile(manifestPath, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open manifest file: %w", err)
	}

	runners.Post(func() {
		fp.Close()
	})

	fileInfo, err := fp.Stat()
	if err != nil {
		return nil, err
	}

	manifest := &Manifest{
		dir:     dir,
		fp:      fp,
		wals:    make(map[uint64]*WalInfo),
		nextFid: 0,
		size:    uint64(fileInfo.Size()),
	}

	if err = manifest.recoverFromManifest(); err != nil {
		return nil, err
	}

	if len(manifest.wals) > 0 {
		maxFid := uint64(0)
		for fid := range manifest.wals {
			maxFid = max(maxFid, fid)
		}
		manifest.active = manifest.wals[maxFid].wal
	}

	// abort all functors
	runners.Rollback()

	return manifest, nil
}

func (m *Manifest) recoverFromManifest() error {
	buf, err := io.ReadAll(m.fp)
	if err != nil {
		return err
	}

	edit := &ManifestEdit{}
	err = edit.DecodeFrom(buf)
	if err != nil {
		return err
	}

	deleteFiles := edit.deleteFiles
	sort.Slice(deleteFiles, func(i, j int) bool {
		return deleteFiles[i].fid < deleteFiles[j].fid
	})

	var found bool
	var addFiles []LogFile
	for idx := range edit.addFiles {
		_, found = sort.Find(len(deleteFiles), func(i int) int {
			return int(edit.addFiles[idx].fid) - int(deleteFiles[i].fid)
		})

		if !found {
			addFiles = append(addFiles, edit.addFiles[idx])
		}
	}

	// hit all delete wals
	if len(addFiles)+len(deleteFiles) != len(edit.addFiles) {
		return ErrCorruptedManifest
	}

	// optimize the edit and load related wals
	for idx := range addFiles {
		wal, err := LoadWal(WalPath(m.dir, addFiles[idx].fid), addFiles[idx].fid)
		if err != nil {
			return err
		}
		addFiles[idx].wal = wal
	}

	// only add files and delete files should be empty in recover
	edit.addFiles = addFiles
	edit.deleteFiles = nil

	return m.Apply(edit)
}

// return the active wal file
func (m *Manifest) ActiveWal() *Wal {
	return m.active
}

// rotate the active wal
func (m *Manifest) RotateWal() (old *Wal, err error) {
	walPath := WalPath(m.dir, m.nextFid)
	wal, err := NewWal(walPath, m.nextFid, time.Now().Unix())
	if err != nil {
		return nil, err
	}

	edit := &ManifestEdit{
		addFiles:   []LogFile{{wal, wal.Fid()}},
		hasNextFid: true,
		nextFid:    m.nextFid + 1,
	}

	if err = m.LogAndApply(edit); err != nil {
		wal.Unref()
		return
	}

	old = m.active
	m.active.Freeze()
	m.active = wal
	m.wals[wal.Fid()] = &WalInfo{
		wal:            wal,
		freeBytes:      0,
		deltaFreeBytes: 0,
	}

	return
}

// rotate MANIFEST file
func (m *Manifest) RotateManifest() error {
	runners := NewRunner()
	defer runners.Do()

	manifestPath := ManifestPath(m.dir, m.nextFid)
	fp, err := os.Create(manifestPath)
	if err != nil {
		return err
	}

	runners.Post(func() {
		fp.Close()
		os.Remove(manifestPath)
	})

	edit := &ManifestEdit{
		hasNextFid: true,
		nextFid:    m.nextFid + 1,
	}

	for fid := range m.wals {
		edit.addFiles = append(edit.addFiles, LogFile{fid: fid})
	}

	content := edit.Encode()
	if _, err = fp.Write(content); err != nil {
		return err
	}

	newManifest := ManifestFilename(m.nextFid)
	if err = os.WriteFile(CurrentPath(m.dir), []byte(newManifest), 0o644); err != nil {
		return err
	}

	m.fp.Close()
	m.fp = fp
	m.fid = m.nextFid
	m.nextFid++
	m.size = uint64(len(content))

	// abort all functors
	runners.Rollback()

	return nil
}

// return the size of MANIFEST file
func (m *Manifest) FileSize() uint64 {
	return m.size
}

// clean the un-used wal files
// maybe include the previous wal logs after compaction
// clean all files that the MANIFEST doesn't referece
func (m *Manifest) CleanFiles() error {
	files, err := os.ReadDir(m.dir)
	if err != nil {
		return err
	}

	for _, file := range files {
		name := file.Name()
		filetype, fid, err := ParseFilename(name)
		if err != nil {
			continue
		}

		needDelete := false

		switch filetype {
		case LockFileType:
			// skip
		case CurrentFileType:
			// skip
		case WalFileType:
			fallthrough
		case HintFileType:
			// wal and hint file
			if _, exists := m.wals[fid]; !exists {
				needDelete = true
			}
		case TmpFileType:
			fallthrough
		case MergeFileType:
			// tmp and merge file
			needDelete = true
		case ManifestFileType:
			// old manifest should be deleted
			needDelete = (fid != m.fid)
		default:
			// skip unknown file type
		}

		if needDelete {
			os.Remove(filepath.Join(m.dir, name))
		}

		if name == LockFile || name == CurrentFile {
			continue
		}
	}

	return nil
}

func (m *Manifest) ToWal(fid uint64) *Wal {
	if info, exists := m.wals[fid]; exists {
		return info.wal
	}

	return nil
}

func (m *Manifest) prepareApply(edit *ManifestEdit) error {
	wals := make(map[uint64]struct{}, len(m.wals))
	for k := range m.wals {
		wals[k] = struct{}{}
	}

	// validate the add files
	for idx := range edit.addFiles {
		if _, exists := wals[edit.addFiles[idx].fid]; exists {
			return errors.New("add the existed file")
		}
	}

	// validate the delete files
	for idx := range edit.deleteFiles {
		if _, exists := wals[edit.deleteFiles[idx].fid]; !exists {
			return errors.New("unknown delete file")
		}
	}

	return nil
}

// apply one edit, but don't persist
func (m *Manifest) Apply(edit *ManifestEdit) error {
	if err := m.prepareApply(edit); err != nil {
		return err
	}

	// reach here: this edit should apply without any error

	m.apply(edit)

	return nil
}

// apply the manifest without any error
func (m *Manifest) apply(edit *ManifestEdit) {
	// add wals
	for _, add := range edit.addFiles {
		m.wals[add.fid] = &WalInfo{
			wal:            add.wal,
			freeBytes:      0,
			deltaFreeBytes: 0,
		}
	}

	// delete wals
	for _, del := range edit.deleteFiles {
		m.wals[del.fid].wal.Unref()
		delete(m.wals, del.fid)
	}

	// update next file number
	if edit.hasNextFid {
		m.nextFid = max(m.nextFid, edit.nextFid)
	}

	// update delta free bytes of wal
	for fid := range edit.freeBytes {
		if _, exists := m.wals[fid]; !exists {
			continue
		}

		m.wals[fid].deltaFreeBytes += edit.freeBytes[fid]
	}
}

// apply one edit and persist it
func (m *Manifest) LogAndApply(edit *ManifestEdit) error {
	var err error
	if err = m.prepareApply(edit); err != nil {
		return err
	}

	// every time an edit is persisted, all delta free bytes are appended
	runners := NewReverseRunner()
	defer runners.Do()

	for fid := range m.wals {
		// FIXME: when delta free bytes large enough and persist it
		if m.wals[fid].deltaFreeBytes != 0 {
			edit.freeBytes[fid] += m.wals[fid].deltaFreeBytes

			runners.Post(func() {
				m.wals[fid].freeBytes += edit.freeBytes[fid]
				m.wals[fid].deltaFreeBytes = 0
			})
		}
	}

	content := edit.Encode()
	if err = m.appendManifest(content); err != nil {
		return err
	}

	if err = m.fp.Sync(); err != nil {
		return err
	}

	// reach here: this edit can apply without any error

	edit.freeBytes = nil
	m.apply(edit)

	// commit the runners
	runners.Commit()

	return nil
}

func (m *Manifest) appendManifest(data []byte) error {
	var err error
	var nbytes int
	totalWrite := 0
	expectWrite := len(data)

	for totalWrite < expectWrite {
		nbytes, err = m.fp.Write(data[totalWrite:])
		totalWrite += nbytes

		if err != nil {
			break
		}
	}

	m.size += uint64(expectWrite)
	return err
}

// close the related files
func (m *Manifest) Close() error {
	for _, info := range m.wals {
		if info != nil && info.wal != nil {
			info.wal.Close()
		}
	}

	return m.fp.Close()
}
