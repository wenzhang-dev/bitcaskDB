package bitcask

import (
	"errors"
	"sync"
)

var (
	ErrAbortedManifestTxn   = errors.New("aborted manifest txn")
	ErrCommittedManfiestTxn = errors.New("committed manifest txn")
)

// this is a rough implementation of manifest transaction. its main purpose is to make the applied
// manifest edit visible to other operations. the pending edit of transaction will be persisted only
// after the transaction is committed.
//
// the design is rough because the deleted wals in the applied manifest edits may also be visible to
// others. also, only one running manifest transaction is supported currently.
//
// nevertheless, this design also works well, and avoid transitional design
type ManifestTxn struct {
	manifest    *Manifest
	pendingEdit *ManifestEdit

	committed bool
	aborted   bool

	mu sync.RWMutex
}

func NewManifestTxn(manifest *Manifest) *ManifestTxn {
	return &ManifestTxn{
		aborted:     false,
		committed:   false,
		manifest:    manifest,
		pendingEdit: NewManifestEdit(),
	}
}

func (txn *ManifestTxn) IsDone() bool {
	txn.mu.RLock()
	defer txn.mu.RUnlock()

	return txn.aborted || txn.committed
}

func (txn *ManifestTxn) toWalLocked(fid uint64) *Wal {
	for idx := range txn.pendingEdit.addFiles {
		if txn.pendingEdit.addFiles[idx].fid == fid {
			return txn.pendingEdit.addFiles[idx].wal
		}
	}

	return nil
}

func (txn *ManifestTxn) ToWal(fid uint64) *Wal {
	txn.mu.RLock()
	defer txn.mu.RUnlock()

	return txn.toWalLocked(fid)
}

func (txn *ManifestTxn) ToWalWithRef(fid uint64) *Wal {
	txn.mu.RLock()
	defer txn.mu.RUnlock()

	if wal := txn.toWalLocked(fid); wal != nil {
		wal.Ref()
		return wal
	}

	return nil
}

func (txn *ManifestTxn) NextFid() uint64 {
	txn.mu.RLock()
	defer txn.mu.RUnlock()

	return txn.pendingEdit.nextFid
}

func (txn *ManifestTxn) Apply(edit *ManifestEdit) {
	if edit == nil {
		return
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	txn.pendingEdit.Merge(edit)
}

func (txn *ManifestTxn) Commit(edit *ManifestEdit) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.committed {
		return ErrCommittedManfiestTxn
	}

	if txn.aborted {
		return ErrAbortedManifestTxn
	}

	if edit != nil {
		txn.pendingEdit.Merge(edit)
	}

	if err := txn.manifest.LogAndApply(txn.pendingEdit); err != nil {
		txn.aborted = true
		return err
	}

	txn.committed = true
	return nil
}

func (txn *ManifestTxn) Abort() {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.committed {
		return
	}

	txn.aborted = true
}
