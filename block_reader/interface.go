package block_reader

import (
	"errors"
	"golang.org/x/sys/unix"
	"slices"
)

var ErrBlockReaderRequestFailed = errors.New("block reader request failed")

type Request struct {
	Fd  int
	Fid uint64
	Off uint64
	Blk []byte

	res int
	err error
}

func (r *Request) Err() error {
	if r.err != nil {
		return r.err
	}

	if r.res < 0 {
		return errors.Join(ErrBlockReaderRequestFailed, unix.Errno(-r.res))
	}
	return nil
}

func (r *Request) NBytes() int {
	return r.res
}

type Requests []*Request

func (r Requests) Sort() {
	slices.SortFunc(r, func(a, b *Request) int {
		if a.Fd < b.Fd {
			return -1
		} else if a.Fd > b.Fd {
			return 1
		}

		if a.Off < b.Off {
			return -1
		} else if a.Off > b.Off {
			return 1
		}

		return 0
	})
}

func (r Requests) BinarySearch(fid, off uint64) (*Request, bool) {
	idx, found := slices.BinarySearchFunc(r, &Request{Fid: fid, Off: off}, func(a, b *Request) int {
		if a.Fid < b.Fid {
			return -1
		} else if a.Fid > b.Fid {
			return 1
		}

		if a.Off < b.Off {
			return -1
		} else if a.Off > b.Off {
			return 1
		}

		return 0
	})

	if !found {
		return nil, false
	}

	return r[idx], true
}

// thread safe
type BlockReader interface {
	NewRequest(fd int, fid, offset uint64, blk []byte) *Request
	Submit(reqs Requests) error
}
