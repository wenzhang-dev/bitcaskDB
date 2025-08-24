package block_reader

import (
	"golang.org/x/sys/unix"
)

type PreadBlockReader struct{}

func NewPreadBlockReader(concurrent uint64) (*PreadBlockReader, error) {
	return &PreadBlockReader{}, nil
}

func (r *PreadBlockReader) NewRequest(fd int, fid, offset uint64, blk []byte) *Request {
	return &Request{
		Fd:  fd,
		Fid: fid,
		Off: offset,
		Blk: blk,
		res: 0,
		err: nil,
	}
}

func (r *PreadBlockReader) Submit(reqs Requests) error {
	for idx, req := range reqs {
		n, err := unix.Pread(req.Fd, req.Blk, int64(req.Off))
		reqs[idx].err = err
		reqs[idx].res = n
	}

	return nil
}
