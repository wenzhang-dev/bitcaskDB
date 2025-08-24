package block_reader

import (
	"errors"
	"sync"

	"github.com/iceber/iouring-go"
)

var ErrBlockReaderConcurrent = errors.New("concurrent too large")

type IOUringBlockReader struct {
	reqPool    sync.Pool
	iour       *iouring.IOURing
	concurrent uint64
}

func NewIOUringBlockReader(concurrent uint64) (*IOUringBlockReader, error) {
	iour, err := iouring.New(uint(concurrent))
	if err != nil {
		return nil, err
	}

	return &IOUringBlockReader{
		reqPool: sync.Pool{
			New: func() any {
				b := make([]iouring.PrepRequest, concurrent)
				return &b
			},
		},
		iour:       iour,
		concurrent: concurrent,
	}, nil
}

func (r *IOUringBlockReader) NewRequest(fd int, fid, offset uint64, blk []byte) *Request {
	return &Request{
		Fd:  fd,
		Fid: fid,
		Off: offset,
		Blk: blk,
		res: 0,
		err: nil,
	}
}

func (r *IOUringBlockReader) Submit(reqs Requests) error {
	if len(reqs) > int(r.concurrent) {
		return ErrBlockReaderConcurrent
	}

	prepReqsPtr := r.reqPool.Get().(*[]iouring.PrepRequest)
	defer r.reqPool.Put(prepReqsPtr)

	prepReqs := *prepReqsPtr

	for idx := range reqs {
		prepReqs[idx] = iouring.Pread(reqs[idx].Fd, reqs[idx].Blk, reqs[idx].Off)
	}

	rset, err := r.iour.SubmitRequests(prepReqs[:len(reqs)], nil)
	if err != nil {
		return err
	}

	// wait for completion
	<-rset.Done()

	// the order of io_uring Request is the same to `reqs` arguments
	for idx, req := range rset.Requests() {
		res, _ := req.GetRes()
		reqs[idx].res = res
	}

	return nil
}
