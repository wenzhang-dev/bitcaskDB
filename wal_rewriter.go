package bitcask

type WalRewriter struct {
	wal *Wal

	bufLen    int
	threshold int
}

func NewWalRewriter(wal *Wal, threshold int) *WalRewriter {
	if threshold < 4*1024 {
		threshold = 4 * 1024
	}

	wal.Ref()
	return &WalRewriter{
		wal:       wal,
		bufLen:    0,
		threshold: threshold,
	}
}

func (r *WalRewriter) Wal() *Wal {
	return r.wal
}

func (r *WalRewriter) Close() error {
	if r.bufLen != 0 {
		if err := r.wal.Flush(); err != nil {
			return err
		}
	}
	r.wal.Unref()
	return nil
}

func (r *WalRewriter) AppendRecord(record []byte) (off uint64, err error) {
	off, err = r.wal.WriteRecord(record)
	if err != nil {
		return 0, err
	}

	r.bufLen += len(record)
	if r.bufLen >= r.threshold {
		err = r.Flush()
	}

	return
}

func (r *WalRewriter) Flush() error {
	if r.bufLen != 0 {
		if err := r.wal.Flush(); err != nil {
			return err
		}
		r.bufLen = 0
	}
	return nil
}
