package bitcask

type Batch struct {
	records []*Record

	// the total number of bytes of serialized records
	byteSize int
}

func NewBatch() *Batch {
	return &Batch{}
}

func (b *Batch) Put(ns, key []byte, meta *Meta, val []byte) {
	record := &Record{
		Ns:      ns,
		Key:     key,
		Meta:    meta,
		Value:   val,
		Deleted: false,
	}
	b.records = append(b.records, record)
	b.byteSize += record.ApproximateSize()
}

func (b *Batch) Delete(ns, key []byte) {
	record := &Record{
		Ns:  ns,
		Key: key,
		// the deletion operation will carry tombstone flag, and store in database
		// at the same time, the related index in memory will be removed. so the key will
		// not be found. the record with tombstone flag will be removed in compaction
		Meta:    NewMetaWithTombstone(),
		Value:   nil,
		Deleted: true,
	}
	b.records = append(b.records, record)
	b.byteSize += record.ApproximateSize()
}

func (b *Batch) Clear() {
	b.byteSize = 0
	b.records = nil
}

func (b *Batch) Append(batch *Batch) {
	if batch == nil {
		return
	}

	b.records = append(b.records, batch.records...)
	b.byteSize += batch.byteSize
}

func (b *Batch) Size() int {
	return len(b.records)
}

func (b *Batch) ByteSize() int {
	return b.byteSize
}
