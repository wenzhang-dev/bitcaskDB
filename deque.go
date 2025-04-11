package bitcask

import "errors"

const (
	DequeChunkSize        = 512
	DequeInitSize         = 64
	DequeFrontReserveSize = 3
)

var (
	ErrDequeEmpty      = errors.New("deque empty")
	ErrDequeOutOfRange = errors.New("deque out of range")
)

type dequeChunk[T any] struct {
	buf [DequeChunkSize]T

	// the [start, end) is actual data range
	start int32
	end   int32
}

func (c *dequeChunk[T]) size() int {
	return int(c.end - c.start)
}

func (c *dequeChunk[T]) rightFull() bool {
	return c.end == DequeChunkSize
}

func (c *dequeChunk[T]) leftFull() bool {
	return c.start == 0
}

func (c *dequeChunk[T]) empty() bool {
	return c.start == c.end
}

type Deque[T any] struct {
	// buffer
	chunks []*dequeChunk[T]

	// the [start, end) is actual data range
	start int64
	end   int64 // end-1 index always points valid chunk

	// the total size of elements
	size int64
}

func NewDeque[T any]() *Deque[T] {
	// the init start end end range: [2, 4)
	d := &Deque[T]{
		chunks: make([]*dequeChunk[T], DequeInitSize),
		start:  DequeFrontReserveSize - 1,
		end:    DequeFrontReserveSize + 1,
		size:   0,
	}

	// the start index(2) is writable
	d.chunks[d.start] = &dequeChunk[T]{
		start: DequeChunkSize,
		end:   DequeChunkSize,
	}

	// the end-1 index(3) is writable
	d.chunks[d.start+1] = &dequeChunk[T]{
		start: 0,
		end:   0,
	}

	return d
}

func (d *Deque[T]) Back() (*T, error) {
	if d.Empty() {
		return nil, ErrDequeEmpty
	}

	return d.At(d.Len() - 1)
}

func (d *Deque[T]) Front() (*T, error) {
	if d.Empty() {
		return nil, ErrDequeEmpty
	}

	return d.At(0)
}

func (d *Deque[T]) grow() {
	if int(d.end) == len(d.chunks) || d.start == 0 {
		// copy pointers
		chunks := make([]*dequeChunk[T], 2*d.end)
		for i := d.start; i < d.end; i++ {
			chunks[DequeFrontReserveSize+(i-d.start)] = d.chunks[i]
		}

		// order matters
		d.end = DequeFrontReserveSize + (d.end - d.start)
		d.start = DequeFrontReserveSize
		d.chunks = chunks
	}
}

func (d *Deque[T]) PushBack(v T) {
	// the end-1 chunk always writable
	chunk := d.chunks[d.end-1]

	chunk.buf[chunk.end] = v
	chunk.end++

	if chunk.rightFull() {
		d.grow()

		d.chunks[d.end] = &dequeChunk[T]{
			start: 0,
			end:   0,
		}
		d.end++
	}

	d.size++
}

func (d *Deque[T]) PushFront(v T) {
	// the start chunk always writable
	chunk := d.chunks[d.start]

	chunk.buf[chunk.start-1] = v
	chunk.start--

	if chunk.leftFull() {
		d.grow()

		d.chunks[d.start-1] = &dequeChunk[T]{
			start: DequeChunkSize,
			end:   DequeChunkSize,
		}
		d.start--
	}

	d.size++
}

func (d *Deque[T]) PopBack() error {
	if d.Empty() {
		return ErrDequeEmpty
	}

	chunk := d.chunks[d.end-1]
	if chunk.empty() {
		d.chunks[d.end-1] = nil
		d.end--
		chunk = d.chunks[d.end-1]
	}

	// make sure the chunk always writable
	chunk.end--

	d.size--
	if d.size == 0 {
		d.Clear()
	}

	return nil
}

func (d *Deque[T]) PopFront() error {
	if d.Empty() {
		return ErrDequeEmpty
	}

	chunk := d.chunks[d.start]
	if chunk.empty() {
		d.chunks[d.start] = nil
		d.start++
		chunk = d.chunks[d.start]
	}

	// make sure the chunk always writable
	chunk.start++

	d.size--
	if d.size == 0 {
		d.Clear()
	}

	return nil
}

func (d *Deque[T]) Len() int {
	return int(d.size)
}

func (d *Deque[T]) Empty() bool {
	return d.size == 0
}

func (d *Deque[T]) At(idx int) (*T, error) {
	if idx >= d.Len() {
		return nil, ErrDequeOutOfRange
	}

	if idx < d.chunks[d.start].size() {
		chunk := d.chunks[d.start]
		return &chunk.buf[int(chunk.start)+idx], nil
	}

	idx -= d.chunks[d.start].size()
	pos := int(d.start+1) + idx/DequeChunkSize
	off := idx % DequeChunkSize
	chunk := d.chunks[pos]
	return &chunk.buf[int(chunk.start)+off], nil
}

func (d *Deque[T]) Clear() {
	for i := d.start; i < d.end; i++ {
		d.chunks[i] = nil
	}

	d.start = DequeFrontReserveSize - 1
	d.end = DequeFrontReserveSize + 1

	// the start index(2) is writable
	d.chunks[d.start] = &dequeChunk[T]{
		start: DequeChunkSize,
		end:   DequeChunkSize,
	}

	// the end-1 index(3) is writable
	d.chunks[d.start+1] = &dequeChunk[T]{
		start: 0,
		end:   0,
	}
}
