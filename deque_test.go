package bitcask

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDequeBasicOperations(t *testing.T) {
	d := NewDeque[int]()

	// Initially, deque should be empty
	if !d.Empty() {
		t.Errorf("Deque should be empty initially")
	}

	// Insert elements
	d.PushBack(10)
	d.PushBack(20)
	d.PushFront(5)

	if d.Len() != 3 {
		t.Errorf("Expected length 3, got %d", d.Len())
	}

	// Check Front and Back
	front, _ := d.Front()
	if *front != 5 {
		t.Errorf("Expected front to be 5, got %d", *front)
	}

	back, _ := d.Back()
	if *back != 20 {
		t.Errorf("Expected back to be 20, got %d", *back)
	}

	// Test At()
	val, _ := d.At(1)
	if *val != 10 {
		t.Errorf("Expected At(1) to be 10, got %d", *val)
	}

	// Test PopFront and PopBack
	_ = d.PopFront() // Remove 5
	front, _ = d.Front()
	if *front != 10 {
		t.Errorf("Expected front to be 10, got %d", *front)
	}

	_ = d.PopBack() // Remove 20
	back, _ = d.Back()
	if *back != 10 {
		t.Errorf("Expected back to be 10, got %d", *back)
	}

	_ = d.PopBack() // Remove 10, should become empty
	if !d.Empty() {
		t.Errorf("Deque should be empty after popping all elements")
	}
}

func TestDequeBounds(t *testing.T) {
	d := NewDeque[int]()

	// Calling Front/Back on an empty deque
	if _, err := d.Front(); err != ErrDequeEmpty {
		t.Errorf("Expected DequeEmptyErr for Front() on empty deque")
	}
	if _, err := d.Back(); err != ErrDequeEmpty {
		t.Errorf("Expected DequeEmptyErr for Back() on empty deque")
	}

	// PopFront / PopBack on an empty deque
	if err := d.PopFront(); err != ErrDequeEmpty {
		t.Errorf("Expected DequeEmptyErr for PopFront() on empty deque")
	}
	if err := d.PopBack(); err != ErrDequeEmpty {
		t.Errorf("Expected DequeEmptyErr for PopBack() on empty deque")
	}

	// Accessing out-of-range index
	d.PushBack(1)
	d.PushBack(2)
	if _, err := d.At(3); err != ErrDequeOutOfRange {
		t.Errorf("Expected DequeOutOfRangeErr for At(3)")
	}
}

func TestDequeAutoGrow(t *testing.T) {
	d := NewDeque[int]()

	// PushBack elements until deque expands
	for i := 0; i < 2000; i++ {
		d.PushBack(i)
	}

	if d.Len() != 2000 {
		t.Errorf("Expected length 2000, got %d", d.Len())
	}

	// Ensure first 10 and last 10 elements are correct
	for i := 0; i < 10; i++ {
		val, _ := d.At(i)
		if *val != i {
			t.Errorf("At(%d) expected %d, got %d", i, i, *val)
		}
	}

	for i := 1990; i < 2000; i++ {
		val, _ := d.At(i)
		if *val != i {
			t.Errorf("At(%d) expected %d, got %d", i, i, *val)
		}
	}

	// Reverse PopBack, deque should become empty
	for i := 0; i < 2000; i++ {
		_ = d.PopBack()
	}

	if !d.Empty() {
		t.Errorf("Expected empty deque after popping all elements")
	}
}

func TestDequePushFrontPopBack(t *testing.T) {
	d := NewDeque[int]()

	// Insert into the front
	for i := 0; i < 100; i++ {
		d.PushFront(i)
	}

	if d.Len() != 100 {
		t.Errorf("Expected length 100, got %d", d.Len())
	}

	// Remove elements from back, should be 0,1,2,...99
	for i := 0; i < 100; i++ {
		val, _ := d.Back()
		if *val != i {
			t.Errorf("Expected back %d, got %d", i, *val)
		}
		_ = d.PopBack()
	}

	if !d.Empty() {
		t.Errorf("Expected empty deque after popping all elements")
	}
}

func TestDequeLargeData(t *testing.T) {
	d := NewDeque[int]()
	num := 1_000_000

	// Performance test: Insert 1M elements
	for i := 0; i < num; i++ {
		d.PushBack(i)
	}

	if d.Len() != num {
		t.Errorf("Expected length %d, got %d", num, d.Len())
	}

	// Check first and last 10 elements
	for i := 0; i < 10; i++ {
		val, _ := d.At(i)
		if *val != i {
			t.Errorf("At(%d) expected %d, got %d", i, i, *val)
		}
	}

	for i := num - 10; i < num; i++ {
		val, _ := d.At(i)
		if *val != i {
			t.Errorf("At(%d) expected %d, got %d", i, i, *val)
		}
	}

	// Remove all elements
	for i := 0; i < num; i++ {
		_ = d.PopFront()
	}

	if !d.Empty() {
		t.Errorf("Expected empty deque after popping all elements")
	}
}

func TestDequeCornerCase1(t *testing.T) {
	d := NewDeque[int]()

	for i := 0; i < DequeChunkSize; i++ {
		d.PushBack(i)
	}

	d.PushBack(100)

	for i := 0; i < DequeChunkSize; i++ {
		err := d.PopFront()
		assert.Nil(t, err)
	}

	assert.Equal(t, d.Len(), 1)
	num, err := d.Front()
	assert.Nil(t, err)
	assert.Equal(t, *num, 100)

	err = d.PopFront()
	assert.Nil(t, err)
	assert.True(t, d.Empty())
}

func TestDequeCornerCase2(t *testing.T) {
	d := NewDeque[int]()

	for i := 0; i < DequeChunkSize*(DequeFrontReserveSize+1); i++ {
		d.PushFront(i)
	}

	for i := 0; i < DequeChunkSize; i++ {
		d.PushBack(i)
	}

	assert.Equal(t, d.Len(), int(DequeChunkSize*(DequeFrontReserveSize+2)))

	for i := 0; i < DequeChunkSize; i++ {
		err := d.PopFront()
		assert.Nil(t, err)
	}

	for i := 0; i < DequeChunkSize*(DequeFrontReserveSize+1); i++ {
		err := d.PopBack()
		assert.Nil(t, err)
	}

	assert.True(t, d.Empty())
}

func TestDequeMemoryInGrow(t *testing.T) {
	d := NewDeque[int]()

	d.PushFront(1000)
	addr1, err := d.Front()
	assert.Nil(t, err)

	for i := 0; i < DequeChunkSize*(DequeFrontReserveSize+1); i++ {
		d.PushFront(i)
	}

	addr2, err := d.Back()
	assert.Nil(t, err)
	assert.Equal(t, addr1, addr2)

	for i := 0; i < DequeChunkSize*(DequeFrontReserveSize+1); i++ {
		err = d.PopFront()
		assert.Nil(t, err)
	}

	addr3, err := d.Front()
	assert.Nil(t, err)
	assert.Equal(t, addr2, addr3)
	assert.Equal(t, *addr3, 1000)
}
