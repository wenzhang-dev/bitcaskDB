package bitcask

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setupWal(name string, t *testing.T) *Wal {
	wal, err := NewWal(name, 0, -1)
	assert.Nil(t, err)
	return wal
}

// Test basic WAL operations: writing and reading records
func TestWal_BasicOperations(t *testing.T) {
	wal := setupWal("test_wal_basic.wal", t)
	defer wal.Unref()

	data := []byte("hello world")
	offset, err := wal.WriteRecord(data)
	if err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}
	assert.Nil(t, wal.Flush())

	readData, err := wal.ReadRecord(offset, uint64(len(data)))
	if err != nil {
		t.Fatalf("Failed to read record: %v", err)
	}

	if string(readData) != string(data) {
		t.Fatalf("Data mismatch: expected %s, got %s", string(data), string(readData))
	}
}

// Test WAL behavior with multiple writes and reads
func TestWal_MultipleRecords(t *testing.T) {
	wal := setupWal("test_wal_multiple.wal", t)
	defer wal.Unref()

	records := [][]byte{
		[]byte("first record"),
		[]byte("second record"),
		[]byte("third record"),
	}

	var offsets []uint64
	for _, record := range records {
		offset, err := wal.WriteRecord(record)
		if err != nil {
			t.Fatalf("Failed to write record: %v", err)
		}
		offsets = append(offsets, offset)
	}
	assert.Nil(t, wal.Flush())

	for i, offset := range offsets {
		readData, err := wal.ReadRecord(offset, uint64(len(records[i])))
		if err != nil {
			t.Fatalf("Failed to read record at offset %d: %v", offset, err)
		}
		assert.Equal(t, readData, records[i])
	}
}

// Test WAL record spanning multiple blocks
func TestWal_LargeRecord(t *testing.T) {
	wal := setupWal("test_wal_large.wal", t)
	defer wal.Unref()

	largeData := make([]byte, BlockSize*2) // A record spanning multiple blocks
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	offset, err := wal.WriteRecord(largeData)
	if err != nil {
		t.Fatalf("Failed to write large record: %v", err)
	}
	assert.Nil(t, wal.Flush())

	readData, err := wal.ReadRecord(offset, uint64(len(largeData)))
	if err != nil {
		t.Fatalf("Failed to read large record: %v", err)
	}

	assert.Equal(t, readData, largeData)
}

func TestWal_LargeRecord2(t *testing.T) {
	wal := setupWal("test_wal_large2.wal", t)
	defer wal.Unref()

	data := GenNKBytes(5)
	offsets := make([]uint64, 1000)
	for i := 0; i < 1000; i++ {
		off, err := wal.WriteRecord(data)
		assert.Nil(t, err)
		assert.Nil(t, wal.Flush())
		offsets[i] = off
	}

	// check
	for i := 0; i < 1000; i++ {
		readData, err := wal.ReadRecord(offsets[i], uint64(len(data)))
		assert.Nil(t, err)
		assert.Equal(t, readData, data)
	}
}

// Test handling of corrupted WAL records
func TestWal_CorruptedRead(t *testing.T) {
	filename := "test_wal_corrupt.wal"
	wal := setupWal(filename, t)
	defer os.Remove(wal.Path())

	data := []byte("valid record")
	offset, err := wal.WriteRecord(data)
	if err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}
	assert.Nil(t, wal.Flush())

	// Close WAL before corrupting the file
	wal.Close()

	// Manually corrupt the file
	file, err := os.OpenFile(filename, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("Failed to open WAL file for corruption: %v", err)
	}

	_, err = file.WriteAt([]byte{0xFF, 0xFF}, int64(offset+2)) // Corrupt part of the record
	if err != nil {
		t.Fatalf("Failed to corrupt WAL file: %v", err)
	}

	// Reopen WAL and try to read
	wal, err = LoadWal(filename, 0)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal.Close()

	_, err = wal.ReadRecord(offset, uint64(len(data)))
	if err == nil {
		t.Fatalf("Expected error when reading corrupted record, but got none")
	}
}

// Test padding when block space is insufficient
func TestWal_BlockPadding(t *testing.T) {
	wal := setupWal("test_wal_padding.wal", t)
	defer wal.Unref()

	// Write a record that nearly fills a block
	data := make([]byte, BlockSize-RecordHeaderSize)
	offset, err := wal.WriteRecord(data)
	if err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}
	assert.Nil(t, wal.Flush())

	// Write another record that should go into the next block due to padding
	secondData := []byte("new block record")
	secondOffset, err := wal.WriteRecord(secondData)
	if err != nil {
		t.Fatalf("Failed to write second record: %v", err)
	}
	assert.Nil(t, wal.Flush())

	// Ensure both records can be read correctly
	readData, err := wal.ReadRecord(offset, uint64(len(data)))
	if err != nil {
		t.Fatalf("Failed to read first record: %v", err)
	}
	assert.Equal(t, readData, data)

	readData, err = wal.ReadRecord(secondOffset, uint64(len(secondData)))
	if err != nil {
		t.Fatalf("Failed to read second record: %v", err)
	}
	assert.Equal(t, readData, secondData)
}

// Test reopening WAL and ensuring persistence
func TestWal_ReopenPersistence(t *testing.T) {
	filename := "test_wal_persistence.wal"
	wal := setupWal(filename, t)
	defer os.Remove(filename)

	data := []byte("persistent data")
	offset, err := wal.WriteRecord(data)
	if err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}
	assert.Nil(t, wal.Flush())

	// Close and reopen WAL
	wal.Close()
	wal, err = LoadWal(filename, 0)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}

	defer wal.Close()

	readData, err := wal.ReadRecord(offset, uint64(len(data)))
	if err != nil {
		t.Fatalf("Failed to read record after reopening: %v", err)
	}

	assert.Equal(t, readData, data)
}
