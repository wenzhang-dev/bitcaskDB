//go:build io_uring
// +build io_uring

package block_reader

func NewDefaultBlockReader(concurrent uint64) (BlockReader, error) {
	return NewIOUringBlockReader(concurrent)
}
