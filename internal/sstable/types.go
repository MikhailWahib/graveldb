package sstable

// File format constants for SSTable
const (
	// IndexOffsetSize is the size in bytes of the index offset field
	IndexOffsetSize = 8
	// IndexSizeSize is the size in bytes of the index size field
	IndexSizeSize = 8
	// FooterSize is the total size of the SSTable footer
	FooterSize = IndexOffsetSize + IndexSizeSize
)

// IndexEntry represents an entry in the sparse index
type IndexEntry struct {
	Key    []byte
	Offset int64
}
