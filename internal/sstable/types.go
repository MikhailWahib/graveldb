package sstable

import "github.com/MikhailWahib/graveldb/internal/shared"

const (
	IndexOffsetSize = 8
	IndexSizeSize   = 8
	FooterSize      = IndexOffsetSize + IndexSizeSize
)

// Entry represents a key-value entry in the SSTable
type Entry struct {
	Type  shared.EntryType
	Key   []byte
	Value []byte
}

// IndexEntry represents an entry in the sparse index
type IndexEntry struct {
	Key    []byte
	Offset int64
}
