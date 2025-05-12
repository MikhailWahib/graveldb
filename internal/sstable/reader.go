package sstable

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	"github.com/MikhailWahib/graveldb/internal/diskmanager"
	"github.com/MikhailWahib/graveldb/internal/shared"
)

const (
	IndexOffsetSize = 8
	IndexSizeSize   = 8
	FooterSize      = IndexOffsetSize + IndexSizeSize
)

type SSTReader struct {
	dm        diskmanager.DiskManager
	file      diskmanager.FileHandle
	index     []IndexEntry
	indexBase int64
}

func NewSSTReader(dm diskmanager.DiskManager) *SSTReader {
	return &SSTReader{dm: dm}
}

func (r *SSTReader) Open(filename string) error {
	file, err := r.dm.Open(filename, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open SST file: %w", err)
	}
	r.file = file

	// Load footer
	stat, err := r.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat SST file: %w", err)
	}

	if stat.Size() < FooterSize {
		return fmt.Errorf("file too small to be valid SSTable")
	}

	footerOffset := stat.Size() - FooterSize
	buf := make([]byte, FooterSize)
	_, err = r.file.ReadAt(buf, footerOffset)
	if err != nil {
		return fmt.Errorf("failed to read footer: %w", err)
	}

	indexOffset := int64(binary.BigEndian.Uint64(buf[:IndexOffsetSize]))
	indexSize := int64(binary.BigEndian.Uint64(buf[IndexOffsetSize:FooterSize]))
	r.indexBase = indexOffset

	// Parse sparse index
	index := []IndexEntry{}
	offset := indexOffset
	end := indexOffset + indexSize

	for offset < end {
		entry, err := shared.ReadEntry(r.file, offset)
		if err != nil {
			return fmt.Errorf("failed to read index entry: %w", err)
		}

		// Read offset immediately after key
		offsetBuf := make([]byte, 8)
		_, err = r.file.ReadAt(offsetBuf, entry.NewOffset)
		if err != nil {
			return fmt.Errorf("failed to read index offset: %w", err)
		}
		dataOffset := int64(binary.BigEndian.Uint64(offsetBuf))
		index = append(index, IndexEntry{Key: entry.Key, Offset: dataOffset})

		offset = entry.NewOffset + 8
	}

	r.index = index
	return nil
}

func (r *SSTReader) Close() error {
	return r.file.Close()
}

// Lookup performs a binary search over the sparse index and returns the entry if found.
func (r *SSTReader) Lookup(key []byte) ([]byte, error) {
	pos := sort.Search(len(r.index), func(i int) bool {
		return shared.CompareKeys(r.index[i].Key, key) >= 0
	})

	if pos == len(r.index) {
		// Key is beyond last indexed key — not found
		return nil, fmt.Errorf("key not found")
	}

	// Linear scan from found position
	offset := r.index[pos].Offset
	for {
		entry, err := shared.ReadEntry(r.file, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to read index entry: %w", err)
		}

		cmp := shared.CompareKeys(entry.Key, key)
		if cmp == 0 {
			// Key found, with a tombstone check, return nil (not found)
			if shared.EntryType(entry.Type) == shared.DeleteEntry {
				return nil, fmt.Errorf("key not found")
			}

			// Key found, return the value
			return entry.Value, nil
		}

		if cmp > 0 {
			break
		}

		offset = entry.NewOffset
	}

	return nil, fmt.Errorf("key not found")
}
