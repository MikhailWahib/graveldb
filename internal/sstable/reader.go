// Package sstable implements sorted string tables for persistent storage
// of key-value pairs on disk in a format optimized for reads.
package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/MikhailWahib/graveldb/internal/diskmanager"
	"github.com/MikhailWahib/graveldb/internal/shared"
)

type sstReader struct {
	dm        diskmanager.DiskManager
	file      diskmanager.FileHandle
	index     []IndexEntry
	indexBase int64
}

func newSSTReader(dm diskmanager.DiskManager) *sstReader {
	return &sstReader{dm: dm}
}

// Open opens an existing SSTable file for reading
func (r *sstReader) Open(filename string) error {
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

// Close closes the SSTable file
func (r *sstReader) Close() error {
	return r.file.Close()
}

// Lookup performs a binary search over the sparse index and returns the entry if found
func (r *sstReader) Lookup(key []byte) ([]byte, error) {
	// Find index entry with key <= target
	pos := sort.Search(len(r.index), func(i int) bool {
		return shared.CompareKeys(r.index[i].Key, key) > 0
	}) - 1

	if pos < 0 {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	// Calculate the block boundary
	blockEnd := r.indexBase
	if pos+1 < len(r.index) {
		blockEnd = r.index[pos+1].Offset
	}

	// Linear scan within the block boundaries
	offset := r.index[pos].Offset
	var lastValue []byte
	var foundDeleted bool

	for offset < blockEnd {
		entry, err := shared.ReadEntry(r.file, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read entry: %w", err)
		}

		cmp := shared.CompareKeys(entry.Key, key)
		if cmp == 0 {
			if shared.EntryType(entry.Type) == shared.DeleteEntry {
				foundDeleted = true
				lastValue = nil
			} else {
				lastValue = entry.Value
				foundDeleted = false
			}
		}

		if cmp > 0 {
			break
		}

		offset = entry.NewOffset
	}

	if foundDeleted || lastValue == nil {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	return lastValue, nil
}

// Iterator provides sequential access to entries in an SSTable
type Iterator struct {
	reader  *sstReader
	offset  int64
	entry   *shared.StoredEntry
	dataEnd int64
	err     error
}

// newIterator creates a new iterator that uses the index to iterate over data entries
func (r *sstReader) newIterator() *Iterator {
	return &Iterator{
		reader:  r,
		offset:  0,
		dataEnd: r.indexBase,
	}
}

// Next advances the iterator to the next entry using the index
func (it *Iterator) Next() bool {
	if it.err != nil || it.offset >= it.dataEnd {
		return false
	}

	entry, err := shared.ReadEntry(it.reader.file, it.offset)
	if err != nil {
		if err == io.EOF {
			it.entry = nil
			return false
		}
		it.err = err
		return false
	}

	it.entry = &entry
	it.offset = entry.NewOffset
	return true
}

// Key returns the current entry's key
func (it *Iterator) Key() []byte {
	if it.entry == nil {
		return nil
	}
	return it.entry.Key
}

// Value returns the current entry's value
func (it *Iterator) Value() []byte {
	if it.entry == nil {
		return nil
	}
	// For delete entries, return nil value
	if it.entry.Type == shared.DeleteEntry {
		return nil
	}
	return it.entry.Value
}

// Type returns the current entry's type
func (it *Iterator) Type() shared.EntryType {
	if it.entry == nil {
		return 0
	}
	return it.entry.Type
}

// Reset resets the iterator
func (it *Iterator) Reset() {
	it.offset = 0
	it.entry = nil
	it.err = nil
}

// IsDeleted return true if the current value is deleted
func (it *Iterator) IsDeleted() bool {
	return it.entry.Type == shared.DeleteEntry
}

// Error returns any error encountered during iteration
func (it *Iterator) Error() error {
	return it.err
}
