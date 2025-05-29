package sstable

import (
	"encoding/binary"
	"fmt"
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
	pos := sort.Search(len(r.index), func(i int) bool {
		return shared.CompareKeys(r.index[i].Key, key) >= 0
	})

	if pos == len(r.index) {
		// Key is beyond last indexed key — not found
		return nil, fmt.Errorf("key not found: %s", key)
	}

	// Linear scan from found position
	offset := r.index[pos].Offset
	for {
		entry, err := shared.ReadEntry(r.file, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to read entry: %w", err)
		}

		cmp := shared.CompareKeys(entry.Key, key)
		if cmp == 0 {
			// Key found, with a tombstone check
			if shared.EntryType(entry.Type) == shared.DeleteEntry {
				return nil, fmt.Errorf("key marked as deleted: %s", key)
			}
			// Key found, return the value
			return entry.Value, nil
		}

		if cmp > 0 {
			break
		}

		offset = entry.NewOffset
	}

	return nil, fmt.Errorf("key not found: %s", key)
}

// SSTableIterator provides sequential access to entries in an SSTable
type sstIterator struct {
	reader *sstReader
	pos    int
	entry  *shared.StoredEntry
	err    error
}

// NewIterator creates a new iterator that uses the index to iterate over data entries
func (r *sstReader) newIterator() *sstIterator {
	return &sstIterator{
		reader: r,
		pos:    0,
	}
}

// Next advances the iterator to the next entry using the index
func (it *sstIterator) Next() bool {
	if it.err != nil {
		return false
	}
	if it.pos >= len(it.reader.index) {
		return false
	}
	offset := it.reader.index[it.pos].Offset
	entry, err := shared.ReadEntry(it.reader.file, offset)
	if err != nil {
		it.err = err
		return false
	}
	it.entry = &entry
	it.pos++
	return true
}

// Key returns the current entry's key
func (it *sstIterator) Key() []byte {
	if it.entry == nil {
		return nil
	}
	return it.entry.Key
}

// Value returns the current entry's value
func (it *sstIterator) Value() []byte {
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
func (it *sstIterator) Type() shared.EntryType {
	if it.entry == nil {
		return 0
	}
	return it.entry.Type
}

// Error returns any error encountered during iteration
func (it *sstIterator) Error() error {
	return it.err
}
