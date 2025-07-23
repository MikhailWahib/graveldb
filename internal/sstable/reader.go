// Package sstable implements sorted string tables for persistent storage
// of key-value pairs on disk in a format optimized for reads.
package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/MikhailWahib/graveldb/internal/storage"
)

// Reader provides functionality to read from an SSTable
type Reader struct {
	file      *os.File
	path      string
	index     []IndexEntry
	indexBase int64
}

// NewReader creates a new SSTable reader
func NewReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open SSTable: %w", err)
	}

	reader := &Reader{
		file: file,
		path: path,
	}

	if err := reader.loadIndex(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to load index: %w", err)
	}

	return reader, nil
}

// loadIndex reads the footer and load the index to memory
func (r *Reader) loadIndex() error {
	stat, err := r.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat SST file: %w", err)
	}
	if stat.Size() < FooterSize {
		return nil
	}

	// Read footer
	footerOffset := stat.Size() - FooterSize
	footer := make([]byte, FooterSize)
	if _, err := io.ReadFull(io.NewSectionReader(r.file, footerOffset, FooterSize), footer); err != nil {
		return fmt.Errorf("failed to read footer: %w", err)
	}

	indexOffset := int64(binary.BigEndian.Uint64(footer[:IndexOffsetSize]))
	indexSize := int64(binary.BigEndian.Uint64(footer[IndexOffsetSize:]))
	r.indexBase = indexOffset

	// Read index section into memory buffer (single read)
	indexBuf := make([]byte, indexSize)
	if _, err := io.ReadFull(io.NewSectionReader(r.file, indexOffset, indexSize), indexBuf); err != nil {
		return fmt.Errorf("failed to read index section: %w", err)
	}

	// Parse index from buffer
	r.index = make([]IndexEntry, 0, indexSize/40) // rough guess; 40 bytes per entry
	var offset int64
	for offset < indexSize {
		entry, bytesRead, err := storage.DecodeEntry(indexBuf[offset:])
		if err != nil {
			return fmt.Errorf("failed to decode index entry: %w", err)
		}

		if offset+int64(bytesRead)+8 > indexSize {
			return fmt.Errorf("corrupt index: missing data offset")
		}

		// Decode data offset
		dataOffset := int64(binary.BigEndian.Uint64(indexBuf[offset+int64(bytesRead) : offset+int64(bytesRead)+8]))
		r.index = append(r.index, IndexEntry{
			Key:    entry.Key,
			Offset: dataOffset,
		})
		offset += int64(bytesRead) + 8
	}

	return nil
}

// Get performs a lookup and returns the entry if found
func (r *Reader) Get(key []byte) (storage.Entry, error) {
	// Find index entry with key <= target
	pos := sort.Search(len(r.index), func(i int) bool {
		return bytes.Compare(r.index[i].Key, key) > 0
	}) - 1

	if pos < 0 {
		return storage.Entry{}, fmt.Errorf("key not found: %s", key)
	}

	// Calculate the block boundary
	blockEnd := r.indexBase
	if pos+1 < len(r.index) {
		blockEnd = r.index[pos+1].Offset
	}

	// Linear scan within the block boundaries
	offset := r.index[pos].Offset
	var lastValue storage.Entry
	var foundDeleted bool

	for offset < blockEnd {
		entry, newOffset, err := storage.ReadEntryAt(r.file, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return storage.Entry{}, fmt.Errorf("failed to read entry: %w", err)
		}

		cmp := bytes.Compare(entry.Key, key)
		if cmp == 0 {
			if storage.EntryType(entry.Type) == storage.DeleteEntry {
				foundDeleted = true
				lastValue = storage.Entry{}
			} else {
				lastValue = entry
				foundDeleted = false
			}
		}

		if cmp > 0 {
			break
		}

		offset = newOffset
	}

	if foundDeleted || lastValue.Value == nil {
		return storage.Entry{}, fmt.Errorf("key not found: %s", key)
	}
	return lastValue, nil
}

// NewIterator creates a new iterator
func (r *Reader) NewIterator() *Iterator {
	return &Iterator{
		reader:  r,
		offset:  0,
		dataEnd: r.indexBase,
	}
}

// Close closes the underlying file
func (r *Reader) Close() error {
	return r.file.Close()
}

// Path returns the SSTable file path
func (r *Reader) Path() string {
	return r.path
}

// Iterator provides sequential access to entries in an SSTable
type Iterator struct {
	reader  *Reader
	offset  int64
	entry   *storage.Entry
	dataEnd int64
	err     error
}

// Next advances the iterator to the next entry
func (it *Iterator) Next() bool {
	if it.err != nil || it.offset >= it.dataEnd {
		return false
	}

	entry, newOffset, err := storage.ReadEntryAt(it.reader.file, it.offset)
	if err != nil {
		if err == io.EOF {
			it.entry = nil
			return false
		}
		it.err = err
		return false
	}

	it.entry = &entry
	it.offset = newOffset
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
	if it.entry.Type == storage.DeleteEntry {
		return nil
	}
	return it.entry.Value
}

// Type returns the current entry's type
func (it *Iterator) Type() storage.EntryType {
	if it.entry == nil {
		return 0
	}
	return it.entry.Type
}

// IsDeleted return true if the current value is deleted
func (it *Iterator) IsDeleted() bool {
	return it.entry != nil && it.entry.Type == storage.DeleteEntry
}

// Reset resets the iterator
func (it *Iterator) Reset() {
	it.offset = 0
	it.entry = nil
	it.err = nil
}

// Error returns any error encountered during iteration
func (it *Iterator) Error() error {
	return it.err
}
