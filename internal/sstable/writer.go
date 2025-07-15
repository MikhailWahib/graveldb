package sstable

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/MikhailWahib/graveldb/internal/shared"
)

const (
	// indexInterval controls how many entries to skip before adding to the sparse index
	indexInterval = 16
)

// sstWriter provides functionality to write to an SSTable
type sstWriter struct {
	file      *os.File
	index     []IndexEntry
	offset    int64
	indexSize int64
	count     int // tracks number of entries for sparse indexing
}

// newSSTWriter creates a new SSTable writer
func newSSTWriter() *sstWriter {
	return &sstWriter{
		index:  make([]IndexEntry, 0),
		offset: 0,
	}
}

// Open prepares the writer for a new SSTable file
func (w *sstWriter) Open(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	w.file = file
	return nil
}

// AppendPut writes a key-value pair to the SSTable
func (w *sstWriter) AppendPut(key, value []byte) error {
	return w.writeEntry(shared.Entry{
		Type:  shared.PutEntry,
		Key:   key,
		Value: value,
	})
}

// AppendDelete writes a deletion marker for a key to the SSTable
func (w *sstWriter) AppendDelete(key []byte) error {
	return w.writeEntry(shared.Entry{
		Type:  shared.DeleteEntry,
		Key:   key,
		Value: nil,
	})
}

// writeEntry writes a key-value pair to the data section
func (w *sstWriter) writeEntry(entry shared.Entry) error {
	entryOffset := w.offset

	// Write the entry prefixed with type byte and k,v
	n, err := shared.WriteEntry(entry, w.file, w.offset)
	if err != nil {
		return err
	}

	w.offset = n

	if w.count%indexInterval == 0 {
		w.index = append(w.index, IndexEntry{Key: entry.Key, Offset: entryOffset})
	}

	w.count++
	return nil
}

// writeIndex writes the sparse index for faster lookups
func (w *sstWriter) writeIndex() error {
	indexStartOffset := w.offset

	for _, entry := range w.index {
		// Write key with prefix using IndexEntry type
		e := shared.Entry{
			Type:  shared.IndexEntry,
			Key:   entry.Key,
			Value: nil,
		}
		newOffset, err := shared.WriteEntry(e, w.file, w.offset)
		if err != nil {
			return err
		}

		w.offset = newOffset

		// Write offset
		offsetBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(offsetBytes, uint64(entry.Offset))
		_, err = w.file.WriteAt(offsetBytes, w.offset)
		if err != nil {
			return err
		}

		w.offset += 8
	}

	// Calculate actual index size
	w.indexSize = w.offset - indexStartOffset
	return nil
}

// Finish finalizes the SSTable and closes the file
func (w *sstWriter) Finish() error {
	// Write the index section to the file
	indexOffset := w.offset // The current offset will be the start of the index section
	if err := w.writeIndex(); err != nil {
		return fmt.Errorf("failed to write index: %w", err)
	}

	// Write the footer section at the end of the file
	// The footer contains:
	// - The offset of the index section
	// - The size of the index section
	footer := make([]byte, FooterSize)
	binary.BigEndian.PutUint64(footer[:IndexOffsetSize], uint64(indexOffset))
	binary.BigEndian.PutUint64(footer[IndexOffsetSize:], uint64(w.indexSize))

	// Write the footer to the file
	_, err := w.file.WriteAt(footer, w.offset)
	if err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}

	// Update the offset for the footer
	w.offset += FooterSize

	// Sync the file to make sure everything is written to disk
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	// Close the file
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	return nil
}
