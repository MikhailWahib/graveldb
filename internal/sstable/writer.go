// Package sstable implements sorted string tables for persistent storage
// of key-value pairs on disk in a format optimized for reads.
package sstable

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/MikhailWahib/graveldb/internal/storage"
)

// Writer provides functionality to write to an SSTable
type Writer struct {
	file          *os.File
	path          string
	index         []IndexEntry
	offset        int64
	indexSize     int64
	count         int // tracks number of entries for sparse indexing
	finished      bool
	indexInterval int
}

// NewWriter creates a new SSTable writer
func NewWriter(path string, indexInterval int) (*Writer, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable: %w", err)
	}

	return &Writer{
		file:          file,
		path:          path,
		index:         make([]IndexEntry, 0),
		indexInterval: indexInterval,
	}, nil
}

// WriteEntry writes a key-value pair to the SSTable
func (w *Writer) WriteEntry(key, value []byte) error {
	if w.finished {
		return fmt.Errorf("cannot write to finished SSTable")
	}
	return w.writeEntry(storage.Entry{
		Type:  storage.SetEntry,
		Key:   key,
		Value: value,
	})
}

// DeleteEntry writes a deletion marker for a key to the SSTable
func (w *Writer) DeleteEntry(key []byte) error {
	if w.finished {
		return fmt.Errorf("cannot write to finished SSTable")
	}
	return w.writeEntry(storage.Entry{
		Type:  storage.DeleteEntry,
		Key:   key,
		Value: nil,
	})
}

// writeEntry writes a key-value pair to the data section
func (w *Writer) writeEntry(entry storage.Entry) error {
	entryOffset := w.offset

	// Write the entry prefixed with type byte and k,v
	n, err := storage.WriteEntryAt(entry, w.file, w.offset)
	if err != nil {
		return err
	}
	w.offset = n

	if w.count%w.indexInterval == 0 {
		w.index = append(w.index, IndexEntry{Key: entry.Key, Offset: entryOffset})
	}
	w.count++
	return nil
}

// writeIndex writes the sparse index for faster lookups
func (w *Writer) writeIndex() error {
	indexStartOffset := w.offset

	for _, entry := range w.index {
		// Write key with prefix using IndexEntry type
		e := storage.Entry{
			Type:  storage.IndexEntry,
			Key:   entry.Key,
			Value: nil,
		}
		newOffset, err := storage.WriteEntryAt(e, w.file, w.offset)
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
func (w *Writer) Finish() error {
	if w.finished {
		return nil // already finished
	}

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

	w.finished = true
	return nil
}

// Close closes the underlying file
func (w *Writer) Close() error {
	if !w.finished {
		if err := w.Finish(); err != nil {
			_ = w.file.Close()
			return err
		}
	}
	return w.file.Close()
}

// Delete removes the SSTable file from disk
func (w *Writer) Delete() error {
	if err := w.Close(); err != nil {
		return err
	}
	return os.Remove(w.path)
}

// Path returns the SSTable file path
func (w *Writer) Path() string {
	return w.path
}
