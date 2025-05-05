package sstable

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/MikhailWahib/graveldb/internal/diskmanager"
	"github.com/MikhailWahib/graveldb/internal/utils"
)

type SSTWriter struct {
	dm        diskmanager.DiskManager
	file      diskmanager.FileHandle
	index     []IndexEntry
	offset    int64
	indexSize int64
}

type IndexEntry struct {
	Key    []byte
	Offset int64
}

func NewSSTWriter(dm diskmanager.DiskManager) *SSTWriter {
	return &SSTWriter{
		dm:     dm,
		index:  make([]IndexEntry, 0),
		offset: 0,
	}
}

// Open prepares the writer for a new SSTable file
func (w *SSTWriter) Open(filename string) error {
	fileHandle, err := w.dm.Open(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	w.file = fileHandle
	return nil
}

// WriteEntry writes a key-value pair to the data section
func (w *SSTWriter) WriteEntry(key, value []byte) error {
	entryOffset := w.offset

	// Write the entry prefixed with k,v lengths.
	n, err := utils.WriteEntryWithPrefix(w.file, w.offset, key, value)
	if err != nil {
		return err
	}
	w.offset += n

	w.index = append(w.index, IndexEntry{Key: key, Offset: entryOffset})
	return nil
}

// WriteIndex writes the sparse index for faster lookups
func (w *SSTWriter) WriteIndex(index []IndexEntry) error {
	indexStartOffset := w.offset

	for _, entry := range index {
		// Write key with prefix
		n, err := utils.WriteEntryWithPrefix(w.file, w.offset, entry.Key, nil)
		if err != nil {
			return err
		}
		w.offset = n

		// Write offset
		offsetBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(offsetBytes, uint64(entry.Offset))
		written, err := w.file.WriteAt(offsetBytes, w.offset)
		if err != nil {
			return err
		}
		w.offset += int64(written)
	}

	// Calculate actual index size
	w.indexSize = w.offset - indexStartOffset
	return nil
}

// Finish finalizes the SSTable and closes the file
func (w *SSTWriter) Finish() error {
	// Write the index section to the file
	indexOffset := w.offset // The current offset will be the start of the index section
	if err := w.WriteIndex(w.index); err != nil {
		return fmt.Errorf("failed to write index: %w", err)
	}

	// Write the footer section at the end of the file
	// The footer contains:
	// - The offset of the index section
	// - The size of the index section

	// Footer data (index offset and index size)
	footer := make([]byte, 16)
	binary.BigEndian.PutUint64(footer[:8], uint64(indexOffset))
	binary.BigEndian.PutUint64(footer[8:], uint64(w.indexSize))

	// Write the footer to the file
	_, err := w.file.WriteAt(footer, w.offset)
	if err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}

	// Update the offset for the footer (move past it)
	w.offset += int64(len(footer))

	// sync the file to make sure everything is written to disk
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	return nil
}
