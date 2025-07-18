// Package wal implements Write-Ahead Logging for durability
package wal

import (
	"bufio"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/MikhailWahib/graveldb/internal/shared"
)

// WAL manages the write-ahead log file
type WAL struct {
	mu sync.Mutex

	path        string
	file        *os.File
	writeOffset int64
}

// NewWAL creates a new WAL
func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// Get current file size to set initial write offset
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &WAL{
		path:        path,
		file:        file,
		writeOffset: fileInfo.Size(),
	}, nil
}

// AppendPut appends a put operation to the WAL
func (w *WAL) AppendPut(key, value []byte) error {
	return w.writeEntry(shared.Entry{
		Type:  shared.PutEntry,
		Key:   key,
		Value: value,
	})
}

// AppendDelete appends a delete operation to the WAL
func (w *WAL) AppendDelete(key []byte) error {
	return w.writeEntry(shared.Entry{
		Type:  shared.DeleteEntry,
		Key:   key,
		Value: []byte{},
	})
}

// writeEntry writes an entry to the disk and immediately sync it
func (w *WAL) writeEntry(e shared.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	n, err := shared.WriteEntryAt(e, w.file, w.writeOffset)
	if err != nil {
		return err
	}

	// Update the write offset
	w.writeOffset = n

	// Sync after each write for durability
	return w.sync()
}

// Replay reads entries from the beginning, returning 0 offset at EOF
func (w *WAL) Replay() ([]shared.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	_, err := w.file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(w.file)

	var walEntries []shared.Entry

	for {
		entry, err := shared.ReadEntryFromReader(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		walEntries = append(walEntries, shared.Entry{
			Type:  entry.Type,
			Key:   entry.Key,
			Value: entry.Value,
		})
	}

	return walEntries, nil
}

// Sync ensures all data is persisted to disk
func (w *WAL) sync() error {
	return w.file.Sync()
}

// Close closes the WAL file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.sync(); err != nil {
		return err
	}
	return w.file.Close()
}
