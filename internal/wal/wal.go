// Package wal implements Write-Ahead Logging for durability
package wal

import (
	"io"
	"os"

	"github.com/MikhailWahib/graveldb/internal/shared"
)

// Entry represents a single write-ahead log entry
type Entry struct {
	Type  shared.EntryType
	Key   string
	Value string
}

// WAL manages the write-ahead log file
type WAL struct {
	path        string
	file        *os.File
	writeOffset int64
}

// NewWAL creates a new WAL that uses DiskManager for file operations
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
func (w *WAL) AppendPut(key, value string) error {
	return w.writeEntry(Entry{
		Type:  shared.PutEntry,
		Key:   key,
		Value: value,
	})
}

// AppendDelete appends a delete operation to the WAL
func (w *WAL) AppendDelete(key string) error {
	return w.writeEntry(Entry{
		Type:  shared.DeleteEntry,
		Key:   key,
		Value: "",
	})
}

// writeEntry formats an entry and writes it using the file handle
// Format: [1 byte Type][4 bytes KeyLen][4 bytes ValueLen][Key][Value]
func (w *WAL) writeEntry(e Entry) error {
	// Write the entry type, key length, value length, key, and value.
	// offset added by one because of the added byte above.
	n, err := shared.WriteEntry(shared.Entry{
		File:   w.file,
		Offset: w.writeOffset,
		Type:   shared.EntryType(e.Type),
		Key:    []byte(e.Key),
		Value:  []byte(e.Value),
	})
	if err != nil {
		return err
	}

	// Update the write offset
	w.writeOffset = n

	// Sync after each write for durability
	return w.Sync()
}

// Replay reads entries from the beginning, returning 0 offset at EOF
func (w *WAL) Replay() ([]Entry, error) {
	var offset int64
	var walEntries []Entry

	for {
		entry, err := shared.ReadEntry(w.file, offset)
		if err != nil {
			if err == io.EOF || entry.NewOffset == 0 {
				break
			}

			return nil, err
		}
		offset = entry.NewOffset

		walEntry := Entry{
			Type:  shared.EntryType(entry.Type),
			Key:   string(entry.Key),
			Value: string(entry.Value),
		}
		walEntries = append(walEntries, walEntry)
	}

	return walEntries, nil
}

// Sync ensures all data is persisted to disk
func (w *WAL) Sync() error {
	return w.file.Sync()
}

// Close closes the WAL file
func (w *WAL) Close() error {
	if err := w.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}
