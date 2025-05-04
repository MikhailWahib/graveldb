package wal

import (
	"io"
	"os"

	"github.com/MikhailWahib/graveldb/internal/diskmanager"
	"github.com/MikhailWahib/graveldb/internal/utils"
)

type EntryType byte

const (
	PutEntry EntryType = iota
	DeleteEntry
)

type Entry struct {
	Type  EntryType
	Key   string
	Value string
}

type WAL struct {
	dm          diskmanager.DiskManager
	path        string
	fileHandle  diskmanager.FileHandle
	writeOffset int64
}

// NewWAL creates a new WAL that uses DiskManager for file operations
func NewWAL(dm diskmanager.DiskManager, path string) (*WAL, error) {
	fileHandle, err := dm.Open(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// Get current file size to set initial write offset
	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	return &WAL{
		dm:          dm,
		path:        path,
		fileHandle:  fileHandle,
		writeOffset: fileInfo.Size(),
	}, nil
}

// AppendPut appends a put operation to the WAL
func (w *WAL) AppendPut(key, value string) error {
	return w.writeEntry(Entry{
		Type:  PutEntry,
		Key:   key,
		Value: value,
	})
}

// AppendDelete appends a delete operation to the WAL
func (w *WAL) AppendDelete(key string) error {
	return w.writeEntry(Entry{
		Type:  DeleteEntry,
		Key:   key,
		Value: "",
	})
}

// writeEntry formats an entry and writes it using the file handle
// Format: [1 byte Type][4 bytes KeyLen][4 bytes ValueLen][Key][Value]
func (w *WAL) writeEntry(e Entry) error {
	// Write the entry type byte first
	_, err := w.fileHandle.WriteAt([]byte{byte(e.Type)}, w.writeOffset)
	if err != nil {
		return err
	}

	// Write the entry type, key length, value length, key, and value.
	// offset added by one because of the added byte above.
	n, err := utils.WriteEntryWithPrefix(w.fileHandle, w.writeOffset+1, []byte(e.Key), []byte(e.Value))
	if err != nil {
		return err
	}

	// Update the write offset
	w.writeOffset = n

	// Sync after each write for durability
	return w.Sync()
}

// Replay reads all WAL entries from the beginning of the file
func (w *WAL) Replay() ([]Entry, error) {
	var entries []Entry
	var offset int64 = 0
	tByte := make([]byte, 1)

	for {
		// Read entry type (1 byte)
		n, err := w.fileHandle.ReadAt(tByte, offset)
		if err != nil {
			if err == io.EOF || n == 0 {
				break // Reached end of file
			}

			return nil, err
		}
		offset += int64(n)

		keyData, valueData, newOffset, err := utils.ReadEntryWithPrefix(w.fileHandle, offset)
		if err != nil {
			return nil, err
		}
		offset = newOffset

		entry := Entry{
			Type:  EntryType(tByte[0]),
			Key:   string(keyData),
			Value: string(valueData),
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// Sync ensures all data is persisted to disk
func (w *WAL) Sync() error {
	return w.fileHandle.Sync()
}

// Close closes the WAL file
func (w *WAL) Close() error {
	if err := w.Sync(); err != nil {
		return err
	}
	return w.dm.Close(w.path)
}
