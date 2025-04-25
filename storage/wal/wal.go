package wal

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/MikhailWahib/graveldb/storage/diskmanager"
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

type WAL interface {
	AppendPut(key, value string) error
	AppendDelete(key string) error
	Replay() ([]Entry, error)
	Sync() error
	Close() error
}

type wal struct {
	dm          diskmanager.DiskManager
	path        string
	writeOffset int64
}

// NewWAL creates a new WAL that uses DiskManager for file operations
func NewWAL(dm diskmanager.DiskManager, path string) (WAL, error) {
	_, err := dm.Open(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// Get current file size to set initial write offset
	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	return &wal{
		dm:          dm,
		path:        path,
		writeOffset: fileInfo.Size(),
	}, nil
}

func (w *wal) AppendPut(key, value string) error {
	return w.writeEntry(Entry{
		Type:  PutEntry,
		Key:   key,
		Value: value,
	})
}

func (w *wal) AppendDelete(key string) error {
	return w.writeEntry(Entry{
		Type:  DeleteEntry,
		Key:   key,
		Value: "",
	})
}

// writeEntry formats an entry and writes it using the disk manager
// Format: [1 byte Type][4 bytes KeyLen][4 bytes ValueLen][Key][Value]
func (w *wal) writeEntry(e Entry) error {
	keyBytes := []byte(e.Key)
	valBytes := []byte(e.Value)

	// Calculate total entry size
	totalLen := 1 + 4 + 4 + len(keyBytes) + len(valBytes)
	buf := make([]byte, totalLen)

	// Format the entry
	buf[0] = byte(e.Type)
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(keyBytes)))
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(valBytes)))
	copy(buf[9:], keyBytes)
	copy(buf[9+len(keyBytes):], valBytes)

	// Write to file at current offset
	n, err := w.dm.WriteAt(w.path, buf, w.writeOffset)
	if err != nil {
		return err
	}

	// Update write offset
	w.writeOffset += int64(n)

	// Sync after each write for durability
	return w.Sync()
}

// Replay reads all WAL entries from the beginning of the file
func (w *wal) Replay() ([]Entry, error) {
	var entries []Entry
	var offset int64 = 0

	for {
		// Read entry type (1 byte)
		tByte := make([]byte, 1)
		n, err := w.dm.ReadAt(w.path, tByte, offset)
		if err == io.EOF || n == 0 {
			break // Reached end of file
		} else if err != nil {
			return nil, err
		}
		offset += int64(n)

		// Read key and value lengths (4 bytes each)
		lenBuf := make([]byte, 8)
		n, err = w.dm.ReadAt(w.path, lenBuf, offset)
		if err != nil {
			return nil, err
		}
		offset += int64(n)

		keyLen := binary.BigEndian.Uint32(lenBuf[0:4])
		valLen := binary.BigEndian.Uint32(lenBuf[4:8])

		// Read the key
		keyData := make([]byte, keyLen)
		n, err = w.dm.ReadAt(w.path, keyData, offset)
		if err != nil {
			return nil, err
		}
		offset += int64(n)

		// Read the value
		valueData := make([]byte, valLen)
		n, err = w.dm.ReadAt(w.path, valueData, offset)
		if err != nil {
			return nil, err
		}
		offset += int64(n)

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
func (w *wal) Sync() error {
	return w.dm.Sync(w.path)
}

// Close closes the WAL file
func (w *wal) Close() error {
	if err := w.Sync(); err != nil {
		return err
	}
	return w.dm.Close(w.path)
}
