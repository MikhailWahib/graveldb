package wal

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
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
	file   *os.File
	writer *bufio.Writer
}

func NewWAL(path string) (WAL, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &wal{
		file:   file,
		writer: bufio.NewWriter(file),
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

func (w *wal) writeEntry(e Entry) error {
	// Format:
	// [1 byte Type][4 bytes KeyLen][4 bytes ValueLen][Key][Value]
	keyBytes := []byte(e.Key)
	valBytes := []byte(e.Value)

	buf := make([]byte, 1+4+4+len(keyBytes)+len(valBytes))
	buf[0] = byte(e.Type)

	binary.BigEndian.PutUint32(buf[1:5], uint32(len(keyBytes)))
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(valBytes)))

	copy(buf[9:], keyBytes)
	copy(buf[9+len(keyBytes):], valBytes)

	_, err := w.writer.Write(buf)
	return err
}

func (w *wal) Replay() ([]Entry, error) {
	// Rewind to beginning
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	reader := bufio.NewReader(w.file)

	var entries []Entry
	for {
		// Read entry type
		t, err := reader.ReadByte()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		// Read lengths
		keyLenBuf := make([]byte, 4)
		valLenBuf := make([]byte, 4)
		if _, err := io.ReadFull(reader, keyLenBuf); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(reader, valLenBuf); err != nil {
			return nil, err
		}
		keyLen := binary.BigEndian.Uint32(keyLenBuf)
		valLen := binary.BigEndian.Uint32(valLenBuf)

		key := make([]byte, keyLen)
		value := make([]byte, valLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, err
		}

		entry := Entry{
			Type:  EntryType(t),
			Key:   string(key),
			Value: string(value),
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (w *wal) Sync() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *wal) Close() error {
	if err := w.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}
