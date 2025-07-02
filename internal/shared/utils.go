package shared

import (
	"encoding/binary"
	"fmt"

	"github.com/MikhailWahib/graveldb/internal/diskmanager"
)

// Entry represents a database entry to be written to storage
type Entry struct {
	File   diskmanager.FileHandle
	Offset int64
	Type   EntryType
	Key    []byte
	Value  []byte
}

// WriteEntry writes a key-value or key-only entry to the file using a length-prefixed format.
// Format: [1 byte EntryType][4 bytes KeyLen][4 bytes ValueLen][Key][Value]
// If value is nil or empty, only the key is written with ValueLen set to 0.
func WriteEntry(e Entry) (int64, error) {
	keyLen := len(e.Key)
	valueLen := len(e.Value)

	buf := make([]byte, PrefixSize+keyLen+valueLen) // 1 byte EntryType + 4 bytes keyLen + 4 bytes valueLen + key + value

	buf[0] = byte(e.Type)
	binary.BigEndian.PutUint32(buf[EntryTypeSize:EntryTypeSize+LengthSize], uint32(keyLen))
	binary.BigEndian.PutUint32(buf[EntryTypeSize+LengthSize:EntryTypeSize+(LengthSize*2)], uint32(valueLen))
	copy(buf[9:], e.Key)
	if valueLen > 0 {
		copy(buf[9+keyLen:], e.Value)
	}

	n, err := e.File.WriteAt(buf, e.Offset)
	if err != nil {
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	return e.Offset + int64(n), nil
}

// StoredEntry represents an entry read from storage
type StoredEntry struct {
	Type      EntryType
	Key       []byte
	Value     []byte
	NewOffset int64
}

// ReadEntry reads a key-value entry from the file with a length-prefixed format.
// Format: [1 byte EntryType][4 bytes KeyLen][4 bytes ValueLen][Key][Value]
func ReadEntry(f diskmanager.FileHandle, offset int64) (StoredEntry, error) {
	// read the length of the EntryType key and value
	lenBuf := make([]byte, PrefixSize)
	_, err := f.ReadAt(lenBuf, offset)
	if err != nil {
		return StoredEntry{}, err
	}

	// Decode EntryType, key length and value length
	entryType := EntryType(lenBuf[0])
	keyLen := binary.BigEndian.Uint32(lenBuf[EntryTypeSize : EntryTypeSize+LengthSize])
	valLen := binary.BigEndian.Uint32(lenBuf[EntryTypeSize+LengthSize : EntryTypeSize+(LengthSize*2)])

	// Prepare buffers for key and value
	key := make([]byte, keyLen)
	value := make([]byte, valLen)

	// Read key and value from file
	_, err = f.ReadAt(key, offset+PrefixSize)
	if err != nil {
		return StoredEntry{}, err
	}

	_, err = f.ReadAt(value, offset+PrefixSize+int64(keyLen))
	if err != nil {
		return StoredEntry{}, err
	}

	// New offset is the position after the current entry
	newOffset := offset + PrefixSize + int64(keyLen) + int64(valLen)

	return StoredEntry{
		Type:      entryType,
		Key:       key,
		Value:     value,
		NewOffset: newOffset,
	}, nil
}

// CompareKeys compares bytes lexicographically
func CompareKeys(a, b []byte) int {
	minLen := min(len(b), len(a))
	for i := range minLen {
		if a[i] < b[i] {
			return -1
		} else if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
