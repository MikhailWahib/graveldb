package shared

import (
	"encoding/binary"
	"fmt"

	"github.com/MikhailWahib/graveldb/internal/diskmanager"
)

type WriteEntry struct {
	FileHandle diskmanager.FileHandle
	Offset     int64
	Type       EntryType
	Key        []byte
	Value      []byte
}

// WriteEntryWithPrefix writes a key-value or key-only entry to the file using a length-prefixed format.
// Format: [1 byte EntryType][4 bytes KeyLen][4 bytes ValueLen][Key][Value]
// If value is nil or empty, only the key is written with ValueLen set to 0.
func WriteEntryWithPrefix(e WriteEntry) (int64, error) {
	keyLen := len(e.Key)
	valueLen := len(e.Value)

	buf := make([]byte, 1+4+4+keyLen+valueLen) // 1 byte EntryType + 4 bytes keyLen + 4 bytes valueLen + key + value

	buf[0] = byte(e.Type)
	binary.BigEndian.PutUint32(buf[1:5], uint32(keyLen))
	binary.BigEndian.PutUint32(buf[5:9], uint32(valueLen))
	copy(buf[9:], e.Key)
	if valueLen > 0 {
		copy(buf[9+keyLen:], e.Value)
	}

	n, err := e.FileHandle.WriteAt(buf, e.Offset)
	if err != nil {
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	return e.Offset + int64(n), nil
}

type ReadEntryReturn struct {
	Type      byte
	Key       []byte
	Value     []byte
	NewOffset int64
	Err       error
}

// ReadEntryWithPrefix reads a key-value entry from the file with a length-prefixed format.
// Format: [1 byte EntryType][4 bytes KeyLen][4 bytes ValueLen][Key][Value]
func ReadEntryWithPrefix(f diskmanager.FileHandle, offset int64) ReadEntryReturn {
	// read the length of the EntryType key and value
	lenBuf := make([]byte, 9)
	_, err := f.ReadAt(lenBuf, offset)
	if err != nil {
		return ReadEntryReturn{Err: fmt.Errorf("failed to read entry length: %w", err)}
	}

	// Decode EntryType, key length and value length
	typeByte := lenBuf[0]
	keyLen := binary.BigEndian.Uint32(lenBuf[1:5])
	valLen := binary.BigEndian.Uint32(lenBuf[5:9])

	// Prepare buffers for key and value
	key := make([]byte, keyLen)
	value := make([]byte, valLen)

	// Read key and value from file
	_, err = f.ReadAt(key, offset+9)
	if err != nil {
		return ReadEntryReturn{Err: fmt.Errorf("failed to read key: %w", err)}
	}

	_, err = f.ReadAt(value, offset+9+int64(keyLen))
	if err != nil {
		return ReadEntryReturn{Err: fmt.Errorf("failed to read key: %w", err)}
	}

	// New offset is the position after the current entry
	newOffset := offset + 9 + int64(keyLen) + int64(valLen)

	return ReadEntryReturn{
		Type:      typeByte,
		Key:       key,
		Value:     value,
		NewOffset: newOffset,
	}
}

// CompareKeys compares bytes lexicographically
func CompareKeys(a, b []byte) int {
	min := len(a)
	if len(b) < min {
		min = len(b)
	}
	for i := range min {
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
