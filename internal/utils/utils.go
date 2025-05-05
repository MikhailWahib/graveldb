package utils

import (
	"encoding/binary"

	"github.com/MikhailWahib/graveldb/internal/diskmanager"
)

// WriteEntryWithPrefix writes a key-value or key-only entry to the file using a length-prefixed format.
// Format: [4 bytes KeyLen][4 bytes ValueLen][Key][Value]
// If value is nil or empty, only the key is written with ValueLen set to 0.
func WriteEntryWithPrefix(f diskmanager.FileHandle, offset int64, key []byte, value []byte) (int64, error) {
	keyLen := len(key)
	valueLen := len(value)

	buf := make([]byte, 4+4+keyLen+valueLen) // 4 bytes keyLen + 4 bytes valueLen + key + value

	binary.BigEndian.PutUint32(buf[0:4], uint32(keyLen))
	binary.BigEndian.PutUint32(buf[4:8], uint32(valueLen))
	copy(buf[8:], key)
	if valueLen > 0 {
		copy(buf[8+keyLen:], value)
	}

	n, err := f.WriteAt(buf, offset)
	if err != nil {
		return 0, err
	}

	return offset + int64(n), nil
}

// ReadEntryWithPrefix reads a key-value entry from the file with a length-prefixed format.
// Format: [4 bytes KeyLen][4 bytes ValueLen][Key][Value]
func ReadEntryWithPrefix(f diskmanager.FileHandle, offset int64) (key, value []byte, newOffset int64, err error) {
	// First, read the length of the key and value
	lenBuf := make([]byte, 8)
	_, err = f.ReadAt(lenBuf, offset)
	if err != nil {
		return nil, nil, offset, err
	}

	// Decode key length and value length
	keyLen := binary.BigEndian.Uint32(lenBuf[:4])
	valLen := binary.BigEndian.Uint32(lenBuf[4:8])

	// Prepare buffers for key and value
	key = make([]byte, keyLen)
	value = make([]byte, valLen)

	// Read key and value from file
	_, err = f.ReadAt(key, offset+8)
	if err != nil {
		return nil, nil, offset, err
	}

	_, err = f.ReadAt(value, offset+8+int64(keyLen))
	if err != nil {
		return nil, nil, offset, err
	}

	// New offset is the position after the current entry
	newOffset = offset + 8 + int64(keyLen) + int64(valLen)
	return key, value, newOffset, nil
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
