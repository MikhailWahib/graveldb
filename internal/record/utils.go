package record

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// WriteEntryAt writes an entry to the given file at the specified offset using a length-prefixed format.
// Format: [1 byte EntryType][4 bytes KeyLen][4 bytes ValueLen][Key][Value]
// If the value is nil or empty, only the key is written with ValueLen set to 0.
func WriteEntryAt(e Entry, file *os.File, offset int64) (int64, error) {
	keyLen := len(e.Key)
	valueLen := len(e.Value)

	buf := make([]byte, PrefixSize+keyLen+valueLen)
	buf[0] = byte(e.Type)
	binary.BigEndian.PutUint32(buf[EntryTypeSize:EntryTypeSize+LengthSize], uint32(keyLen))
	binary.BigEndian.PutUint32(buf[EntryTypeSize+LengthSize:EntryTypeSize+(LengthSize*2)], uint32(valueLen))
	copy(buf[9:], e.Key)
	if valueLen > 0 {
		copy(buf[9+keyLen:], e.Value)
	}

	n, err := file.WriteAt(buf, offset)
	if err != nil {
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	return offset + int64(n), nil
}

// ReadEntryAt reads an entry from a file at the given offset using a length-prefixed format.
// Format: [1 byte EntryType][4 bytes KeyLen][4 bytes ValueLen][Key][Value]
func ReadEntryAt(f *os.File, offset int64) (Entry, int64, error) {
	lenBuf := make([]byte, PrefixSize)
	_, err := f.ReadAt(lenBuf, offset)
	if err != nil {
		return Entry{}, 0, err
	}

	entryType := EntryType(lenBuf[0])
	keyLen := binary.BigEndian.Uint32(lenBuf[EntryTypeSize : EntryTypeSize+LengthSize])
	valLen := binary.BigEndian.Uint32(lenBuf[EntryTypeSize+LengthSize : EntryTypeSize+(LengthSize*2)])

	key := make([]byte, keyLen)
	value := make([]byte, valLen)

	_, err = f.ReadAt(key, offset+PrefixSize)
	if err != nil {
		return Entry{}, 0, err
	}

	_, err = f.ReadAt(value, offset+PrefixSize+int64(keyLen))
	if err != nil {
		return Entry{}, 0, err
	}

	newOffset := offset + PrefixSize + int64(keyLen) + int64(valLen)

	return Entry{
		Type:  entryType,
		Key:   key,
		Value: value,
	}, newOffset, nil
}

// ReadEntryFromReader reads a single entry from a buffered reader using a length-prefixed format.
// Used for sequential read (e.g., during SSTable scan or WAL replay).
func ReadEntryFromReader(r *bufio.Reader) (Entry, error) {
	lenBuf := make([]byte, PrefixSize)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return Entry{}, err
	}

	entryType := EntryType(lenBuf[0])
	keyLen := binary.BigEndian.Uint32(lenBuf[EntryTypeSize : EntryTypeSize+LengthSize])
	valLen := binary.BigEndian.Uint32(lenBuf[EntryTypeSize+LengthSize : EntryTypeSize+(LengthSize*2)])

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return Entry{}, err
	}

	value := make([]byte, valLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return Entry{}, err
	}

	return Entry{
		Type:  entryType,
		Key:   key,
		Value: value,
	}, nil
}

// DecodeEntry parses an entry from a byte slice.
// Returns the parsed entry and the number of bytes consumed.
// Expects the same length-prefixed format used in file encoding.
func DecodeEntry(buf []byte) (Entry, int, error) {
	if len(buf) < PrefixSize {
		return Entry{}, 0, io.ErrUnexpectedEOF
	}

	entryType := EntryType(buf[0])
	keyLen := binary.BigEndian.Uint32(buf[EntryTypeSize : EntryTypeSize+LengthSize])
	valLen := binary.BigEndian.Uint32(buf[EntryTypeSize+LengthSize : EntryTypeSize+(LengthSize*2)])
	totalLen := PrefixSize + int(keyLen) + int(valLen)

	if len(buf) < totalLen {
		return Entry{}, 0, io.ErrUnexpectedEOF
	}

	key := buf[PrefixSize : PrefixSize+keyLen]
	value := buf[PrefixSize+keyLen : totalLen]

	return Entry{
		Type:  entryType,
		Key:   key,
		Value: value,
	}, totalLen, nil
}

// SerializeEntry converts an Entry to a byte slice
func SerializeEntry(e Entry) []byte {
	keyLen := len(e.Key)
	valLen := len(e.Value)
	totalSize := PrefixSize + keyLen + valLen

	buf := make([]byte, totalSize)

	buf[0] = byte(e.Type)

	binary.BigEndian.PutUint32(buf[EntryTypeSize:EntryTypeSize+LengthSize], uint32(keyLen))
	binary.BigEndian.PutUint32(buf[EntryTypeSize+LengthSize:PrefixSize], uint32(valLen))

	copy(buf[PrefixSize:], e.Key)

	if valLen > 0 {
		copy(buf[PrefixSize+keyLen:], e.Value)
	}

	return buf
}
