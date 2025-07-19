package record_test

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb/internal/record"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteEntry(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.db")

	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	require.NoError(t, err)

	key := []byte("mykey")
	value := []byte("myvalue")
	offset := int64(0)
	entry := record.Entry{
		Type:  record.PutEntry,
		Key:   key,
		Value: value,
	}

	// Write entry with prefix
	newOffset, err := record.WriteEntryAt(entry, f, offset)
	require.NoError(t, err)

	expectedLen := 1 + 4 + 4 + len(key) + len(value)
	expectedOffset := offset + int64(expectedLen)
	assert.Equal(t, expectedOffset, newOffset, "unexpected new offset")

	// Read back the data
	buf := make([]byte, expectedLen)
	_, err = f.ReadAt(buf, offset)
	require.NoError(t, err)

	entryType := buf[0]
	keyLen := binary.BigEndian.Uint32(buf[1:5])
	readKey := buf[9 : 9+keyLen]
	readValue := buf[9+keyLen:]

	// Validate entryType, key and value
	assert.Equal(t, record.PutEntry, record.EntryType(entryType), "entry type mismatch")
	assert.Equal(t, key, readKey, "key mismatch")
	assert.Equal(t, value, readValue, "value mismatch")
}

func TestReadEntry(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.db")

	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	require.NoError(t, err)

	key := []byte("mykey")
	value := []byte("myvalue")
	offset := int64(0)
	e := record.Entry{
		Type:  record.DeleteEntry,
		Key:   key,
		Value: value,
	}
	// Write entry with prefix
	_, err = record.WriteEntryAt(e, f, offset)
	require.NoError(t, err)

	// Read the entry back
	entry, newOffset, err := record.ReadEntryAt(f, offset)
	require.NoError(t, err)

	// Validate key and value
	assert.Equal(t, key, entry.Key, "key mismatch")
	assert.Equal(t, value, entry.Value, "value mismatch")

	expectedLen := 1 + 4 + 4 + len(key) + len(value)
	expectedOffset := offset + int64(expectedLen)
	assert.Equal(t, expectedOffset, newOffset, "unexpected new offset")
	assert.Equal(t, record.DeleteEntry, record.EntryType(entry.Type), "entry type mismatch")
}
