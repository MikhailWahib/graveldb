package shared_test

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb/internal/diskmanager/mockdm"
	"github.com/MikhailWahib/graveldb/internal/shared"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteEntryWithPrefix(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.db")

	dm := mockdm.NewMockDiskManager()
	fh, err := dm.Open(filePath, os.O_CREATE|os.O_RDWR, 0644)
	require.NoError(t, err)
	defer func() {
		_ = dm.Close(filePath)
		_ = dm.Delete(filePath)
	}()

	key := []byte("mykey")
	value := []byte("myvalue")
	offset := int64(0)

	// Write entry with prefix
	newOffset, err := shared.WriteEntryWithPrefix(shared.WriteEntry{
		FileHandle: fh,
		Offset:     offset,
		Type:       shared.PutEntry,
		Key:        key,
		Value:      value,
	})
	require.NoError(t, err)

	expectedLen := 1 + 4 + 4 + len(key) + len(value)
	expectedOffset := offset + int64(expectedLen)
	assert.Equal(t, expectedOffset, newOffset, "unexpected new offset")

	// Read back the data
	buf := make([]byte, expectedLen)
	_, err = fh.ReadAt(buf, offset)
	require.NoError(t, err)

	entryType := buf[0]
	keyLen := binary.BigEndian.Uint32(buf[1:5])
	readKey := buf[9 : 9+keyLen]
	readValue := buf[9+keyLen:]

	// Validate entryType, key and value
	assert.Equal(t, shared.PutEntry, shared.EntryType(entryType), "entry type mismatch")
	assert.Equal(t, key, readKey, "key mismatch")
	assert.Equal(t, value, readValue, "value mismatch")
}

func TestReadEntryWithPrefix(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.db")

	dm := mockdm.NewMockDiskManager()
	fh, err := dm.Open(filePath, os.O_CREATE|os.O_RDWR, 0644)
	require.NoError(t, err)
	defer func() {
		_ = dm.Close(filePath)
		_ = dm.Delete(filePath)
	}()

	key := []byte("mykey")
	value := []byte("myvalue")
	offset := int64(0)

	// Write entry with prefix
	_, err = shared.WriteEntryWithPrefix(shared.WriteEntry{
		FileHandle: fh,
		Offset:     offset,
		Type:       shared.DeleteEntry,
		Key:        key,
		Value:      value,
	})
	require.NoError(t, err)

	// Read the entry back
	e := shared.ReadEntryWithPrefix(fh, offset)
	require.NoError(t, err)

	// Validate key and value
	assert.Equal(t, key, e.Key, "key mismatch")
	assert.Equal(t, value, e.Value, "value mismatch")

	expectedLen := 1 + 4 + 4 + len(key) + len(value)
	expectedOffset := offset + int64(expectedLen)
	assert.Equal(t, expectedOffset, e.NewOffset, "unexpected new offset")
	assert.Equal(t, shared.DeleteEntry, shared.EntryType(e.Type), "entry type mismatch")
}
