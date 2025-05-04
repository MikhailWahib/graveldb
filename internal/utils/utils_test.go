package utils_test

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb/internal/diskmanager"
	"github.com/MikhailWahib/graveldb/internal/utils"
)

func TestWriteEntryWithPrefix(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.db")

	dm := diskmanager.NewDiskManager()
	fh, err := dm.Open(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer func() {
		_ = dm.Close(filePath)
		_ = dm.Delete(filePath)
	}()

	key := []byte("mykey")
	value := []byte("myvalue")
	offset := int64(0)

	newOffset, err := utils.WriteEntryWithPrefix(fh, offset, key, value)
	if err != nil {
		t.Fatalf("WriteEntryWithPrefix failed: %v", err)
	}

	expectedLen := 4 + 4 + len(key) + len(value)
	if newOffset != offset+int64(expectedLen) {
		t.Errorf("unexpected new offset: got %d, want %d", newOffset, offset+int64(expectedLen))
	}

	// Read back the data
	buf := make([]byte, expectedLen)
	_, err = fh.ReadAt(buf, offset)
	if err != nil {
		t.Fatalf("failed to read written data: %v", err)
	}

	keyLen := binary.BigEndian.Uint32(buf[:4])
	readKey := buf[8 : 8+keyLen]
	readValue := buf[8+keyLen:]

	if !bytes.Equal(readKey, key) {
		t.Errorf("key mismatch: got %s, want %s", readKey, key)
	}
	if !bytes.Equal(readValue, value) {
		t.Errorf("value mismatch: got %s, want %s", readValue, value)
	}
}

func TestReadEntryWithPrefix(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.db")

	dm := diskmanager.NewDiskManager()
	fh, err := dm.Open(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer func() {
		_ = dm.Close(filePath)
		_ = dm.Delete(filePath)
	}()

	key := []byte("mykey")
	value := []byte("myvalue")
	offset := int64(0)

	// Write an entry to the file
	_, err = utils.WriteEntryWithPrefix(fh, offset, key, value)
	if err != nil {
		t.Fatalf("WriteEntryWithPrefix failed: %v", err)
	}

	// Now, read the entry back
	readKey, readValue, newOffset, err := utils.ReadEntryWithPrefix(fh, offset)
	if err != nil {
		t.Fatalf("ReadEntryWithPrefix failed: %v", err)
	}

	if !bytes.Equal(readKey, key) {
		t.Errorf("key mismatch: got %s, want %s", readKey, key)
	}
	if !bytes.Equal(readValue, value) {
		t.Errorf("value mismatch: got %s, want %s", readValue, value)
	}

	expectedLen := 4 + 4 + len(key) + len(value)
	if newOffset != offset+int64(expectedLen) {
		t.Errorf("unexpected new offset: got %d, want %d", newOffset, offset+int64(expectedLen))
	}
}
