package sstable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb/internal/diskmanager/mockdm"
)

func TestSSTableWriteRead(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "sstable_test")
	os.MkdirAll(tempDir, 0755)
	defer os.RemoveAll(tempDir)

	dm := mockdm.NewMockDiskManager()

	testData := []struct {
		key   string
		value string
	}{
		{"apple", "red"},
		{"banana", "yellow"},
		{"cherry", "dark red"},
		{"date", "brown"},
	}

	// Create the SSTable
	sstPath := filepath.Join(tempDir, "test.sst")
	writer := NewSSTWriter(dm)

	err := writer.Open(sstPath)
	if err != nil {
		t.Fatalf("Failed to open writer: %v", err)
	}

	// Write entries
	for _, data := range testData {
		err = writer.WriteEntry([]byte(data.key), []byte(data.value))
		if err != nil {
			t.Fatalf("Failed to write entry %s: %v", data.key, err)
		}
	}

	// Finish writing
	err = writer.Finish()
	if err != nil {
		t.Fatalf("Failed to finish writing: %v", err)
	}

	// Read the SSTable
	reader := NewSSTReader(dm)
	err = reader.Open(sstPath)
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}

	// Test lookup for each key
	for _, data := range testData {
		value, err := reader.Lookup([]byte(data.key))
		if err != nil {
			t.Errorf("Failed to lookup key %s: %v", data.key, err)
			continue
		}

		if !bytes.Equal(value, []byte(data.value)) {
			t.Errorf("Value mismatch for %s: got %s, want %s",
				data.key, string(value), data.value)
		}
	}

	// Test lookup for non-existent key
	_, err = reader.Lookup([]byte("nonexistent"))
	if err == nil {
		t.Error("Expected error for non-existent key, got nil")
	}

	reader.Close()
}

func TestSSTableEmptyValue(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "sstable_test_empty")
	os.MkdirAll(tempDir, 0755)
	defer os.RemoveAll(tempDir)

	dm := mockdm.NewMockDiskManager()
	sstPath := filepath.Join(tempDir, "empty_value.sst")

	// Create SSTable with empty values
	writer := NewSSTWriter(dm)
	err := writer.Open(sstPath)
	if err != nil {
		t.Fatalf("Failed to open writer: %v", err)
	}

	err = writer.WriteEntry([]byte("key1"), []byte{})
	if err != nil {
		t.Fatalf("Failed to write entry with empty value: %v", err)
	}

	err = writer.Finish()
	if err != nil {
		t.Fatalf("Failed to finish writing: %v", err)
	}

	// Read the SSTable
	reader := NewSSTReader(dm)
	err = reader.Open(sstPath)
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}

	value, err := reader.Lookup([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to lookup key with empty value: %v", err)
	}

	if len(value) != 0 {
		t.Errorf("Expected empty value, got %v", value)
	}

	reader.Close()
}

func TestSSTTableLargeKeyValues(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "sstable_test_large")
	os.MkdirAll(tempDir, 0755)
	defer os.RemoveAll(tempDir)

	dm := mockdm.NewMockDiskManager()
	sstPath := filepath.Join(tempDir, "large_data.sst")

	// Create SSTable with large values
	writer := NewSSTWriter(dm)
	err := writer.Open(sstPath)
	if err != nil {
		t.Fatalf("Failed to open writer: %v", err)
	}

	// Create a 100KB value
	largeValue := make([]byte, 100*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Create a 10KB key
	largeKey := make([]byte, 10*1024)
	for i := range largeKey {
		largeKey[i] = byte((i * 7) % 256)
	}

	err = writer.WriteEntry(largeKey, largeValue)
	if err != nil {
		t.Fatalf("Failed to write large entry: %v", err)
	}

	// Add a normal key after the large one
	err = writer.WriteEntry([]byte("small-key"), []byte("small-value"))
	if err != nil {
		t.Fatalf("Failed to write small entry: %v", err)
	}

	err = writer.Finish()
	if err != nil {
		t.Fatalf("Failed to finish writing: %v", err)
	}

	// Read the SSTable
	reader := NewSSTReader(dm)
	err = reader.Open(sstPath)
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}

	// Check the large key/value
	value, err := reader.Lookup(largeKey)
	if err != nil {
		t.Errorf("Failed to lookup large key: %v", err)
	}

	if !bytes.Equal(value, largeValue) {
		t.Errorf("Large value mismatch: lengths got %d, want %d",
			len(value), len(largeValue))
	}

	// Check we can still find the small key
	smallValue, err := reader.Lookup([]byte("small-key"))
	if err != nil {
		t.Errorf("Failed to lookup small key after large key: %v", err)
	}

	if !bytes.Equal(smallValue, []byte("small-value")) {
		t.Errorf("Small value mismatch after large key")
	}

	reader.Close()
}

func BenchmarkSSTableWriting(b *testing.B) {
	tempDir := filepath.Join(os.TempDir(), "sstable_bench_write")
	os.MkdirAll(tempDir, 0755)
	defer os.RemoveAll(tempDir)

	dm := mockdm.NewMockDiskManager()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sstPath := filepath.Join(tempDir, "bench_write.sst")
		writer := NewSSTWriter(dm)
		writer.Open(sstPath)
		b.StartTimer()

		// Write 1000 entries
		for j := 0; j < 1000; j++ {
			key := []byte(fmt.Sprintf("key-%d", j))
			value := []byte(fmt.Sprintf("value-%d", j))
			err := writer.WriteEntry(key, value)
			if err != nil {
				b.Fatalf("Failed to write entry: %v", err)
			}
		}

		writer.Finish()
	}
}

func BenchmarkSSTableReading(b *testing.B) {
	tempDir := filepath.Join(os.TempDir(), "sstable_bench_read")
	os.MkdirAll(tempDir, 0755)
	defer os.RemoveAll(tempDir)

	dm := mockdm.NewMockDiskManager()
	sstPath := filepath.Join(tempDir, "bench_read.sst")

	// Create a benchmark SSTable first
	writer := NewSSTWriter(dm)
	writer.Open(sstPath)

	// Write 1000 entries
	for j := 0; j < 1000; j++ {
		key := []byte(fmt.Sprintf("key-%d", j))
		value := []byte(fmt.Sprintf("value-%d", j))
		writer.WriteEntry(key, value)
	}
	writer.Finish()

	// Now benchmark lookups
	reader := NewSSTReader(dm)
	reader.Open(sstPath)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Look up a random key
		keyNum := i % 1000
		key := []byte(fmt.Sprintf("key-%d", keyNum))
		value, err := reader.Lookup(key)
		if err != nil {
			b.Fatalf("Failed to lookup key: %v", err)
		}

		expectedValue := []byte(fmt.Sprintf("value-%d", keyNum))
		if !bytes.Equal(value, expectedValue) {
			b.Fatalf("Value mismatch: got %s, want %s",
				string(value), string(expectedValue))
		}
	}
}

func TestNonExistentKeyLookup(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "sstable_test_missing")
	os.MkdirAll(tempDir, 0755)
	defer os.RemoveAll(tempDir)

	dm := mockdm.NewMockDiskManager()
	sstPath := filepath.Join(tempDir, "test_missing.sst")

	// Create the SSTable with some entries
	writer := NewSSTWriter(dm)
	writer.Open(sstPath)

	writer.WriteEntry([]byte("a"), []byte("value-a"))
	writer.WriteEntry([]byte("c"), []byte("value-c"))
	writer.WriteEntry([]byte("e"), []byte("value-e"))
	writer.Finish()

	// Read the SSTable
	reader := NewSSTReader(dm)
	reader.Open(sstPath)

	// Test lookup for keys that don't exist but are within range
	testMissingKeys := []string{"b", "d", "f"}
	for _, key := range testMissingKeys {
		_, err := reader.Lookup([]byte(key))
		if err == nil {
			t.Errorf("Expected error for missing key %s, got nil", key)
		}
	}

	// Test keys that are completely out of range
	outOfRangeKeys := []string{"0", "z"}
	for _, key := range outOfRangeKeys {
		_, err := reader.Lookup([]byte(key))
		if err == nil {
			t.Errorf("Expected error for out-of-range key %s, got nil", key)
		}
	}

	reader.Close()
}

func TestCorruptedSSTable(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "sstable_test_corrupt")
	os.MkdirAll(tempDir, 0755)
	defer os.RemoveAll(tempDir)

	dm := mockdm.NewMockDiskManager()
	sstPath := filepath.Join(tempDir, "corrupted.sst")

	// Create a corrupted file (too small to be valid)
	file, _ := dm.Open(sstPath, os.O_CREATE|os.O_RDWR, 0644)
	file.WriteAt([]byte("corrupted data"), 0)

	// Try to read the corrupted SSTable
	reader := NewSSTReader(dm)
	err := reader.Open(sstPath)

	// Should fail to open
	if err == nil {
		t.Error("Expected error when opening corrupt SSTable, got nil")
	}
}

func TestMultipleOpenClose(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "sstable_test_multi")
	os.MkdirAll(tempDir, 0755)
	defer os.RemoveAll(tempDir)

	dm := mockdm.NewMockDiskManager()
	sstPath := filepath.Join(tempDir, "test_multi.sst")

	// Create the SSTable
	writer := NewSSTWriter(dm)
	writer.Open(sstPath)
	writer.WriteEntry([]byte("key1"), []byte("value1"))
	writer.WriteEntry([]byte("key2"), []byte("value2"))
	writer.Finish()

	// Open and close multiple times
	for i := 0; i < 5; i++ {
		reader := NewSSTReader(dm)
		err := reader.Open(sstPath)
		if err != nil {
			t.Fatalf("Failed to open reader on iteration %d: %v", i, err)
		}

		v, err := reader.Lookup([]byte("key1"))
		if err != nil || !bytes.Equal(v, []byte("value1")) {
			t.Errorf("Failed lookup on iteration %d", i)
		}

		reader.Close()
	}
}
