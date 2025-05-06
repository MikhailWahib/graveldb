package sstable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb/internal/diskmanager/mockdm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSTableWriteRead(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "sstable_test")
	defer os.RemoveAll(tempDir)
	err := os.MkdirAll(tempDir, 0755)
	require.NoError(t, err)

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
	err = writer.Open(sstPath)
	require.NoError(t, err)

	// Write entries
	for _, data := range testData {
		err = writer.WriteEntry([]byte(data.key), []byte(data.value))
		require.NoError(t, err)
	}

	// Finish writing
	err = writer.Finish()
	require.NoError(t, err)

	// Read the SSTable
	reader := NewSSTReader(dm)
	err = reader.Open(sstPath)
	require.NoError(t, err)

	// Test lookup for each key
	for _, data := range testData {
		value, err := reader.Lookup([]byte(data.key))
		assert.NoError(t, err)
		assert.True(t, bytes.Equal(value, []byte(data.value)), "Value mismatch for %s: got %s, want %s", data.key, string(value), data.value)
	}

	// Test lookup for non-existent key
	_, err = reader.Lookup([]byte("nonexistent"))
	assert.Error(t, err)

	reader.Close()
}

func TestSSTableEmptyValue(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "sstable_test_empty")
	defer os.RemoveAll(tempDir)
	err := os.MkdirAll(tempDir, 0755)
	require.NoError(t, err)

	dm := mockdm.NewMockDiskManager()
	sstPath := filepath.Join(tempDir, "empty_value.sst")

	// Create SSTable with empty values
	writer := NewSSTWriter(dm)
	err = writer.Open(sstPath)
	require.NoError(t, err)

	err = writer.WriteEntry([]byte("key1"), []byte{})
	require.NoError(t, err)

	err = writer.Finish()
	require.NoError(t, err)

	// Read the SSTable
	reader := NewSSTReader(dm)
	err = reader.Open(sstPath)
	require.NoError(t, err)

	value, err := reader.Lookup([]byte("key1"))
	require.NoError(t, err)

	assert.Empty(t, value)

	reader.Close()
}

func TestSSTableLargeKeyValues(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "sstable_test_large")
	defer os.RemoveAll(tempDir)
	err := os.MkdirAll(tempDir, 0755)
	require.NoError(t, err)

	dm := mockdm.NewMockDiskManager()
	sstPath := filepath.Join(tempDir, "large_data.sst")

	// Create SSTable with large values
	writer := NewSSTWriter(dm)
	err = writer.Open(sstPath)
	require.NoError(t, err)

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
	require.NoError(t, err)

	// Add a normal key after the large one
	err = writer.WriteEntry([]byte("small-key"), []byte("small-value"))
	require.NoError(t, err)

	err = writer.Finish()
	require.NoError(t, err)

	// Read the SSTable
	reader := NewSSTReader(dm)
	err = reader.Open(sstPath)
	require.NoError(t, err)

	// Check the large key/value
	value, err := reader.Lookup(largeKey)
	require.NoError(t, err)

	assert.True(t, bytes.Equal(value, largeValue), "Large value mismatch: lengths got %d, want %d", len(value), len(largeValue))

	// Check we can still find the small key
	smallValue, err := reader.Lookup([]byte("small-key"))
	require.NoError(t, err)

	assert.Equal(t, "small-value", string(smallValue))

	reader.Close()
}

func BenchmarkSSTableWriting(b *testing.B) {
	tempDir := filepath.Join(os.TempDir(), "sstable_bench_write")
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
			b.Fatalf("Value mismatch: got %s, want %s", string(value), string(expectedValue))
		}
	}
}

func TestNonExistentKeyLookup(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "sstable_test_missing")
	defer os.RemoveAll(tempDir)
	err := os.MkdirAll(tempDir, 0755)
	require.NoError(t, err)

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
		assert.Error(t, err, "Expected error for missing key %s, got nil", key)
	}

	// Test keys that are completely out of range
	outOfRangeKeys := []string{"0", "z"}
	for _, key := range outOfRangeKeys {
		_, err := reader.Lookup([]byte(key))
		assert.Error(t, err, "Expected error for out-of-range key %s, got nil", key)
	}

	reader.Close()
}
