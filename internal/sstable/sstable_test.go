package sstable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb/internal/diskmanager"
	"github.com/MikhailWahib/graveldb/internal/diskmanager/mockdm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) (string, diskmanager.DiskManager) {
	tempDir := filepath.Join(os.TempDir(), "sstable_test")
	err := os.MkdirAll(tempDir, 0755)
	require.NoError(t, err)

	dm := mockdm.NewMockDiskManager()
	return tempDir, dm
}

func TestSSTableWriteRead(t *testing.T) {
	tempDir, dm := setup(t)

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
	require.NoError(t, err)

	// Write entries
	for _, data := range testData {
		err = writer.AppendPut([]byte(data.key), []byte(data.value))
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
	tempDir, dm := setup(t)
	defer os.RemoveAll(tempDir)

	sstPath := filepath.Join(tempDir, "empty_value.sst")

	// Create SSTable with empty values
	writer := NewSSTWriter(dm)
	err := writer.Open(sstPath)
	require.NoError(t, err)

	err = writer.AppendPut([]byte("key1"), []byte(""))

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
	tempDir, dm := setup(t)
	defer os.RemoveAll(tempDir)

	sstPath := filepath.Join(tempDir, "large_data.sst")

	// Create SSTable with large values
	writer := NewSSTWriter(dm)
	err := writer.Open(sstPath)
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

	err = writer.AppendPut(largeKey, largeValue)
	require.NoError(t, err)

	// Add a normal key after the large one
	err = writer.AppendPut([]byte("small-key"), []byte("small-value"))

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

	for b.Loop() {
		b.StopTimer()
		sstPath := filepath.Join(tempDir, "bench_write.sst")
		writer := NewSSTWriter(dm)
		writer.Open(sstPath)
		b.StartTimer()

		// Write 1000 entries
		for j := range 1000 {
			key := fmt.Appendf(nil, "key-%d", j)
			value := fmt.Appendf(nil, "value-%d", j)
			err := writer.AppendPut(key, value)

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
	for j := range 1000 {
		key := fmt.Appendf(nil, "key-%d", j)
		value := fmt.Appendf(nil, "value-%d", j)
		writer.AppendPut(key, value)
	}
	writer.Finish()

	// Now benchmark lookups
	reader := NewSSTReader(dm)
	reader.Open(sstPath)

	for i := 0; b.Loop(); i++ {
		// Look up a random key
		keyNum := i % 1000
		key := fmt.Appendf(nil, "key-%d", keyNum)
		value, err := reader.Lookup(key)
		if err != nil {
			b.Fatalf("Failed to lookup key: %v", err)
		}

		expectedValue := fmt.Appendf(nil, "value-%d", keyNum)
		if !bytes.Equal(value, expectedValue) {
			b.Fatalf("Value mismatch: got %s, want %s", string(value), string(expectedValue))
		}
	}
}

func TestNonExistentKeyLookup(t *testing.T) {
	tempDir, dm := setup(t)
	defer os.RemoveAll(tempDir)

	sstPath := filepath.Join(tempDir, "test_missing.sst")

	// Create the SSTable with some entries
	writer := NewSSTWriter(dm)
	writer.Open(sstPath)

	writer.AppendPut([]byte("a"), []byte("apple"))
	writer.AppendPut([]byte("c"), []byte("cherry"))
	writer.AppendPut([]byte("e"), []byte("eheee"))

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
