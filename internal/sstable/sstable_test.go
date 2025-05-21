package sstable_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb/internal/diskmanager"
	"github.com/MikhailWahib/graveldb/internal/diskmanager/mockdm"
	"github.com/MikhailWahib/graveldb/internal/shared"
	"github.com/MikhailWahib/graveldb/internal/sstable"
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
	sst := sstable.NewSSTable(dm)
	err := sst.OpenForWrite(sstPath)
	require.NoError(t, err)

	// Write entries
	for _, data := range testData {
		err = sst.AppendPut([]byte(data.key), []byte(data.value))
		require.NoError(t, err)
	}

	// Finish writing
	err = sst.Finish()
	require.NoError(t, err)

	// Read the SSTable
	sst = sstable.NewSSTable(dm)
	err = sst.OpenForRead(sstPath)
	require.NoError(t, err)

	// Test lookup for each key
	for _, data := range testData {
		value, err := sst.Lookup([]byte(data.key))
		assert.NoError(t, err)
		assert.True(t, bytes.Equal(value, []byte(data.value)), "Value mismatch for %s: got %s, want %s", data.key, string(value), data.value)
	}

	// Test lookup for non-existent key
	_, err = sst.Lookup([]byte("nonexistent"))
	assert.Error(t, err)

	sst.Close()
}

func TestSSTableEmptyValue(t *testing.T) {
	tempDir, dm := setup(t)
	defer os.RemoveAll(tempDir)

	sstPath := filepath.Join(tempDir, "empty_value.sst")

	// Create SSTable with empty values
	sst := sstable.NewSSTable(dm)
	err := sst.OpenForWrite(sstPath)
	require.NoError(t, err)

	err = sst.AppendPut([]byte("key1"), []byte(""))
	require.NoError(t, err)

	err = sst.Finish()
	require.NoError(t, err)

	// Read the SSTable
	sst = sstable.NewSSTable(dm)
	err = sst.OpenForRead(sstPath)
	require.NoError(t, err)

	value, err := sst.Lookup([]byte("key1"))
	require.NoError(t, err)

	assert.Empty(t, value)

	sst.Close()
}

func TestSSTableLargeKeyValues(t *testing.T) {
	tempDir, dm := setup(t)
	defer os.RemoveAll(tempDir)

	sstPath := filepath.Join(tempDir, "large_data.sst")

	// Create SSTable with large values
	sst := sstable.NewSSTable(dm)
	err := sst.OpenForWrite(sstPath)
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

	err = sst.AppendPut(largeKey, largeValue)
	require.NoError(t, err)

	// Add a normal key after the large one
	err = sst.AppendPut([]byte("small-key"), []byte("small-value"))
	require.NoError(t, err)

	err = sst.Finish()
	require.NoError(t, err)

	// Read the SSTable
	sst = sstable.NewSSTable(dm)
	err = sst.OpenForRead(sstPath)
	require.NoError(t, err)

	// Check the large key/value
	value, err := sst.Lookup(largeKey)
	require.NoError(t, err)

	assert.True(t, bytes.Equal(value, largeValue), "Large value mismatch: lengths got %d, want %d", len(value), len(largeValue))

	// Check we can still find the small key
	smallValue, err := sst.Lookup([]byte("small-key"))
	require.NoError(t, err)

	assert.Equal(t, "small-value", string(smallValue))

	sst.Close()
}

func BenchmarkSSTableWriting(b *testing.B) {
	tempDir := filepath.Join(os.TempDir(), "sstable_bench_write")
	defer os.RemoveAll(tempDir)

	dm := mockdm.NewMockDiskManager()

	for b.Loop() {
		b.StopTimer()
		sstPath := filepath.Join(tempDir, "bench_write.sst")
		sst := sstable.NewSSTable(dm)
		sst.OpenForWrite(sstPath)
		b.StartTimer()

		// Write 1000 entries
		for j := range 1000 {
			key := fmt.Appendf(nil, "key-%d", j)
			value := fmt.Appendf(nil, "value-%d", j)
			err := sst.AppendPut(key, value)

			if err != nil {
				b.Fatalf("Failed to write entry: %v", err)
			}
		}

		sst.Finish()
	}
}

func BenchmarkSSTableReading(b *testing.B) {
	tempDir := filepath.Join(os.TempDir(), "sstable_bench_read")
	defer os.RemoveAll(tempDir)

	dm := mockdm.NewMockDiskManager()
	sstPath := filepath.Join(tempDir, "bench_read.sst")

	// Create a benchmark SSTable first
	sst := sstable.NewSSTable(dm)
	sst.OpenForWrite(sstPath)

	// Write 1000 entries
	for j := range 1000 {
		key := fmt.Appendf(nil, "key-%d", j)
		value := fmt.Appendf(nil, "value-%d", j)
		sst.AppendPut(key, value)
	}
	sst.Finish()

	// Now benchmark lookups
	sst = sstable.NewSSTable(dm)
	sst.OpenForRead(sstPath)

	for i := 0; b.Loop(); i++ {
		// Look up a random key
		keyNum := i % 1000
		key := fmt.Appendf(nil, "key-%d", keyNum)
		value, err := sst.Lookup(key)
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
	sst := sstable.NewSSTable(dm)
	sst.OpenForWrite(sstPath)

	sst.AppendPut([]byte("a"), []byte("apple"))
	sst.AppendPut([]byte("c"), []byte("cherry"))
	sst.AppendPut([]byte("e"), []byte("eheee"))

	sst.Finish()

	// Read the SSTable
	sst = sstable.NewSSTable(dm)
	sst.OpenForRead(sstPath)

	// Test lookup for keys that don't exist but are within range
	testMissingKeys := []string{"b", "d", "f"}
	for _, key := range testMissingKeys {
		_, err := sst.Lookup([]byte(key))
		assert.Error(t, err, "Expected error for missing key %s, got nil", key)
	}

	// Test keys that are completely out of range
	outOfRangeKeys := []string{"0", "z"}
	for _, key := range outOfRangeKeys {
		_, err := sst.Lookup([]byte(key))
		assert.Error(t, err, "Expected error for out-of-range key %s, got nil", key)
	}

	sst.Close()
}
func TestSSTableIterator(t *testing.T) {
	tempDir, dm := setup(t)
	defer os.RemoveAll(tempDir)

	sstPath := filepath.Join(tempDir, "test_iterator.sst")

	// Create the SSTable with some entries
	sst := sstable.NewSSTable(dm)
	err := sst.OpenForWrite(sstPath)
	require.NoError(t, err)

	// Define test cases
	testCases := []struct {
		key   []byte
		value []byte
		typ   shared.EntryType
	}{
		{[]byte("a"), []byte("apple"), shared.PutEntry},
		{[]byte("b"), nil, shared.DeleteEntry},
		{[]byte("c"), []byte("cherry"), shared.PutEntry},
		{[]byte("d"), []byte("date"), shared.PutEntry},
		{[]byte("e"), nil, shared.DeleteEntry},
	}

	// Write entries
	for _, tc := range testCases {
		if tc.typ == shared.PutEntry {
			sst.AppendPut(tc.key, tc.value)
		} else {
			sst.AppendDelete(tc.key)
		}
	}

	err = sst.Finish()
	require.NoError(t, err)

	// Read the SSTable
	err = sst.OpenForRead(sstPath)
	require.NoError(t, err)

	// Test iteration
	iter, err := sst.NewIterator()
	require.NoError(t, err)
	require.NotNil(t, iter)

	// Check each entry
	for i, tc := range testCases {
		require.True(t, iter.Next(), "Expected entry %d to exist", i)
		assert.Equal(t, tc.key, iter.Key(), "Key mismatch at entry %d", i)
		assert.Equal(t, tc.value, iter.Value(), "Value mismatch at entry %d", i)
		assert.Equal(t, tc.typ, iter.Type(), "Type mismatch at entry %d", i)
	}

	// No more entries
	require.False(t, iter.Next())
	assert.NoError(t, iter.Error())

	sst.Close()
}

func TestSSTableEmptyIterator(t *testing.T) {
	tempDir, dm := setup(t)
	defer os.RemoveAll(tempDir)

	sstPath := filepath.Join(tempDir, "test_empty_iterator.sst")

	// Create an empty SSTable
	sst := sstable.NewSSTable(dm)
	err := sst.OpenForWrite(sstPath)
	require.NoError(t, err)

	err = sst.Finish()
	require.NoError(t, err)

	// Read the SSTable
	err = sst.OpenForRead(sstPath)
	require.NoError(t, err)

	// Test iteration on empty SSTable
	iter, err := sst.NewIterator()
	require.NoError(t, err)
	require.NotNil(t, iter)

	// No entries should be present
	require.False(t, iter.Next())
	assert.NoError(t, iter.Error())

	sst.Close()
}
