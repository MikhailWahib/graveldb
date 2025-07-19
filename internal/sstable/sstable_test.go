package sstable_test

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb/internal/record"
	"github.com/MikhailWahib/graveldb/internal/sstable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSTableWriteRead(t *testing.T) {
	testData := []struct {
		key   string
		value string
	}{
		{"apple", "red"},
		{"banana", "yellow"},
		{"cherry", "dark red"},
		{"date", "brown"},
	}

	// Create temporary directory
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "01.sst")

	// Create and write to SSTable
	sst, err := sstable.NewWriter(sstPath)
	require.NoError(t, err)

	// Write entries
	for _, data := range testData {
		err = sst.PutEntry([]byte(data.key), []byte(data.value))
		require.NoError(t, err)
	}

	// Finish writing
	err = sst.Finish()
	require.NoError(t, err)

	err = sst.Close()
	require.NoError(t, err)

	// Read the SSTable
	sstReader, err := sstable.NewReader(sstPath)
	require.NoError(t, err)

	// Test lookup for each key
	for _, data := range testData {
		entry, err := sstReader.Get([]byte(data.key))
		assert.NoError(t, err)
		assert.True(t, bytes.Equal(entry.Value, []byte(data.value)), "Value mismatch for %s: got %s, want %s", data.key, string(entry.Value), data.value)
	}

	// Test lookup for non-existent key
	_, err = sstReader.Get([]byte("nonexistent"))
	assert.Error(t, err)

	require.NoError(t, sstReader.Close())
}

func TestSSTableEmptyValue(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "empty_value.sst")

	// Create SSTable with empty values
	sst, err := sstable.NewWriter(sstPath)
	require.NoError(t, err)

	err = sst.PutEntry([]byte("key1"), []byte(""))
	require.NoError(t, err)

	err = sst.Finish()
	require.NoError(t, err)

	err = sst.Close()
	require.NoError(t, err)

	// Read the SSTable
	sstReader, err := sstable.NewReader(sstPath)
	require.NoError(t, err)

	entry, err := sstReader.Get([]byte("key1"))
	require.NoError(t, err)

	assert.Empty(t, entry.Value)

	require.NoError(t, sstReader.Close())
}

func TestSSTableLargeKeyValues(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "large_data.sst")

	// Create SSTable with large values
	sst, err := sstable.NewWriter(sstPath)
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

	err = sst.PutEntry(largeKey, largeValue)
	require.NoError(t, err)

	// Add a normal key after the large one
	err = sst.PutEntry([]byte("small-key"), []byte("small-value"))
	require.NoError(t, err)

	err = sst.Finish()
	require.NoError(t, err)

	err = sst.Close()
	require.NoError(t, err)

	// Read the SSTable
	sstReader, err := sstable.NewReader(sstPath)
	require.NoError(t, err)

	// Check the large key/value
	entry, err := sstReader.Get(largeKey)
	require.NoError(t, err)

	assert.True(t, bytes.Equal(entry.Value, largeValue), "Large value mismatch: lengths got %d, want %d", len(entry.Value), len(largeValue))

	// Check we can still find the small key
	smallValueEntry, err := sstReader.Get([]byte("small-key"))
	require.NoError(t, err)

	assert.Equal(t, "small-value", string(smallValueEntry.Value))

	require.NoError(t, sstReader.Close())
}

func BenchmarkSSTableWriting(b *testing.B) {
	for b.Loop() {
		b.StopTimer()
		tempDir := b.TempDir()
		sstPath := filepath.Join(tempDir, "bench_write.sst")
		sst, err := sstable.NewWriter(sstPath)
		if err != nil {
			b.Fatalf("Failed to open SSTable for write: %v", err)
		}
		b.StartTimer()

		// Write 1000 entries
		for j := range 1000 {
			key := fmt.Appendf(nil, "key-%d", j)
			value := fmt.Appendf(nil, "value-%d", j)
			err := sst.PutEntry(key, value)

			if err != nil {
				b.Fatalf("Failed to write entry: %v", err)
			}
		}

		err = sst.Finish()
		if err != nil {
			b.Fatalf("Failed to finish SSTable: %v", err)
		}

		err = sst.Close()
		if err != nil {
			b.Fatalf("Failed to close SSTable: %v", err)
		}
	}
}

func BenchmarkSSTableReading(b *testing.B) {
	tempDir := b.TempDir()
	sstPath := filepath.Join(tempDir, "bench_read.sst")

	// Create a benchmark SSTable first
	sst, err := sstable.NewWriter(sstPath)
	if err != nil {
		b.Fatalf("Failed to open SSTable for write: %v", err)
	}

	// Write 1000 entries
	for j := range 1000 {
		key := fmt.Appendf(nil, "key-%d", j)
		value := fmt.Appendf(nil, "value-%d", j)
		err := sst.PutEntry(key, value)
		if err != nil {
			b.Fatalf("Failed to write entry: %v", err)
		}
	}
	err = sst.Finish()
	if err != nil {
		b.Fatalf("Failed to finish SSTable: %v", err)
	}
	err = sst.Close()
	if err != nil {
		b.Fatalf("Failed to close SSTable: %v", err)
	}

	// Now benchmark lookups
	sstReader, err := sstable.NewReader(sstPath)
	if err != nil {
		b.Fatalf("Failed to open SSTable for read: %v", err)
	}

	for i := 0; b.Loop(); i++ {
		// Look up a random key
		keyNum := i % 1000
		key := fmt.Appendf(nil, "key-%d", keyNum)
		entry, err := sstReader.Get(key)
		if err != nil {
			b.Fatalf("Failed to lookup key: %v", err)
		}

		expectedValue := fmt.Appendf(nil, "value-%d", keyNum)
		if !bytes.Equal(entry.Value, expectedValue) {
			b.Fatalf("Value mismatch: got %s, want %s", string(entry.Value), string(expectedValue))
		}
	}
}

func TestNonExistentKeyLookup(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "test_missing.sst")

	// Create the SSTable with some entries
	sst, err := sstable.NewWriter(sstPath)
	require.NoError(t, err)

	err = sst.PutEntry([]byte("a"), []byte("apple"))
	require.NoError(t, err)
	err = sst.PutEntry([]byte("c"), []byte("cherry"))
	require.NoError(t, err)
	err = sst.PutEntry([]byte("e"), []byte("eheee"))
	require.NoError(t, err)

	err = sst.Finish()
	require.NoError(t, err)

	err = sst.Close()
	require.NoError(t, err)

	// Read the SSTable
	sstReader, err := sstable.NewReader(sstPath)
	require.NoError(t, err)

	// Test lookup for keys that don't exist but are within range
	testMissingKeys := []string{"b", "d", "f"}
	for _, key := range testMissingKeys {
		_, err := sstReader.Get([]byte(key))
		assert.Error(t, err, "Expected error for missing key %s, got nil", key)
	}

	// Test keys that are completely out of range
	outOfRangeKeys := []string{"0", "z"}
	for _, key := range outOfRangeKeys {
		_, err := sstReader.Get([]byte(key))
		assert.Error(t, err, "Expected error for out-of-range key %s, got nil", key)
	}

	require.NoError(t, sstReader.Close())
}

func Test_DeleteSST(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "delete-test.sst")

	sst, err := sstable.NewWriter(sstPath)
	require.NoError(t, err)

	err = sst.PutEntry([]byte("key"), []byte("value"))
	require.NoError(t, err)

	err = sst.Finish()
	require.NoError(t, err)

	// Delete the table
	err = sst.Delete()
	require.NoError(t, err)

	// Attempt to read the SSTable
	_, err = sstable.NewReader(sstPath)
	require.Error(t, err)
}

func TestSSTableIterator(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "test_iterator.sst")

	// Create the SSTable with some entries
	sst, err := sstable.NewWriter(sstPath)
	require.NoError(t, err)

	// Define test cases
	testCases := []struct {
		key   []byte
		value []byte
		typ   record.EntryType
	}{
		{[]byte("a"), []byte("apple"), record.PutEntry},
		{[]byte("b"), nil, record.DeleteEntry},
		{[]byte("c"), []byte("cherry"), record.PutEntry},
		{[]byte("d"), []byte("date"), record.PutEntry},
		{[]byte("e"), nil, record.DeleteEntry},
	}

	// Write entries
	for _, tc := range testCases {
		if tc.typ == record.PutEntry {
			err = sst.PutEntry(tc.key, tc.value)
			require.NoError(t, err)
		} else {
			err = sst.DeleteEntry(tc.key)
			require.NoError(t, err)
		}
	}

	err = sst.Finish()
	require.NoError(t, err)

	err = sst.Close()
	require.NoError(t, err)

	// Read the SSTable
	sstReader, err := sstable.NewReader(sstPath)
	require.NoError(t, err)

	// Test iteration
	iter := sstReader.NewIterator()

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

	require.NoError(t, sstReader.Close())
}

func TestSSTableEmptyIterator(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "test_empty_iterator.sst")

	// Create an empty SSTable
	sst, err := sstable.NewWriter(sstPath)
	require.NoError(t, err)

	err = sst.Finish()
	require.NoError(t, err)

	err = sst.Close()
	require.NoError(t, err)

	// Read the SSTable
	sstReader, err := sstable.NewReader(sstPath)
	require.NoError(t, err)

	// Test iteration on empty SSTable
	iter := sstReader.NewIterator()

	// No entries should be present
	require.False(t, iter.Next())
	assert.NoError(t, iter.Error())

	require.NoError(t, sstReader.Close())
}

func createSST(t *testing.T, path string, entries []entry) *sstable.Reader {
	sst, err := sstable.NewWriter(path)
	require.NoError(t, err)

	for _, e := range entries {
		if e.typ == record.PutEntry {
			require.NoError(t, sst.PutEntry([]byte(e.key), []byte(e.value)))
		} else {
			require.NoError(t, sst.DeleteEntry([]byte(e.key)))
		}
	}
	require.NoError(t, sst.Finish())
	require.NoError(t, sst.Close())

	// Reopen for reading
	sstReader, err := sstable.NewReader(path)
	require.NoError(t, err)
	return sstReader
}

type entry struct {
	key   string
	value string
	typ   record.EntryType
}

func TestMerger_MergesCorrectly(t *testing.T) {
	tempDir := t.TempDir()

	// Old SSTable: a=1, b=2, c=3
	oldSSTPath := filepath.Join(tempDir, "old.sst")
	oldEntries := []entry{
		{"a", "1", record.PutEntry},
		{"b", "2", record.PutEntry},
		{"c", "3", record.PutEntry},
	}
	oldSST := createSST(t, oldSSTPath, oldEntries)

	// New SSTable: b=22 (overwrite), c=delete, d=4
	newSSTPath := filepath.Join(tempDir, "new.sst")
	newEntries := []entry{
		{"b", "22", record.PutEntry},
		{"c", "", record.DeleteEntry},
		{"d", "4", record.PutEntry},
	}
	newSST := createSST(t, newSSTPath, newEntries)

	// Setup output SSTable
	outputPath := filepath.Join(tempDir, "merged.sst")
	outputSST, err := sstable.NewWriter(outputPath)
	require.NoError(t, err)

	// Merge
	merger := sstable.NewMerger()
	require.NoError(t, merger.AddSource(oldSST)) // Older goes first
	require.NoError(t, merger.AddSource(newSST)) // Newer overrides
	merger.SetOutput(outputSST)

	require.NoError(t, merger.Merge())

	// Close and reopen for reading
	require.NoError(t, outputSST.Close())
	outputSSTReader, err := sstable.NewReader(outputPath)
	require.NoError(t, err)

	iter := outputSSTReader.NewIterator()

	expected := []entry{
		{"a", "1", record.PutEntry},
		{"b", "22", record.PutEntry},  // newer value
		{"c", "", record.DeleteEntry}, // deleted
		{"d", "4", record.PutEntry},
	}

	i := 0
	for iter.Next() {
		require.Less(t, i, len(expected), "Too many entries")
		exp := expected[i]
		assert.Equal(t, exp.key, string(iter.Key()), "key mismatch at index %d", i)
		assert.Equal(t, exp.typ, iter.Type(), "entry type mismatch")

		if exp.typ == record.PutEntry {
			assert.Equal(t, exp.value, string(iter.Value()), "value mismatch")
		} else {
			assert.True(t, iter.IsDeleted(), "should be deleted")
			assert.Nil(t, iter.Value())
		}
		i++
	}

	assert.Equal(t, len(expected), i, "entry count mismatch")
	assert.NoError(t, iter.Error())
	require.NoError(t, outputSSTReader.Close())
}

func TestMerger_MultipleSSTablesMerge(t *testing.T) {
	tempDir := t.TempDir()

	// SST1: a=1, b=2, c=3
	sst1Path := filepath.Join(tempDir, "sst1.sst")
	sst1 := createSST(t, sst1Path, []entry{
		{"a", "1", record.PutEntry},
		{"b", "2", record.PutEntry},
		{"c", "3", record.PutEntry},
	})

	// SST2: b=22, d=4
	sst2Path := filepath.Join(tempDir, "sst2.sst")
	sst2 := createSST(t, sst2Path, []entry{
		{"b", "22", record.PutEntry}, // overwrites b from sst1
		{"d", "4", record.PutEntry},
	})

	// SST3: c=delete, e=5
	sst3Path := filepath.Join(tempDir, "sst3.sst")
	sst3 := createSST(t, sst3Path, []entry{
		{"c", "", record.DeleteEntry}, // deletes c from sst1
		{"e", "5", record.PutEntry},
	})

	sst4Path := filepath.Join(tempDir, "sst4.sst")
	sst4 := createSST(t, sst4Path, []entry{
		{"f", "fifi", record.PutEntry},
		{"g", "gigi", record.PutEntry},
	})

	// Output SSTable
	mergedPath := filepath.Join(tempDir, "merged_multi.sst")
	output, err := sstable.NewWriter(mergedPath)
	require.NoError(t, err)

	// Merge
	merger := sstable.NewMerger()
	require.NoError(t, merger.AddSource(sst1)) // oldest
	require.NoError(t, merger.AddSource(sst2))
	require.NoError(t, merger.AddSource(sst3)) // newest
	require.NoError(t, merger.AddSource(sst4)) // newest
	merger.SetOutput(output)
	require.NoError(t, merger.Merge())

	// Close and reopen for reading
	require.NoError(t, output.Close())
	outputReader, err := sstable.NewReader(mergedPath)
	require.NoError(t, err)

	iter := outputReader.NewIterator()

	// Expected after merge
	expected := []entry{
		{"a", "1", record.PutEntry},
		{"b", "22", record.PutEntry},  // from sst2
		{"c", "", record.DeleteEntry}, // deleted in sst3
		{"d", "4", record.PutEntry},   // from sst2
		{"e", "5", record.PutEntry},   // from sst3
		{"f", "fifi", record.PutEntry},
		{"g", "gigi", record.PutEntry},
	}

	i := 0
	for iter.Next() {
		require.Less(t, i, len(expected), "Too many entries")
		exp := expected[i]
		assert.Equal(t, exp.key, string(iter.Key()), "Key mismatch at index %d", i)
		assert.Equal(t, exp.typ, iter.Type(), "Type mismatch at index %d", i)
		if exp.typ == record.PutEntry {
			assert.Equal(t, exp.value, string(iter.Value()), "Value mismatch")
		} else {
			assert.True(t, iter.IsDeleted())
			assert.Nil(t, iter.Value())
		}
		i++
	}

	assert.Equal(t, len(expected), i, "Entry count mismatch")
	assert.NoError(t, iter.Error())
	require.NoError(t, outputReader.Close())
}
