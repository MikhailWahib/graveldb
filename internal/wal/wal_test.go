package wal_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb/internal/record"
	"github.com/MikhailWahib/graveldb/internal/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T, path string) string {
	testDir := t.TempDir()
	walPath := filepath.Join(testDir, path)
	return walPath
}

func TestWAL_BasicOperations(t *testing.T) {
	walPath := setup(t, "basic.wal")

	w, err := wal.NewWAL(walPath)
	require.NoError(t, err)

	require.NoError(t, w.AppendPut([]byte("key1"), []byte("value1")))
	require.NoError(t, w.AppendPut([]byte("key2"), []byte("value2")))

	require.NoError(t, w.AppendDelete([]byte("key3")))

	require.NoError(t, w.Close())

	assert.FileExists(t, walPath)
}

func TestWAL_Replay(t *testing.T) {
	walPath := setup(t, "replay.wal")

	w, err := wal.NewWAL(walPath)
	require.NoError(t, err)

	expected := []struct {
		op    string
		key   []byte
		value []byte
	}{
		{"put", []byte("key1"), []byte("value1")},
		{"put", []byte("key2"), []byte("value2")},
		{"delete", []byte("key1"), []byte{}},
		{"put", []byte("key3"), []byte("value3")},
	}

	for _, e := range expected {
		if e.op == "put" {
			require.NoError(t, w.AppendPut(e.key, e.value))
		} else {
			require.NoError(t, w.AppendDelete(e.key))
		}
	}

	require.NoError(t, w.Close())

	// Reopen WAL for replay
	w, err = wal.NewWAL(walPath)
	require.NoError(t, err)

	// Replay and verify entries
	entries, err := w.Replay()
	require.NoError(t, err)

	assert.Len(t, entries, len(expected))

	for i, entry := range entries {
		e := expected[i]
		expectedType := record.PutEntry
		if e.op == "delete" {
			expectedType = record.DeleteEntry
		}

		assert.Equal(t, expectedType, entry.Type, "Entry type mismatch")
		assert.True(t, bytes.Equal(e.key, entry.Key), "Key mismatch")
		assert.True(t, bytes.Equal(e.value, entry.Value), "Value mismatch")
	}

	require.NoError(t, w.Close())
}

func TestWAL_EmptyReplay(t *testing.T) {
	walPath := setup(t, "empty.wal")

	// Create empty WAL
	w, err := wal.NewWAL(walPath)
	require.NoError(t, err)

	// Replay empty WAL
	entries, err := w.Replay()
	require.NoError(t, err)

	assert.Len(t, entries, 0, "Expected empty replay, got entries")
	require.NoError(t, w.Close())
}

func TestWAL_LargeEntries(t *testing.T) {
	walPath := setup(t, "large.wal")

	w, err := wal.NewWAL(walPath)
	require.NoError(t, err)

	// Generate large key and value
	largeKey := make([]byte, 1024)
	largeValue := make([]byte, 4096)

	// Write large entry
	require.NoError(t, w.AppendPut(largeKey, largeValue))
	// Write normal entry
	require.NoError(t, w.AppendPut([]byte("small_key"), []byte("small_value")))

	require.NoError(t, w.Close())

	// Reopen and replay
	w, err = wal.NewWAL(walPath)
	require.NoError(t, err)

	entries, err := w.Replay()
	require.NoError(t, err)

	assert.Len(t, entries, 2)

	// Verify large entry
	assert.True(t, bytes.Equal(largeKey, entries[0].Key), "Large key mismatch")
	assert.True(t, bytes.Equal(largeValue, entries[0].Value), "Large value mismatch")

	// Verify small entry
	assert.True(t, bytes.Equal([]byte("small_key"), entries[1].Key), "Small key mismatch")
	assert.True(t, bytes.Equal([]byte("small_value"), entries[1].Value), "Small value mismatch")

	require.NoError(t, w.Close())
}

func TestWAL_Reopening(t *testing.T) {
	walPath := setup(t, "reopen.wal")

	// Create WAL and add entries
	w, err := wal.NewWAL(walPath)
	require.NoError(t, err)

	require.NoError(t, w.AppendPut([]byte("key1"), []byte("value1")))
	require.NoError(t, w.Close())

	// Reopen and add more entries
	w, err = wal.NewWAL(walPath)
	require.NoError(t, err)

	require.NoError(t, w.AppendPut([]byte("key2"), []byte("value2")))
	require.NoError(t, w.Close())

	// Open and replay
	w, err = wal.NewWAL(walPath)
	require.NoError(t, err)

	entries, err := w.Replay()
	require.NoError(t, err)

	assert.Len(t, entries, 2)
	assert.True(t, bytes.Equal([]byte("key1"), entries[0].Key))
	assert.True(t, bytes.Equal([]byte("value1"), entries[0].Value))
	assert.True(t, bytes.Equal([]byte("key2"), entries[1].Key))
	assert.True(t, bytes.Equal([]byte("value2"), entries[1].Value))

	require.NoError(t, w.Close())
}

func TestWAL_InvalidPath(t *testing.T) {
	// Try to create WAL in non-existent directory
	_, err := wal.NewWAL("/nonexistent/directory/test.wal")
	assert.Error(t, err, "Expected error with invalid path, got nil")
}
