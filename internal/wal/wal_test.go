package wal_test

import (
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb/internal/diskmanager"
	"github.com/MikhailWahib/graveldb/internal/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T, path string) (string, diskmanager.DiskManager) {
	testDir := t.TempDir()
	walPath := filepath.Join(testDir, path)
	dm := diskmanager.NewDiskManager()
	return walPath, dm

}

func TestWAL_BasicOperations(t *testing.T) {
	walPath, dm := setup(t, "basic.wal")

	w, err := wal.NewWAL(dm, walPath)
	require.NoError(t, err)

	require.NoError(t, w.AppendPut("key1", "value1"))
	require.NoError(t, w.AppendPut("key2", "value2"))

	require.NoError(t, w.AppendDelete("key3"))

	require.NoError(t, w.Sync())

	require.NoError(t, w.Close())

	assert.FileExists(t, walPath)
}

func TestWAL_Replay(t *testing.T) {
	walPath, dm := setup(t, "replay.wal")

	w, err := wal.NewWAL(dm, walPath)
	require.NoError(t, err)

	expected := []struct {
		op    string
		key   string
		value string
	}{
		{"put", "key1", "value1"},
		{"put", "key2", "value2"},
		{"delete", "key1", ""},
		{"put", "key3", "value3"},
	}

	for _, e := range expected {
		if e.op == "put" {
			require.NoError(t, w.AppendPut(e.key, e.value))
		} else {
			require.NoError(t, w.AppendDelete(e.key))
		}
	}

	// Sync and close
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	// Reopen WAL for replay
	w, err = wal.NewWAL(dm, walPath)
	require.NoError(t, err)

	// Replay and verify entries
	entries, err := w.Replay()
	require.NoError(t, err)

	assert.Len(t, entries, len(expected))

	for i, entry := range entries {
		e := expected[i]
		expectedType := wal.PutEntry
		if e.op == "delete" {
			expectedType = wal.DeleteEntry
		}

		assert.Equal(t, expectedType, entry.Type, "Entry type mismatch")
		assert.Equal(t, e.key, entry.Key, "Key mismatch")
		assert.Equal(t, e.value, entry.Value, "Value mismatch")
	}

	require.NoError(t, w.Close())
}

func TestWAL_EmptyReplay(t *testing.T) {
	walPath, dm := setup(t, "empty.wal")

	// Create empty WAL
	w, err := wal.NewWAL(dm, walPath)
	require.NoError(t, err)

	// Replay empty WAL
	entries, err := w.Replay()
	require.NoError(t, err)

	assert.Len(t, entries, 0, "Expected empty replay, got entries")
	require.NoError(t, w.Close())
}

func TestWAL_LargeEntries(t *testing.T) {
	walPath, dm := setup(t, "large.wal")

	w, err := wal.NewWAL(dm, walPath)
	require.NoError(t, err)

	// Generate large key and value
	largeKey := make([]byte, 1024)
	largeValue := make([]byte, 4096)

	// Write large entry
	require.NoError(t, w.AppendPut(string(largeKey), string(largeValue)))
	// Write normal entry
	require.NoError(t, w.AppendPut("small_key", "small_value"))

	require.NoError(t, w.Close())

	// Reopen and replay
	w, err = wal.NewWAL(dm, walPath)
	require.NoError(t, err)

	entries, err := w.Replay()
	require.NoError(t, err)

	assert.Len(t, entries, 2)

	// Verify large entry
	assert.Equal(t, string(largeKey), entries[0].Key, "Large key mismatch")
	assert.Equal(t, string(largeValue), entries[0].Value, "Large value mismatch")

	// Verify small entry
	assert.Equal(t, "small_key", entries[1].Key, "Small key mismatch")
	assert.Equal(t, "small_value", entries[1].Value, "Small value mismatch")

	require.NoError(t, w.Close())
}

func TestWAL_Reopening(t *testing.T) {
	walPath, dm := setup(t, "reopen.wal")

	// Create WAL and add entries
	w, err := wal.NewWAL(dm, walPath)
	require.NoError(t, err)

	require.NoError(t, w.AppendPut("key1", "value1"))
	require.NoError(t, w.Close())

	// Reopen and add more entries
	w, err = wal.NewWAL(dm, walPath)
	require.NoError(t, err)

	require.NoError(t, w.AppendPut("key2", "value2"))
	require.NoError(t, w.Close())

	// Open and replay
	w, err = wal.NewWAL(dm, walPath)
	require.NoError(t, err)

	entries, err := w.Replay()
	require.NoError(t, err)

	assert.Len(t, entries, 2)
	assert.Equal(t, "key1", entries[0].Key)
	assert.Equal(t, "value1", entries[0].Value)
	assert.Equal(t, "key2", entries[1].Key)
	assert.Equal(t, "value2", entries[1].Value)

	require.NoError(t, w.Close())
}

func TestWAL_InvalidPath(t *testing.T) {
	dm := diskmanager.NewDiskManager()

	// Try to create WAL in non-existent directory
	_, err := wal.NewWAL(dm, "/nonexistent/directory/test.wal")
	assert.Error(t, err, "Expected error with invalid path, got nil")
}
