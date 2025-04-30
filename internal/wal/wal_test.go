package wal_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb/internal/diskmanager"
	"github.com/MikhailWahib/graveldb/internal/wal"
)

func TestWAL_BasicOperations(t *testing.T) {
	// Setup
	testDir := t.TempDir()
	walPath := filepath.Join(testDir, "test.wal")
	dm := diskmanager.NewDiskManager()

	// Create new WAL
	w, err := wal.NewWAL(dm, walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Test AppendPut
	err = w.AppendPut("key1", "value1")
	if err != nil {
		t.Fatalf("AppendPut failed: %v", err)
	}

	err = w.AppendPut("key2", "value2")
	if err != nil {
		t.Fatalf("AppendPut failed: %v", err)
	}

	// Test AppendDelete
	err = w.AppendDelete("key3")
	if err != nil {
		t.Fatalf("AppendDelete failed: %v", err)
	}

	// Test Sync
	err = w.Sync()
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Test Close
	err = w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify file exists
	_, err = os.Stat(walPath)
	if err != nil {
		t.Fatalf("WAL file does not exist after operations: %v", err)
	}
}

func TestWAL_Replay(t *testing.T) {
	// Setup
	testDir := t.TempDir()
	walPath := filepath.Join(testDir, "replay.wal")
	dm := diskmanager.NewDiskManager()

	// Create and populate WAL
	w, err := wal.NewWAL(dm, walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

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
			err = w.AppendPut(e.key, e.value)
		} else {
			err = w.AppendDelete(e.key)
		}
		if err != nil {
			t.Fatalf("Failed to append entry: %v", err)
		}
	}

	// Sync and close
	err = w.Sync()
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen WAL for replay
	w, err = wal.NewWAL(dm, walPath)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}

	// Replay and verify entries
	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if len(entries) != len(expected) {
		t.Fatalf("Expected %d entries, got %d", len(expected), len(entries))
	}

	for i, entry := range entries {
		e := expected[i]

		var expectedType wal.EntryType
		if e.op == "put" {
			expectedType = wal.PutEntry
		} else {
			expectedType = wal.DeleteEntry
		}

		if entry.Type != expectedType {
			t.Errorf("Entry %d: expected type %v, got %v", i, expectedType, entry.Type)
		}

		if entry.Key != e.key {
			t.Errorf("Entry %d: expected key %q, got %q", i, e.key, entry.Key)
		}

		if entry.Value != e.value {
			t.Errorf("Entry %d: expected value %q, got %q", i, e.value, entry.Value)
		}
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Close after replay failed: %v", err)
	}
}

func TestWAL_EmptyReplay(t *testing.T) {
	// Setup
	testDir := t.TempDir()
	walPath := filepath.Join(testDir, "empty.wal")
	dm := diskmanager.NewDiskManager()

	// Create empty WAL
	w, err := wal.NewWAL(dm, walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Replay empty WAL
	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if len(entries) != 0 {
		t.Fatalf("Expected empty replay, got %d entries", len(entries))
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWAL_LargeEntries(t *testing.T) {
	// Setup
	testDir := t.TempDir()
	walPath := filepath.Join(testDir, "large.wal")
	dm := diskmanager.NewDiskManager()

	// Create WAL
	w, err := wal.NewWAL(dm, walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Generate large key and value
	largeKey := make([]byte, 1024)
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}

	largeValue := make([]byte, 4096)
	for i := range largeValue {
		largeValue[i] = byte((i * 7) % 256)
	}

	// Write large entry
	err = w.AppendPut(string(largeKey), string(largeValue))
	if err != nil {
		t.Fatalf("Failed to append large entry: %v", err)
	}

	// Write normal entry
	err = w.AppendPut("small_key", "small_value")
	if err != nil {
		t.Fatalf("Failed to append small entry: %v", err)
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and replay
	w, err = wal.NewWAL(dm, walPath)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}

	// Verify large entry
	if entries[0].Key != string(largeKey) {
		t.Errorf("Large key mismatch")
	}

	if entries[0].Value != string(largeValue) {
		t.Errorf("Large value mismatch")
	}

	// Verify small entry
	if entries[1].Key != "small_key" {
		t.Errorf("Small key mismatch: expected 'small_key', got %q", entries[1].Key)
	}

	if entries[1].Value != "small_value" {
		t.Errorf("Small value mismatch: expected 'small_value', got %q", entries[1].Value)
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWAL_Reopening(t *testing.T) {
	// Setup
	testDir := t.TempDir()
	walPath := filepath.Join(testDir, "reopen.wal")
	dm := diskmanager.NewDiskManager()

	// Create WAL and add entries
	w, err := wal.NewWAL(dm, walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	err = w.AppendPut("key1", "value1")
	if err != nil {
		t.Fatalf("AppendPut failed: %v", err)
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and add more entries
	w, err = wal.NewWAL(dm, walPath)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}

	err = w.AppendPut("key2", "value2")
	if err != nil {
		t.Fatalf("AppendPut failed: %v", err)
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Open and replay
	w, err = wal.NewWAL(dm, walPath)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}

	if entries[0].Key != "key1" || entries[0].Value != "value1" {
		t.Errorf("First entry mismatch: expected key1/value1, got %s/%s", entries[0].Key, entries[0].Value)
	}

	if entries[1].Key != "key2" || entries[1].Value != "value2" {
		t.Errorf("Second entry mismatch: expected key2/value2, got %s/%s", entries[1].Key, entries[1].Value)
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWAL_InvalidPath(t *testing.T) {
	dm := diskmanager.NewDiskManager()

	// Try to create WAL in non-existent directory
	_, err := wal.NewWAL(dm, "/nonexistent/directory/test.wal")
	if err == nil {
		t.Fatalf("Expected error with invalid path, got nil")
	}
}
