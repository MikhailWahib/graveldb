package wal_test

import (
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb/storage/wal"
)

func setupTestWAL(t *testing.T) (wal.WAL, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	w, err := wal.NewWAL(path)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	return w, path
}

func TestWAL_AppendPutAndReplay(t *testing.T) {
	w, path := setupTestWAL(t)
	defer w.Close()

	err := w.AppendPut("key1", "value1")
	if err != nil {
		t.Fatalf("AppendPut failed: %v", err)
	}
	err = w.Sync()
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	w.Close()

	// Reopen and replay
	w2, err := wal.NewWAL(path)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	entries, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	e := entries[0]
	if e.Type != wal.PutEntry || e.Key != "key1" || e.Value != "value1" {
		t.Errorf("unexpected entry: %+v", e)
	}
}

func TestWAL_AppendDeleteAndReplay(t *testing.T) {
	w, path := setupTestWAL(t)
	defer w.Close()

	err := w.AppendDelete("key2")
	if err != nil {
		t.Fatalf("AppendDelete failed: %v", err)
	}
	err = w.Sync()
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	w.Close()

	// Reopen and replay
	w2, err := wal.NewWAL(path)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	entries, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	e := entries[0]
	if e.Type != wal.DeleteEntry || e.Key != "key2" || e.Value != "" {
		t.Errorf("unexpected entry: %+v", e)
	}
}

func TestWAL_MultipleEntries(t *testing.T) {
	w, path := setupTestWAL(t)
	defer w.Close()

	_ = w.AppendPut("a", "1")
	_ = w.AppendPut("b", "2")
	_ = w.AppendDelete("a")
	_ = w.Sync()
	_ = w.Close()

	w2, err := wal.NewWAL(path)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	entries, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	want := []wal.Entry{
		{Type: wal.PutEntry, Key: "a", Value: "1"},
		{Type: wal.PutEntry, Key: "b", Value: "2"},
		{Type: wal.DeleteEntry, Key: "a", Value: ""},
	}

	for i, e := range entries {
		if e != want[i] {
			t.Errorf("entry[%d] mismatch: got %+v, want %+v", i, e, want[i])
		}
	}
}
