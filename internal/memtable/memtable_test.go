package memtable_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb/internal/diskmanager/mockdm"
	"github.com/MikhailWahib/graveldb/internal/memtable"
)

func setupTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "memtable-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	return dir
}

func TestMemtable_PutAndGet(t *testing.T) {
	dir := setupTempDir(t)
	defer os.RemoveAll(dir)

	dm := mockdm.NewMockDiskManager()
	mt, err := memtable.NewMemtable(dm, filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("failed to create memtable: %v", err)
	}

	err = mt.Put("key1", "value1")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	val, ok := mt.Get("key1")
	if !ok || val != "value1" {
		t.Errorf("expected value1, got %q", val)
	}
}

func TestMemtable_Delete(t *testing.T) {
	dir := setupTempDir(t)
	defer os.RemoveAll(dir)

	dm := mockdm.NewMockDiskManager()
	mt, err := memtable.NewMemtable(dm, filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("failed to create memtable: %v", err)
	}

	err = mt.Put("key1", "value1")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	err = mt.Delete("key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, ok := mt.Get("key1")
	if ok {
		t.Errorf("expected key1 to be deleted")
	}
}

func TestMemtable_Size(t *testing.T) {
	dir := setupTempDir(t)
	defer os.RemoveAll(dir)

	dm := mockdm.NewMockDiskManager()
	mt, err := memtable.NewMemtable(dm, filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("failed to create memtable: %v", err)
	}

	mt.Put("a", "1")
	mt.Put("b", "2")
	mt.Put("c", "3")

	if got := mt.Size(); got != 3 {
		t.Errorf("expected size 3, got %d", got)
	}

	mt.Delete("b")
	if got := mt.Size(); got != 2 {
		t.Errorf("expected size 2 after delete, got %d", got)
	}
}

func TestMemtable_Replay(t *testing.T) {
	dir := setupTempDir(t)
	defer os.RemoveAll(dir)

	walPath := filepath.Join(dir, "wal.log")
	dm := mockdm.NewMockDiskManager()

	// Initial memtable and writes
	mt1, err := memtable.NewMemtable(dm, walPath)
	if err != nil {
		t.Fatalf("failed to create memtable: %v", err)
	}
	mt1.Put("alpha", "1")
	mt1.Put("beta", "2")
	mt1.Delete("beta")

	// Simulate restart by reloading WAL
	mt2, err := memtable.NewMemtable(dm, walPath)
	if err != nil {
		t.Fatalf("failed to reopen memtable: %v", err)
	}

	val, ok := mt2.Get("alpha")
	if !ok || val != "1" {
		t.Errorf("expected alpha=1, got %q", val)
	}

	if _, ok := mt2.Get("beta"); ok {
		t.Errorf("expected beta to be deleted after replay")
	}
}
