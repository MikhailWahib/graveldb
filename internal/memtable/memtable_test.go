package memtable_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb/internal/diskmanager"
	"github.com/MikhailWahib/graveldb/internal/diskmanager/mockdm"
	"github.com/MikhailWahib/graveldb/internal/memtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "memtable-test-*")
	require.NoError(t, err, "failed to create temp dir")
	return dir
}

func TestMemtable_PutAndGet(t *testing.T) {
	dir := setupTempDir(t)
	defer func() {
		err := os.RemoveAll(dir)
		require.NoError(t, err, "failed to remove temp dir")
	}()

	dm := mockdm.NewMockDiskManager()
	mt, err := memtable.NewMemtable(dm, filepath.Join(dir, "wal.log"))
	require.NoError(t, err, "failed to create memtable")

	err = mt.Put("key1", "value1")
	require.NoError(t, err, "Put failed")

	val, ok := mt.Get("key1")
	assert.True(t, ok, "expected key1 to exist")
	assert.Equal(t, "value1", val, "expected value1, got different value")
}

func TestMemtable_Delete(t *testing.T) {
	dir := setupTempDir(t)
	defer func() {
		err := os.RemoveAll(dir)
		require.NoError(t, err, "failed to remove temp dir")
	}()

	dm := mockdm.NewMockDiskManager()
	mt, err := memtable.NewMemtable(dm, filepath.Join(dir, "wal.log"))
	require.NoError(t, err, "failed to create memtable")

	err = mt.Put("key1", "value1")
	require.NoError(t, err, "Put failed")

	err = mt.Delete("key1")
	require.NoError(t, err, "Delete failed")

	_, ok := mt.Get("key1")
	assert.False(t, ok, "expected key1 to be deleted")
}

func TestMemtable_Size(t *testing.T) {
	dir := setupTempDir(t)
	defer func() {
		err := os.RemoveAll(dir)
		require.NoError(t, err, "failed to remove temp dir")
	}()

	dm := mockdm.NewMockDiskManager()
	mt, err := memtable.NewMemtable(dm, filepath.Join(dir, "wal.log"))
	require.NoError(t, err, "failed to create memtable")

	err = mt.Put("a", "1")
	require.NoError(t, err)
	err = mt.Put("b", "2")
	require.NoError(t, err)
	err = mt.Put("c", "3")
	require.NoError(t, err)

	assert.Equal(t, 3, mt.Size(), "expected size 3")

	err = mt.Delete("b")
	require.NoError(t, err)
	assert.Equal(t, 2, mt.Size(), "expected size 2 after delete")
}

func TestMemtable_Replay(t *testing.T) {
	dir := setupTempDir(t)
	defer func() {
		err := os.RemoveAll(dir)
		require.NoError(t, err, "failed to remove temp dir")
	}()

	walPath := filepath.Join(dir, "wal.log")
	dm := diskmanager.NewDiskManager()

	// Initial memtable and writes
	mt1, err := memtable.NewMemtable(dm, walPath)
	require.NoError(t, err, "failed to create memtable")
	err = mt1.Put("alpha", "1")
	require.NoError(t, err)
	err = mt1.Put("beta", "2")
	require.NoError(t, err)
	err = mt1.Delete("beta")
	require.NoError(t, err)

	// Simulate restart by reloading WAL
	mt2, err := memtable.NewMemtable(dm, walPath)
	require.NoError(t, err, "failed to reopen memtable")

	val, ok := mt2.Get("alpha")
	assert.True(t, ok, "expected alpha to exist after replay")
	assert.Equal(t, "1", val, "expected alpha=1, got different value")

	_, ok = mt2.Get("beta")
	assert.False(t, ok, "expected beta to be deleted after replay")
}

func TestMemtable_Tombstone(t *testing.T) {
	dir := setupTempDir(t)
	defer func() {
		err := os.RemoveAll(dir)
		require.NoError(t, err, "failed to remove temp dir")
	}()

	dm := mockdm.NewMockDiskManager()
	mt, err := memtable.NewMemtable(dm, filepath.Join(dir, "wal.log"))
	require.NoError(t, err, "failed to create memtable")

	// Try to delete a non-existent key as if it was flushed to sstable before
	err = mt.Delete("key1")
	require.NoError(t, err, "Delete failed")

	val, ok := mt.Get("key1")
	assert.True(t, ok, "expected key1 to exist after delete")
	assert.Equal(t, memtable.TOMBSTONE, val, "expected TOMBSTONE value")

	// Try to delete the key again
	err = mt.Delete("key1")
	require.NoError(t, err, "Delete failed again")

	val, ok = mt.Get("key1")
	assert.True(t, ok, "expected key1 to not be deleted")
	assert.Equal(t, memtable.TOMBSTONE, val, "expected value remains TOMBSTONE")
}
