package memtable_test

import (
	"os"
	"testing"

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
	mt := memtable.NewMemtable(dm)

	err := mt.Put("key1", "value1")
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
	mt := memtable.NewMemtable(dm)

	err := mt.Put("key1", "value1")
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
	mt := memtable.NewMemtable(dm)

	err := mt.Put("a", "1")
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

func TestMemtable_Tombstone(t *testing.T) {
	dir := setupTempDir(t)
	defer func() {
		err := os.RemoveAll(dir)
		require.NoError(t, err, "failed to remove temp dir")
	}()

	dm := mockdm.NewMockDiskManager()
	mt := memtable.NewMemtable(dm)

	// Try to delete a non-existent key as if it was flushed to sstable before
	err := mt.Delete("key1")
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
