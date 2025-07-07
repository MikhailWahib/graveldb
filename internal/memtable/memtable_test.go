package memtable_test

import (
	"testing"

	"github.com/MikhailWahib/graveldb/internal/memtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemtable_PutAndGet(t *testing.T) {
	mt := memtable.NewMemtable()

	err := mt.Put("key1", "value1")
	require.NoError(t, err)

	val, ok := mt.Get("key1")
	assert.True(t, ok, "expected key1 to exist")
	assert.Equal(t, "value1", val, "expected value1")
}

func TestMemtable_Delete(t *testing.T) {
	mt := memtable.NewMemtable()

	err := mt.Put("key1", "value1")
	require.NoError(t, err)

	err = mt.Delete("key1")
	require.NoError(t, err)

	val, ok := mt.Get("key1")
	assert.True(t, ok, "expected key1 to exist after deletion")
	assert.Equal(t, memtable.TOMBSTONE, val, "expected TOMBSTONE marker after delete")
}

func TestMemtable_Size(t *testing.T) {
	mt := memtable.NewMemtable()

	require.NoError(t, mt.Put("a", "1"))
	require.NoError(t, mt.Put("b", "2"))
	require.NoError(t, mt.Put("c", "3"))

	assert.Equal(t, 6, mt.Size(), "expected size 6")

	require.NoError(t, mt.Delete("b"))

	// (3x 2bytes) - 1 byte for the deleted value + 9 bytes for TOMBSONE = 14
	assert.Equal(t, 14, mt.Size(), "expected size 3 after logical delete")
}

func TestMemtable_Tombstone(t *testing.T) {
	mt := memtable.NewMemtable()
	err := mt.Put("foo", "bar")
	require.NoError(t, err)

	require.NoError(t, mt.Delete("foo"))

	val, ok := mt.Get("foo")
	assert.True(t, ok, "expected foo to still exist")
	assert.Equal(t, memtable.TOMBSTONE, val, "expected foo value to be TOMBSTONE")
}
