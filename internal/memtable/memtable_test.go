package memtable_test

import (
	"testing"

	"github.com/MikhailWahib/graveldb/internal/memtable"
	"github.com/MikhailWahib/graveldb/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemtable_SetAndGet(t *testing.T) {
	mt := memtable.NewMemtable()

	err := mt.Set([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	entry, ok := mt.Get([]byte("key1"))
	assert.True(t, ok, "expected key1 to exist")
	assert.Equal(t, []byte("value1"), entry.Value, "expected value1")
}

func TestMemtable_Delete(t *testing.T) {
	mt := memtable.NewMemtable()

	err := mt.Set([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	err = mt.Delete([]byte("key1"))
	require.NoError(t, err)

	entry, ok := mt.Get([]byte("key1"))
	assert.True(t, ok, "expected key1 to exist after deletion")
	assert.Equal(t, storage.DeleteEntry, entry.Type, "expected type to be DeleteEntry after delete")
	assert.Nil(t, entry.Value, "expected value to be nil after delete")
}

func TestMemtable_Size(t *testing.T) {
	mt := memtable.NewMemtable()

	require.NoError(t, mt.Set([]byte("a"), []byte("1")))
	require.NoError(t, mt.Set([]byte("b"), []byte("2")))
	require.NoError(t, mt.Set([]byte("c"), []byte("3")))

	assert.Equal(t, 6, mt.Size(), "expected size 6")

	require.NoError(t, mt.Delete([]byte("b")))

	assert.Equal(t, 5, mt.Size(), "expected size 5 after logical delete")
}
