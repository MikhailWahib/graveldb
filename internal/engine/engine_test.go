package engine_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngine_PutGetDelete(t *testing.T) {
	tmpDir := t.TempDir()

	// Initialize engine
	db, err := engine.NewEngine(tmpDir)
	require.NoError(t, err)

	// Insert some keys
	err = db.Put("foo", "bar")
	require.NoError(t, err)
	err = db.Put("baz", "qux")
	require.NoError(t, err)

	// Get keys
	val, found := db.Get("foo")
	assert.True(t, found)
	assert.Equal(t, "bar", val)

	val, found = db.Get("baz")
	assert.True(t, found)
	assert.Equal(t, "qux", val)

	// Delete one
	err = db.Delete("foo")
	require.NoError(t, err)

	val, found = db.Get("foo")
	assert.False(t, found)
	assert.Equal(t, "", val)
}

func TestEngine_WALReplay(t *testing.T) {
	tmpDir := t.TempDir()

	func() {
		// Create engine and write entries
		db, err := engine.NewEngine(tmpDir)
		require.NoError(t, err)

		err = db.Put("a", "1")
		require.NoError(t, err)
		err = db.Put("b", "2")
		require.NoError(t, err)
		err = db.Delete("a")
		require.NoError(t, err)
	}()

	// Simulate restart (replay WAL)
	db2, err := engine.NewEngine(tmpDir)
	require.NoError(t, err)

	val, found := db2.Get("a")
	assert.False(t, found)
	assert.Equal(t, "", val)

	val, found = db2.Get("b")
	assert.True(t, found)
	assert.Equal(t, "2", val)
}

func TestEngine_OpenDB_ParseLevels(t *testing.T) {
	tmpDir := t.TempDir()

	// Create dummy level structure
	levelDir := filepath.Join(tmpDir, "sstables", "L0")
	err := os.MkdirAll(levelDir, 0755)
	require.NoError(t, err)

	// Create dummy SST file
	fakeSST := filepath.Join(levelDir, "000001.sst")
	err = os.WriteFile(fakeSST, []byte("placeholder"), 0644)
	require.NoError(t, err)

	db, err := engine.NewEngine(tmpDir)
	require.NoError(t, err)

	err = db.OpenDB()
	require.NoError(t, err)

	// Expect the level to be parsed
	require.True(t, len(db.Levels()) > 0)
	require.True(t, len(db.Levels()[0]) == 1)
}
