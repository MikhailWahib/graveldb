package engine_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/MikhailWahib/graveldb/internal/engine"
	"github.com/MikhailWahib/graveldb/internal/sstable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngine_BasicPutGetDelete(t *testing.T) {
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
	fakeSST := filepath.Join(levelDir, "00000001.sst")
	err = os.WriteFile(fakeSST, []byte("placeholder"), 0644)
	require.NoError(t, err)

	db, err := engine.NewEngine(tmpDir)
	require.NoError(t, err)

	err = db.OpenDB()
	require.NoError(t, err)

	// Expect the level to be parsed
	require.True(t, len(db.Tiers()) > 0)
	require.True(t, len(db.Tiers()[0]) == 1)
}

func TestMemtableFlush(t *testing.T) {
	tmpDir := t.TempDir()

	// Lower threshold for easier testing
	engine.MAX_MEMTABLE_SIZE = 1 // force flush on first insert

	e, err := engine.NewEngine(tmpDir)
	require.NoError(t, err)

	// Insert one key to trigger flush
	err = e.Put("key1", "value1")
	require.NoError(t, err)

	// Wait a bit to ensure background flush finishes
	time.Sleep(100 * time.Millisecond)

	// Check SSTable file exists
	sstPath := filepath.Join(tmpDir, "sstables", "L0", "000001.sst")
	_, err = os.Stat(sstPath)
	require.NoError(t, err, "Expected SSTable file to exist")

	// Try to open it and read contents
	sst := sstable.NewSSTable(sstPath)
	err = sst.OpenForRead()
	require.NoError(t, err)

	val, err := sst.Lookup([]byte("key1"))
	require.NoError(t, err, "Expected key1 to be found in flushed SSTable with no errors")
	require.Equal(t, []byte("value1"), val)
}

func TestEngine_GetFromSSTable(t *testing.T) {
	tmpDir := t.TempDir()

	// Force flush immediately
	engine.MAX_MEMTABLE_SIZE = 1

	db, err := engine.NewEngine(tmpDir)
	require.NoError(t, err)

	// Put key to trigger flush
	err = db.Put("flushed_key", "flushed_value")
	require.NoError(t, err)

	// Wait for flush to complete
	time.Sleep(100 * time.Millisecond)

	engine.MAX_MEMTABLE_SIZE = 100

	// Put another key in memtable
	err = db.Put("memtable_key", "memtable_value")
	require.NoError(t, err)

	// Should find both keys
	val, found := db.Get("flushed_key")
	assert.True(t, found, "Should find key in SSTable")
	assert.Equal(t, "flushed_value", val)

	val, found = db.Get("memtable_key")
	assert.True(t, found, "Should find key in memtable")
	assert.Equal(t, "memtable_value", val)
}

func TestEngine_GetDeletedFromSSTable(t *testing.T) {
	tmpDir := t.TempDir()

	// Force flush immediately
	engine.MAX_MEMTABLE_SIZE = 1

	db, err := engine.NewEngine(tmpDir)
	require.NoError(t, err)

	// Put and delete key to trigger flush with tombstone
	err = db.Put("deleted_key", "some_value")
	require.NoError(t, err)
	err = db.Delete("deleted_key")
	require.NoError(t, err)

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	// Should not find the deleted key
	val, found := db.Get("deleted_key")
	assert.False(t, found, "Should not find deleted key")
	assert.Equal(t, "", val)
}

func TestEngine_SSTCounterRestoration(t *testing.T) {
	tmpDir := t.TempDir()

	// Create existing SSTable files
	l0Dir := filepath.Join(tmpDir, "sstables", "L0")
	err := os.MkdirAll(l0Dir, 0755)
	require.NoError(t, err)

	// Create files with different numbers
	for _, num := range []string{"000003.sst", "000001.sst", "000005.sst"} {
		err = os.WriteFile(filepath.Join(l0Dir, num), []byte("test"), 0644)
		require.NoError(t, err)
	}

	// Initialize engine
	db, err := engine.NewEngine(tmpDir)
	require.NoError(t, err)

	err = db.OpenDB()
	require.NoError(t, err)

	// Force flush to see next counter value
	engine.MAX_MEMTABLE_SIZE = 1
	err = db.Put("test_key", "test_value")
	require.NoError(t, err)

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	// Should create 000006.sst (next after highest existing 000005.sst)
	newSSTPath := filepath.Join(l0Dir, "000006.sst")
	_, err = os.Stat(newSSTPath)
	require.NoError(t, err, "Expected new SSTable to have counter 000006")
}

func TestEngine_NonExistentKey(t *testing.T) {
	tmpDir := t.TempDir()

	db, err := engine.NewEngine(tmpDir)
	require.NoError(t, err)

	// Try to get non-existent key
	val, found := db.Get("nonexistent")
	assert.False(t, found)
	assert.Equal(t, "", val)

	// Add some data and flush
	engine.MAX_MEMTABLE_SIZE = 1
	err = db.Put("existing", "value")
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Still shouldn't find non-existent key
	val, found = db.Get("nonexistent")
	assert.False(t, found)
	assert.Equal(t, "", val)

	// But should find existing key
	val, found = db.Get("existing")
	assert.True(t, found)
	assert.Equal(t, "value", val)
}
