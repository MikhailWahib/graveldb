package engine_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb/internal/config"
	"github.com/MikhailWahib/graveldb/internal/engine"
	"github.com/MikhailWahib/graveldb/internal/sstable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngine_BasicPutGetDelete(t *testing.T) {
	tmpDir := t.TempDir()

	// Initialize engine
	e := engine.NewEngine(nil)
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	// Insert some keys
	err = e.Put([]byte("foo"), []byte("bar"))
	require.NoError(t, err)
	err = e.Put([]byte("baz"), []byte("qux"))
	require.NoError(t, err)

	// Get keys
	val, found := e.Get([]byte("foo"))
	assert.True(t, found)
	assert.True(t, bytes.Equal([]byte("bar"), val))

	val, found = e.Get([]byte("baz"))
	assert.True(t, found)
	assert.True(t, bytes.Equal([]byte("qux"), val))

	// Delete one
	err = e.Delete([]byte("foo"))
	require.NoError(t, err)

	val, found = e.Get([]byte("foo"))
	assert.False(t, found)
	assert.Nil(t, val)
}

func TestEngine_WALReplay(t *testing.T) {
	tmpDir := t.TempDir()

	func() {
		// Create engine and write entries
		e := engine.NewEngine(nil)
		err := e.OpenDB(tmpDir)
		require.NoError(t, err)

		err = e.Put([]byte("a"), []byte("1"))
		require.NoError(t, err)
		err = e.Put([]byte("b"), []byte("2"))
		require.NoError(t, err)
		err = e.Delete([]byte("a"))
		require.NoError(t, err)

		err = e.Close()
		require.NoError(t, err)
	}()

	// Simulate restart (replay WAL)
	db2 := engine.NewEngine(nil)
	err := db2.OpenDB(tmpDir)
	require.NoError(t, err)

	val, found := db2.Get([]byte("a"))
	assert.False(t, found)
	assert.Nil(t, val)

	val, found = db2.Get([]byte("b"))
	assert.True(t, found)
	assert.True(t, bytes.Equal([]byte("2"), val))
}

func TestEngine_OpenDB_ParseLevels(t *testing.T) {
	tmpDir := t.TempDir()

	// Create dummy level structure
	levelDir := filepath.Join(tmpDir, "sstables", "T0")
	err := os.MkdirAll(levelDir, 0755)
	require.NoError(t, err)

	// Create dummy SST file
	fakeSST := filepath.Join(levelDir, "00000001.sst")
	err = os.WriteFile(fakeSST, []byte("placeholder"), 0644)
	require.NoError(t, err)

	e := engine.NewEngine(nil)
	err = e.OpenDB(tmpDir)
	require.NoError(t, err)

	// Expect the level to be parsed
	require.True(t, len(e.Tiers()) > 0)
	require.True(t, len(e.Tiers()[0]) == 1)
}

func TestMemtableFlush(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine(&config.Config{MaxMemtableSize: 1})
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	// Insert one key to trigger flush
	err = e.Put([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	e.WaitForFlush()

	// Check SSTable file exists
	sstPath := filepath.Join(tmpDir, "sstables", "T0", "000001.sst")
	_, err = os.Stat(sstPath)
	require.NoError(t, err, "Expected SSTable file to exist")

	// Try to open it and read contents
	sst, err := sstable.NewReader(sstPath)
	require.NoError(t, err)

	entry, err := sst.Get([]byte("key1"))
	require.NoError(t, err, "Expected key1 to be found in flushed SSTable with no errors")
	require.True(t, bytes.Equal([]byte("value1"), entry.Value))
}

func TestEngine_GetFromSSTable(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine(nil)
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	// Force flush immediately

	// Put key to trigger flush
	err = e.Put([]byte("flushed_key"), []byte("flushed_value"))
	require.NoError(t, err)

	// Wait for flush to complete
	e.WaitForFlush()

	// Put another key in memtable
	err = e.Put([]byte("memtable_key"), []byte("memtable_value"))
	require.NoError(t, err)

	// Should find both keys
	val, found := e.Get([]byte("flushed_key"))
	assert.True(t, found, "Should find key in SSTable")
	assert.True(t, bytes.Equal([]byte("flushed_value"), val))

	val, found = e.Get([]byte("memtable_key"))
	assert.True(t, found, "Should find key in memtable")
	assert.True(t, bytes.Equal([]byte("memtable_value"), val))
}

func TestEngine_GetDeletedFromSSTable(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine(nil)
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	// Put and delete key to trigger flush with tombstone
	err = e.Put([]byte("deleted_key"), []byte("some_value"))
	require.NoError(t, err)
	err = e.Delete([]byte("deleted_key"))
	require.NoError(t, err)

	e.WaitForFlush()

	// Should not find the deleted key
	val, found := e.Get([]byte("deleted_key"))
	assert.False(t, found, "Should not find deleted key")
	assert.Nil(t, val)
}

func TestEngine_SSTCounterRestoration(t *testing.T) {
	tmpDir := t.TempDir()

	// Create existing SSTable files
	t0Dir := filepath.Join(tmpDir, "sstables", "T0")
	err := os.MkdirAll(t0Dir, 0755)
	require.NoError(t, err)

	// Create files with different numbers
	for _, num := range []string{"000003.sst", "000001.sst", "000005.sst"} {
		err := os.WriteFile(filepath.Join(t0Dir, num), []byte("test"), 0644)
		require.NoError(t, err)
	}

	// Force flush to see next counter value
	e := engine.NewEngine(&config.Config{MaxMemtableSize: 1})
	err = e.OpenDB(tmpDir)
	require.NoError(t, err)

	err = e.Put([]byte("test_key"), []byte("test_value"))
	require.NoError(t, err)

	// Wait for flush
	e.WaitForFlush()

	// Should create 000006.sst (next after highest existing 000005.sst)
	newSSTPath := filepath.Join(t0Dir, "000006.sst")
	_, err = os.Stat(newSSTPath)
	require.NoError(t, err, "Expected new SSTable to have counter 000006")
}

func TestEngine_NonExistentKey(t *testing.T) {
	tmpDir := t.TempDir()
	e := engine.NewEngine(nil)
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	// Try to get non-existent key
	val, found := e.Get([]byte("nonexistent"))
	assert.False(t, found)
	assert.Nil(t, val)

	// Add some data and flush
	err = e.Put([]byte("existing"), []byte("value"))
	require.NoError(t, err)

	e.WaitForFlush()

	// Still shouldn't find non-existent key
	val, found = e.Get([]byte("nonexistent"))
	assert.False(t, found)
	assert.Nil(t, val)

	// But should find existing key
	val, found = e.Get([]byte("existing"))
	assert.True(t, found)
	assert.True(t, bytes.Equal([]byte("value"), val))
}

func Test_ReadLatestFromMultipleSSTsInOneTier(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine(nil)
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	for i := range 2 {
		key := fmt.Appendf(nil, "key%d", i)
		val := fmt.Appendf(nil, "val%d", i)
		require.NoError(t, e.Put(key, val))
		e.WaitForFlush()
	}

	// Update key0
	require.NoError(t, e.Put([]byte("key0"), []byte("new")))
	e.WaitForFlush()

	val, found := e.Get([]byte("key0"))
	require.True(t, found)
	require.Equal(t, "new", string(val))
}

func TestCompaction_TriggersWhenThresholdExceeded(t *testing.T) {
	tmpDir := t.TempDir()
	e := engine.NewEngine(&config.Config{MaxTablesPerTier: 2, MaxMemtableSize: 1})

	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	for i := range 3 {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		require.NoError(t, e.Put([]byte(key), []byte(val)))
	}

	e.WaitForFlush()

	tiers := e.Tiers()
	require.True(t, len(tiers) > 1)
	assert.GreaterOrEqual(t, len(tiers[1]), 1, "Expected compaction output in T1")
}

func TestCompaction_MergedOutputContainsLatestValues(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine(&config.Config{MaxTablesPerTier: 1})
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	// Write initial value
	require.NoError(t, e.Put([]byte("a"), []byte("old")))
	e.WaitForFlush()

	// Overwrite it in a new SST
	require.NoError(t, e.Put([]byte("a"), []byte("new")))
	e.WaitForFlush()

	// Compact should have happened
	val, found := e.Get([]byte("a"))
	assert.True(t, found)
	assert.True(t, bytes.Equal([]byte("new"), val))
}

func TestCompaction_RespectsDeletes(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine(&config.Config{MaxTablesPerTier: 2})
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	require.NoError(t, e.Put([]byte("x"), []byte("1")))
	e.WaitForFlush()

	require.NoError(t, e.Delete([]byte("x")))
	e.WaitForFlush()

	// Should NOT be found after compaction
	val, found := e.Get([]byte("x"))
	assert.False(t, found)
	assert.Nil(t, val)
}

func TestCompaction_DeletesOldSSTables(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine(&config.Config{MaxTablesPerTier: 2, MaxMemtableSize: 1})
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	for i := range 3 {
		require.NoError(t, e.Put(fmt.Appendf(nil, "k%d", i), fmt.Appendf(nil, "v%d", i)))
	}

	e.WaitForFlush()

	tier0 := e.Tiers()[0]
	for _, sst := range tier0 {
		_, err := os.Stat(sst.Path())
		assert.Error(t, err, "Expected SST to be deleted: %s", sst.Path())
	}
}

func TestCompaction_WritesToCorrectTier(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine(&config.Config{MaxTablesPerTier: 2, MaxMemtableSize: 1})
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	for i := range 4 {
		require.NoError(t, e.Put(fmt.Appendf(nil, "k%d", i), fmt.Appendf(nil, "v%d", i)))
		e.WaitForFlush()
	}

	e.WaitForFlush()

	tiers := e.Tiers()
	assert.GreaterOrEqual(t, len(tiers), 2)
	assert.Greater(t, len(tiers[1]), 0)

	for _, sst := range tiers[1] {
		assert.Contains(t, sst.Path(), "T1")
	}
}

func TestCompaction_CreatesValidMergedSSTable(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine(&config.Config{MaxTablesPerTier: 2, MaxMemtableSize: 1})
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	require.NoError(t, e.Put([]byte("z"), []byte("last")))
	e.WaitForFlush()

	require.NoError(t, e.Put([]byte("z"), []byte("latest")))
	e.WaitForFlush()

	require.NoError(t, e.Put([]byte("z"), []byte("last latest")))
	e.WaitForFlush()

	// Validate merged SST file in T1
	tiers := e.Tiers()
	require.Equal(t, len(tiers), 2)
	var merged *sstable.Reader
	for _, s := range tiers[1] {
		if filepath.Base(s.Path()) != "" {
			merged = s
			break
		}
	}
	require.NotNil(t, merged)
	entry, err := merged.Get([]byte("z"))
	require.NoError(t, err)
	assert.True(t, bytes.Equal([]byte("last latest"), entry.Value))
	require.NoError(t, merged.Close())
}

func TestCompaction_PromotesToHigherTiers(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine(&config.Config{MaxTablesPerTier: 1, MaxMemtableSize: 1})
	require.NoError(t, e.OpenDB(tmpDir))

	// Insert enough keys to trigger multi-tier compaction
	for i := range 5 {
		require.NoError(t, e.Put(fmt.Appendf(nil, "k%d", i), fmt.Appendf(nil, "v%d", i)))
		e.WaitForFlush()
	}

	e.WaitForFlush()

	tiers := e.Tiers()
	require.GreaterOrEqual(t, len(tiers), 3, "Expected compaction to reach tier T2")

	// Check key still exists
	val, found := e.Get([]byte("k0"))
	assert.True(t, found)
	assert.True(t, bytes.Equal([]byte("v0"), val))

	// Ensure at least one SSTable exists in T2
	assert.GreaterOrEqual(t, len(tiers[2]), 1, "Expected SSTables in tier T2")
	for _, sst := range tiers[2] {
		assert.Contains(t, sst.Path(), "T2")
	}
}

func TestEngine_WALReplay_MixedTombstones(t *testing.T) {
	tmpDir := t.TempDir()

	func() {
		e := engine.NewEngine(nil)
		require.NoError(t, e.OpenDB(tmpDir))

		require.NoError(t, e.Put([]byte("a"), []byte("1")))
		require.NoError(t, e.Put([]byte("b"), []byte("2")))
		require.NoError(t, e.Delete([]byte("a")))
		require.NoError(t, e.Put([]byte("c"), []byte("3")))
		require.NoError(t, e.Delete([]byte("b")))
		require.NoError(t, e.Close())
	}()

	// Simulate restart
	e := engine.NewEngine(nil)
	require.NoError(t, e.OpenDB(tmpDir))

	val, found := e.Get([]byte("a"))
	assert.False(t, found)
	assert.Nil(t, val)

	val, found = e.Get([]byte("b"))
	assert.False(t, found)
	assert.Nil(t, val)

	val, found = e.Get([]byte("c"))
	assert.True(t, found)
	assert.True(t, bytes.Equal([]byte("3"), val))
}

func TestEngine_Close_WaitsForBackgroundWork(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine(&config.Config{MaxTablesPerTier: 1})
	require.NoError(t, e.OpenDB(tmpDir))

	// Write enough keys to trigger multiple flushes and compactions
	for i := range 5 {
		key := fmt.Appendf(nil, "key%d", i)
		val := fmt.Appendf(nil, "val%d", i)
		require.NoError(t, e.Put(key, val))
		e.WaitForFlush()
	}

	// Call Close and ensure it waits for all background work
	err := e.Close()
	assert.NoError(t, err)

	// Reopen engine and check all data is present
	e2 := engine.NewEngine(nil)
	require.NoError(t, e2.OpenDB(tmpDir))
	for i := range 5 {
		key := fmt.Appendf(nil, "key%d", i)
		val := fmt.Appendf(nil, "val%d", i)
		got, found := e2.Get(key)
		assert.True(t, found, "Should find key after Close and reopen")
		assert.True(t, bytes.Equal(val, got), "Value should match after Close and reopen")
	}
}

func TestEngine_Close_FlushesMemtableEvenIfBelowThreshold(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine(nil)
	require.NoError(t, e.OpenDB(tmpDir))

	key := []byte("unflushed_key")
	val := []byte("unflushed_value")
	require.NoError(t, e.Put(key, val))

	// Do not trigger flush, just close
	err := e.Close()
	assert.NoError(t, err)

	// Reopen engine and check the key is persisted
	e2 := engine.NewEngine(nil)
	require.NoError(t, e2.OpenDB(tmpDir))
	got, found := e2.Get(key)
	assert.True(t, found, "Should find key after Close and reopen, even if memtable was not full")
	assert.True(t, bytes.Equal(val, got), "Value should match after Close and reopen")
}
