package engine_test

import (
	"bytes"
	"fmt"
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
	e := engine.NewEngine()
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
		e := engine.NewEngine()
		err := e.OpenDB(tmpDir)
		require.NoError(t, err)

		err = e.Put([]byte("a"), []byte("1"))
		require.NoError(t, err)
		err = e.Put([]byte("b"), []byte("2"))
		require.NoError(t, err)
		err = e.Delete([]byte("a"))
		require.NoError(t, err)
	}()

	// Simulate restart (replay WAL)
	db2 := engine.NewEngine()
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

	e := engine.NewEngine()
	err = e.OpenDB(tmpDir)
	require.NoError(t, err)

	// Expect the level to be parsed
	require.True(t, len(e.Tiers()) > 0)
	require.True(t, len(e.Tiers()[0]) == 1)
}

func TestMemtableFlush(t *testing.T) {
	tmpDir := t.TempDir()

	// Lower threshold for easier testing

	e := engine.NewEngine()
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	e.SetMaxMemtableSize(1) // force flush on first insert

	// Insert one key to trigger flush
	err = e.Put([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	// Wait a bit to ensure background flush finishes
	time.Sleep(100 * time.Millisecond)

	// Check SSTable file exists
	sstPath := filepath.Join(tmpDir, "sstables", "T0", "000001.sst")
	_, err = os.Stat(sstPath)
	require.NoError(t, err, "Expected SSTable file to exist")

	// Try to open it and read contents
	sst := sstable.NewSSTable(sstPath)
	err = sst.OpenForRead()
	require.NoError(t, err)

	val, err := sst.Lookup([]byte("key1"))
	require.NoError(t, err, "Expected key1 to be found in flushed SSTable with no errors")
	require.True(t, bytes.Equal([]byte("value1"), val))
}

func TestEngine_GetFromSSTable(t *testing.T) {
	tmpDir := t.TempDir()

	// Force flush immediately

	e := engine.NewEngine()
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	e.SetMaxMemtableSize(1)

	// Put key to trigger flush
	err = e.Put([]byte("flushed_key"), []byte("flushed_value"))
	require.NoError(t, err)

	// Wait for flush to complete
	time.Sleep(100 * time.Millisecond)

	e.SetMaxMemtableSize(100)

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

	// Force flush immediately

	e := engine.NewEngine()
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	e.SetMaxMemtableSize(1)

	// Put and delete key to trigger flush with tombstone
	err = e.Put([]byte("deleted_key"), []byte("some_value"))
	require.NoError(t, err)
	err = e.Delete([]byte("deleted_key"))
	require.NoError(t, err)

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

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

	// Initialize engine
	e := engine.NewEngine()
	err = e.OpenDB(tmpDir)
	require.NoError(t, err)

	// Force flush to see next counter value
	e.SetMaxMemtableSize(1)
	err = e.Put([]byte("test_key"), []byte("test_value"))
	require.NoError(t, err)

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	// Should create 000006.sst (next after highest existing 000005.sst)
	newSSTPath := filepath.Join(t0Dir, "000006.sst")
	_, err = os.Stat(newSSTPath)
	require.NoError(t, err, "Expected new SSTable to have counter 000006")
}

func TestEngine_NonExistentKey(t *testing.T) {
	tmpDir := t.TempDir()
	e := engine.NewEngine()
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	// Try to get non-existent key
	val, found := e.Get([]byte("nonexistent"))
	assert.False(t, found)
	assert.Nil(t, val)

	// Add some data and flush
	e.SetMaxMemtableSize(1)
	err = e.Put([]byte("existing"), []byte("value"))
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

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

	e := engine.NewEngine()
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	e.SetMaxMemtableSize(1)

	for i := range 2 {
		key := []byte(fmt.Sprintf("key%d", i))
		val := []byte(fmt.Sprintf("val%d", i))
		require.NoError(t, e.Put(key, val))
	}

	// Update key0
	require.NoError(t, e.Put([]byte("key0"), []byte("new")))

	time.Sleep(300 * time.Millisecond)

	val, found := e.Get([]byte("key0"))
	require.True(t, found)
	require.True(t, bytes.Equal([]byte("new"), val))
}

func TestCompaction_TriggersWhenThresholdExceeded(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine()
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)
	e.SetMaxMemtableSize(1)
	e.SetMaxTablesPerTier(2)

	for i := range 3 {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		require.NoError(t, e.Put([]byte(key), []byte(val)))
	}

	time.Sleep(300 * time.Millisecond)

	tiers := e.Tiers()
	require.True(t, len(tiers) > 1)
	assert.GreaterOrEqual(t, len(tiers[1]), 1, "Expected compaction output in T1")
}

func TestCompaction_MergedOutputContainsLatestValues(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine()
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	e.SetMaxMemtableSize(1)
	e.SetMaxTablesPerTier(1)

	// Write initial value
	require.NoError(t, e.Put([]byte("a"), []byte("old")))
	time.Sleep(50 * time.Millisecond)

	// Overwrite it in a new SST
	require.NoError(t, e.Put([]byte("a"), []byte("new")))
	time.Sleep(300 * time.Millisecond)

	// Compact should have happened
	val, found := e.Get([]byte("a"))
	assert.True(t, found)
	assert.True(t, bytes.Equal([]byte("new"), val))
}

func TestCompaction_RespectsDeletes(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine()
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	e.SetMaxMemtableSize(1)
	e.SetMaxTablesPerTier(2)

	require.NoError(t, e.Put([]byte("x"), []byte("1")))
	time.Sleep(50 * time.Millisecond)

	require.NoError(t, e.Delete([]byte("x")))
	time.Sleep(300 * time.Millisecond)

	// Should NOT be found after compaction
	val, found := e.Get([]byte("x"))
	assert.False(t, found)
	assert.Nil(t, val)
}

func TestCompaction_DeletesOldSSTables(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine()
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	e.SetMaxMemtableSize(1)
	e.SetMaxTablesPerTier(2)

	for i := range 3 {
		require.NoError(t, e.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))))
	}

	time.Sleep(500 * time.Millisecond)

	tier0 := e.Tiers()[0]
	for _, sst := range tier0 {
		_, err := os.Stat(sst.GetPath())
		assert.Error(t, err, "Expected SST to be deleted: %s", sst.GetPath())
	}
}

func TestCompaction_WritesToCorrectTier(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine()
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	e.SetMaxMemtableSize(1)
	e.SetMaxTablesPerTier(2)

	for i := range 4 {
		require.NoError(t, e.Put(fmt.Appendf(nil, "k%d", i), fmt.Appendf(nil, "v%d", i)))
	}

	time.Sleep(500 * time.Millisecond)

	tiers := e.Tiers()
	assert.GreaterOrEqual(t, len(tiers), 2)
	assert.Greater(t, len(tiers[1]), 0)

	for _, sst := range tiers[1] {
		assert.Contains(t, sst.GetPath(), "T1")
	}
}

func TestCompaction_CreatesValidMergedSSTable(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine()
	err := e.OpenDB(tmpDir)
	require.NoError(t, err)

	e.SetMaxMemtableSize(1)
	e.SetMaxTablesPerTier(2)

	require.NoError(t, e.Put([]byte("z"), []byte("last")))
	time.Sleep(100 * time.Millisecond)

	require.NoError(t, e.Put([]byte("z"), []byte("latest")))
	time.Sleep(400 * time.Millisecond)

	require.NoError(t, e.Put([]byte("z"), []byte("last latest")))
	time.Sleep(400 * time.Millisecond)

	// Validate merged SST file in T1
	tiers := e.Tiers()
	require.Equal(t, len(tiers), 2)
	var merged *sstable.SSTable
	for _, s := range tiers[1] {
		if filepath.Base(s.GetPath()) != "" {
			merged = s
			break
		}
	}
	require.NotNil(t, merged)
	require.NoError(t, merged.OpenForRead())
	val, err := merged.Lookup([]byte("z"))
	require.NoError(t, err)
	assert.True(t, bytes.Equal([]byte("last latest"), val))
	require.NoError(t, merged.Close())
}

func TestCompaction_PromotesToHigherTiers(t *testing.T) {
	tmpDir := t.TempDir()

	e := engine.NewEngine()
	require.NoError(t, e.OpenDB(tmpDir))

	e.SetMaxMemtableSize(1)
	e.SetMaxTablesPerTier(1) // So that every tier overflows immediately

	// Insert enough keys to trigger multi-tier compaction
	for i := range 5 {
		require.NoError(t, e.Put(fmt.Appendf(nil, "k%d", i), fmt.Appendf(nil, "v%d", i)))
		time.Sleep(100 * time.Millisecond) // Give time for flush + compaction
	}

	time.Sleep(1 * time.Second) // Ensure all compactions finish

	tiers := e.Tiers()
	require.GreaterOrEqual(t, len(tiers), 3, "Expected compaction to reach tier T2")

	// Check key still exists
	val, found := e.Get([]byte("k0"))
	assert.True(t, found)
	assert.True(t, bytes.Equal([]byte("v0"), val))

	// Ensure at least one SSTable exists in T2
	assert.GreaterOrEqual(t, len(tiers[2]), 1, "Expected SSTables in tier T2")
	for _, sst := range tiers[2] {
		assert.Contains(t, sst.GetPath(), "T2")
	}
}

func TestEngine_WALReplay_MixedTombstones(t *testing.T) {
	tmpDir := t.TempDir()

	func() {
		e := engine.NewEngine()
		require.NoError(t, e.OpenDB(tmpDir))

		require.NoError(t, e.Put([]byte("a"), []byte("1")))
		require.NoError(t, e.Put([]byte("b"), []byte("2")))
		require.NoError(t, e.Delete([]byte("a")))
		require.NoError(t, e.Put([]byte("c"), []byte("3")))
		require.NoError(t, e.Delete([]byte("b")))
	}()

	// Simulate restart
	e := engine.NewEngine()
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
