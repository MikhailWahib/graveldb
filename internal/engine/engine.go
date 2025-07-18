package engine

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/MikhailWahib/graveldb/internal/memtable"
	"github.com/MikhailWahib/graveldb/internal/shared"
	"github.com/MikhailWahib/graveldb/internal/sstable"
	"github.com/MikhailWahib/graveldb/internal/wal"
)

// Engine is the main database engine, managing memtable, WAL, SSTables, and compaction.
type Engine struct {
	mu sync.RWMutex

	dataDir          string
	memtable         memtable.Memtable
	wal              *wal.WAL
	tiers            [][]*sstable.Reader
	compactionMgr    *CompactionManager
	wg               sync.WaitGroup // WaitGroup for background flush/compaction
	sstCounter       *atomic.Uint64
	maxMemtableSize  int
	maxTablesPerTier int
}

// NewEngine creates a new Engine instance for the given data directory.
func NewEngine() *Engine {
	return &Engine{
		memtable:         memtable.NewMemtable(),
		tiers:            make([][]*sstable.Reader, 0),
		sstCounter:       new(atomic.Uint64),
		maxMemtableSize:  MaxMemtableSize,
		maxTablesPerTier: MaxTablesPerTier,
	}
}

// OpenDB initializes the compaction manager and parses existing SSTables.
func (e *Engine) OpenDB(dataDir string) error {
	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		return err
	}
	e.dataDir = dataDir

	wal, err := wal.NewWAL(dataDir + "/wal.log")
	if err != nil {
		return err
	}

	entries, err := wal.Replay()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		switch entry.Type {
		case shared.PutEntry:
			if err := e.memtable.Put(entry.Key, entry.Value); err != nil {
				return err
			}
		case shared.DeleteEntry:
			if err := e.memtable.Delete(entry.Key); err != nil {
				return err
			}
		}
	}
	e.wal = wal

	compactionMgr := NewCompactionManager(e)
	e.compactionMgr = compactionMgr

	return e.parseTiers()
}

// parseTiers scans the SSTable directory and populates the engine's tier structure.
func (e *Engine) parseTiers() error {
	sstableDir := filepath.Join(e.dataDir, "sstables")
	subdirs, err := os.ReadDir(sstableDir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	var maxSSTNumber uint64

	for _, dir := range subdirs {
		if !dir.IsDir() || !strings.HasPrefix(dir.Name(), "T") {
			continue
		}

		tierStr := strings.TrimPrefix(dir.Name(), "T")
		tier, err := strconv.Atoi(tierStr)
		if err != nil {
			return fmt.Errorf("invalid tier dir name %q: %w", dir.Name(), err)
		}

		// Ensure tiers slice is long enough
		for len(e.tiers) <= tier {
			e.tiers = append(e.tiers, nil)
		}

		sstDir := filepath.Join(sstableDir, fmt.Sprintf("T%d", tier))
		files, err := os.ReadDir(sstDir)
		if err != nil {
			return err
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			filename := file.Name()
			if strings.HasSuffix(filename, ".sst") {
				numberStr := strings.TrimSuffix(filename, ".sst")
				if sstNum, err := strconv.ParseUint(numberStr, 10, 64); err == nil {
					if sstNum > maxSSTNumber {
						maxSSTNumber = sstNum
					}
				}
			}

			path := filepath.Join(sstDir, file.Name())
			reader, err := sstable.NewReader(path)
			if err != nil {
				log.Printf("failed to open SSTable for read: %v", err)
				continue
			}

			e.tiers[tier] = append(e.tiers[tier], reader)
		}
	}

	e.sstCounter.Store(maxSSTNumber)
	return nil
}

// Tiers returns the current SSTable tiers managed by the engine.
func (e *Engine) Tiers() [][]*sstable.Reader {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.tiers
}

// Put inserts or updates a key-value pair in the database.
func (e *Engine) Put(key, value []byte) error {
	if err := e.wal.AppendPut(key, value); err != nil {
		return err
	}

	if err := e.memtable.Put(key, value); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.memtable.Size() > e.maxMemtableSize {
		old := e.memtable
		e.memtable = memtable.NewMemtable()
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			if err := e.flushMemtable(old); err != nil {
				log.Printf("flushMemtable error: %v", err)
			}
		}()
	}

	return nil
}

// Get retrieves the value for a given key, searching memtable and all SSTable tiers.
func (e *Engine) Get(key []byte) ([]byte, bool) {
	// First check memtable
	entry, found := e.memtable.Get(key)
	if found {
		// If found and marked as deleted, return immediately
		if entry.Type == shared.DeleteEntry {
			return nil, false
		}
		return entry.Value, true
	}

	// Not found in memtable, search in disk
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Search all tiers, newest to oldest
	for _, tier := range e.tiers {
		for i := len(tier) - 1; i >= 0; i-- {
			reader := tier[i]

			// Use the new Get method instead of Lookup
			entry, err := reader.Get(key)
			if err == nil {
				if entry.Type == shared.DeleteEntry {
					return nil, false
				}
				return entry.Value, true
			}
		}
	}

	return nil, false
}

// Delete removes a key from the database.
func (e *Engine) Delete(key []byte) error {
	if err := e.wal.AppendDelete(key); err != nil {
		return err
	}
	return e.memtable.Delete(key)
}

// flushMemtable writes the contents of a memtable to a new SSTable on disk.
func (e *Engine) flushMemtable(mt memtable.Memtable) error {
	entries := mt.Entries()

	l0Dir := filepath.Join(e.dataDir, "sstables", "T0")
	if err := os.MkdirAll(l0Dir, 0755); err != nil {
		return fmt.Errorf("failed to create T0 directory: %w", err)
	}

	filename := fmt.Sprintf("%s/%06d.sst", l0Dir, e.sstCounter.Add(1))

	writer, err := sstable.NewWriter(filename)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if err := writer.PutEntry(entry.Key, entry.Value); err != nil {
			return fmt.Errorf("failed to put entry to SSTable: %w", err)
		}
	}

	// Close will automatically call Finish()
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to finish SSTable: %w", err)
	}

	reader, err := sstable.NewReader(filename)
	if err != nil {
		return fmt.Errorf("failed to open SSTable for reading: %w", err)
	}

	e.mu.Lock()
	// Ensure tier 0 exists
	if len(e.tiers) == 0 {
		e.tiers = append(e.tiers, []*sstable.Reader{})
	}

	// Append reader to T0
	e.tiers[0] = append(e.tiers[0], reader)

	shouldCompact := e.compactionMgr.shouldCompactTier(0)
	e.mu.Unlock()

	// Run compaction on T0 if needed
	if shouldCompact {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			if err := e.compactionMgr.compactTiers(0); err != nil {
				log.Printf("compaction error: %v", err)
			}
		}()
	}

	return nil
}

// SetMaxMemtableSize sets the maximum size for memtable before flushing.
func (e *Engine) SetMaxMemtableSize(sizeInBytes int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.maxMemtableSize = sizeInBytes
}

// SetMaxTablesPerTier sets the maximum number of SSTables per tier before compaction.
func (e *Engine) SetMaxTablesPerTier(n int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.maxTablesPerTier = n
}

// Close gracefully shuts down the engine, ensuring all data is persisted.
// This method:
//   - Flushes any remaining memtable data to disk
//   - Closes the WAL file
//   - Waits for any ongoing compaction operations to complete
//
// After calling Close, the engine should not be used for any operations.
// Returns an error if any cleanup operation fails.
func (e *Engine) Close() error {
	// Flush any remaining memtable data
	if e.memtable != nil && e.memtable.Size() > 0 {
		old := e.memtable
		e.memtable = memtable.NewMemtable()
		// Flush synchronously to ensure data is persisted
		if err := e.flushMemtable(old); err != nil {
			return fmt.Errorf("failed to flush final memtable: %w", err)
		}
	}

	e.mu.Lock()
	// Close opened SST readers
	for _, tier := range e.tiers {
		for _, reader := range tier {
			_ = reader.Close()
		}
	}
	e.mu.Unlock()

	// Close the WAL
	if e.wal != nil {
		if err := e.wal.Close(); err != nil {
			return fmt.Errorf("failed to close WAL: %w", err)
		}
	}

	// Wait for all background flush/compaction operations to finish
	e.wg.Wait()

	return nil
}

// WaitForFlush waits for all flushed to be done (for tests)
func (e *Engine) WaitForFlush() {
	e.wg.Wait()
}
