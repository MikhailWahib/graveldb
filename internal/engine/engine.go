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

	"github.com/MikhailWahib/graveldb/internal/config"
	"github.com/MikhailWahib/graveldb/internal/memtable"
	"github.com/MikhailWahib/graveldb/internal/sstable"
	"github.com/MikhailWahib/graveldb/internal/storage"
	"github.com/MikhailWahib/graveldb/internal/wal"
)

// Engine is the main database engine, managing memtable, WAL, SSTables, and compaction.
type Engine struct {
	mu   sync.RWMutex
	once sync.Once
	wg   sync.WaitGroup

	dataDir            string
	memtable           memtable.Memtable
	immutableMemtables []memtable.Memtable
	wal                *wal.WAL
	tiers              [][]*sstable.Reader
	compactionMgr      *CompactionManager
	sstCounter         *atomic.Uint64
	maxMemtableSize    int
	maxTablesPerTier   int
	config             *config.Config
}

// NewEngine creates a new Engine instance for the given data directory.
func NewEngine(cfg *config.Config) *Engine {
	if cfg == nil {
		cfg = config.DefaultConfig()
	} else {
		cfg.FillDefaults()
	}
	return &Engine{
		memtable:         memtable.NewMemtable(),
		tiers:            make([][]*sstable.Reader, 0),
		sstCounter:       new(atomic.Uint64),
		maxMemtableSize:  cfg.MaxMemtableSize,
		maxTablesPerTier: cfg.MaxTablesPerTier,
		config:           cfg,
	}
}

// OpenDB initializes the compaction manager and parses existing SSTables.
func (e *Engine) OpenDB(dataDir string) error {
	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		return err
	}
	e.dataDir = dataDir

	wal, err := wal.NewWAL(dataDir+"/wal.log", e.config.WALFlushThreshold, e.config.WALFlushInterval)
	if err != nil {
		return err
	}

	entries, err := wal.Replay()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		switch entry.Type {
		case storage.PutEntry:
			if err := e.memtable.Put(entry.Key, entry.Value); err != nil {
				return err
			}
		case storage.DeleteEntry:
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
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.wal.AppendPut(key, value); err != nil {
		return err
	}

	if err := e.memtable.Put(key, value); err != nil {
		return err
	}

	if e.memtable.Size() > e.maxMemtableSize {
		e.immutableMemtables = append(e.immutableMemtables, e.memtable)
		e.memtable = memtable.NewMemtable()
		immutableToFlush := e.immutableMemtables[len(e.immutableMemtables)-1]
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			if err := e.flushMemtable(immutableToFlush); err != nil {
				log.Printf("flushMemtable error: %v", err)
			}
		}()
	}

	return nil
}

// Get retrieves the value for a given key, searching memtable and all SSTable tiers.
func (e *Engine) Get(key []byte) ([]byte, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// First check memtable
	entry, found := e.memtable.Get(key)
	if found {
		if entry.Type == storage.DeleteEntry {
			return nil, false
		}
		return entry.Value, true
	}

	// Check immutable memtables
	for _, mt := range e.immutableMemtables {
		entry, found := mt.Get(key)
		if found {
			if entry.Type == storage.DeleteEntry {
				return nil, false
			}
			return entry.Value, true
		}
	}

	// Not found in memtable, search in disk
	// Search all tiers, newest to oldest
	for _, tier := range e.tiers {
		for i := len(tier) - 1; i >= 0; i-- {
			reader := tier[i]

			entry, err := reader.Get(key)
			if err == nil {
				if entry.Type == storage.DeleteEntry {
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
	e.mu.Lock()
	defer e.mu.Unlock()

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

	writer, err := sstable.NewWriter(filename, e.config.IndexInterval)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		switch entry.Type {
		case storage.PutEntry:
			if err := writer.PutEntry(entry.Key, entry.Value); err != nil {
				return fmt.Errorf("failed to put entry to SSTable: %w", err)
			}
		case storage.DeleteEntry:
			if err := writer.DeleteEntry(entry.Key); err != nil {
				return fmt.Errorf("failed to put entry to SSTable: %w", err)
			}
		}
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to finish SSTable: %w", err)
	}

	reader, err := sstable.NewReader(filename)
	if err != nil {
		return fmt.Errorf("failed to open SSTable for reading: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Ensure tier 0 exists
	if len(e.tiers) == 0 {
		e.tiers = append(e.tiers, []*sstable.Reader{})
	}

	// Append reader to T0
	e.tiers[0] = append(e.tiers[0], reader)

	// Remove the flushed memtable from immutableMemtables
	for i, immutable := range e.immutableMemtables {
		if immutable == mt {
			e.immutableMemtables = append(e.immutableMemtables[:i], e.immutableMemtables[i+1:]...)
			break
		}
	}

	shouldCompact := e.compactionMgr.shouldCompactTier(0)

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

// Close gracefully shuts down the engine, ensuring all data is persisted.
// This method:
//   - Flushes any remaining memtable data to disk
//   - Closes the WAL file
//   - Waits for any ongoing compaction operations to complete
//
// After calling Close, the engine should not be used for any operations.
// Returns an error if any cleanup operation fails.
func (e *Engine) Close() error {
	var finalErr error

	e.once.Do(func() {
		// Flush any remaining memtable data
		if e.memtable != nil && e.memtable.Size() > 0 {
			old := e.memtable
			e.memtable = memtable.NewMemtable()
			// Flush synchronously to ensure data is persisted
			if err := e.flushMemtable(old); err != nil {
				finalErr = fmt.Errorf("failed to flush final memtable: %w", err)
			}
		}

		// Flush all immutable memtables
		for _, immutable := range e.immutableMemtables {
			if err := e.flushMemtable(immutable); err != nil {
				finalErr = fmt.Errorf("failed to flush immutable memtable: %w", err)
			}
		}

		// Close opened SST readers
		for _, tier := range e.tiers {
			for _, reader := range tier {
				_ = reader.Close()
			}
		}

		// Close the WAL
		if e.wal != nil {
			if err := e.wal.Close(); err != nil {
				finalErr = fmt.Errorf("failed to close WAL: %w", err)
			}
		}

		// Wait for all background flush/compaction operations to finish
		e.wg.Wait()
	})

	return finalErr
}

// WaitForFlush waits for all flushed to be done (for tests)
func (e *Engine) WaitForFlush() {
	e.wg.Wait()
}
