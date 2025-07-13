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
	tiers            [][]*sstable.SSTable
	compactionMgr    *CompactionManager
	sstCounter       *atomic.Uint64
	maxMemtableSize  int
	maxTablesPerTier int
}

// NewEngine creates a new Engine instance for the given data directory.
func NewEngine() *Engine {
	return &Engine{
		memtable:         memtable.NewMemtable(),
		tiers:            make([][]*sstable.SSTable, 0),
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

			sst := sstable.NewSSTable(filepath.Join(sstDir, file.Name()))
			e.tiers[tier] = append(e.tiers[tier], sst)
		}
	}

	e.sstCounter.Store(maxSSTNumber)
	return nil
}

// Tiers returns the current SSTable tiers managed by the engine.
func (e *Engine) Tiers() [][]*sstable.SSTable {
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
		go func() {
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
	val, found := e.memtable.Get(key)
	if found {
		if string(val) == memtable.TOMBSTONE {
			return nil, false
		}
		return val, true
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// Search all tiers, newest to oldest
	for _, tier := range e.tiers {
		for i := len(tier) - 1; i >= 0; i-- {
			sst := tier[i]

			if err := sst.OpenForRead(); err != nil {
				log.Printf("error opening sst: %s for read: %v", sst.GetPath(), err)
				continue
			}

			val, err := sst.Lookup(key)

			// Close immediately after lookup
			if closeErr := sst.Close(); closeErr != nil {
				log.Printf("error closing sst: %s: %v", sst.GetPath(), closeErr)
			}

			if err == nil {
				if string(val) == memtable.TOMBSTONE {
					return nil, false
				}
				return val, true
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
	sst := sstable.NewSSTable(filename)

	if err := sst.OpenForWrite(); err != nil {
		return fmt.Errorf("failed to open SSTable for writing: %w", err)
	}

	for _, entry := range entries {
		if err := sst.AppendPut(entry.Key, entry.Value); err != nil {
			return fmt.Errorf("failed to append entry to SSTable: %w", err)
		}
	}

	if err := sst.Finish(); err != nil {
		return fmt.Errorf("failed to finish SSTable: %w", err)
	}

	if err := sst.Close(); err != nil {
		return fmt.Errorf("failed to close SSTable: %w", err)
	}

	e.mu.Lock()
	// Ensure tier 0 exists
	if len(e.tiers) == 0 {
		e.tiers = append(e.tiers, []*sstable.SSTable{})
	}

	// Append SST to T0
	e.tiers[0] = append(e.tiers[0], sst)

	shouldCompact := e.compactionMgr.shouldCompactTier(0)
	e.mu.Unlock()

	// Run compaction on T0 if needed
	if shouldCompact {
		go func() {
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
