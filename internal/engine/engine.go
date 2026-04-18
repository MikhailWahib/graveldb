package engine

import (
	"fmt"
	gerrors "github.com/MikhailWahib/graveldb/internal/errors"
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
	immutableMemtables []immutableMemtable
	wal                *wal.WAL
	tiers              [][]*sstable.Reader
	compactionMgr      *CompactionManager
	sstCounter         *atomic.Uint64
	walCounter         *atomic.Uint64
	maxMemtableSize    int
	maxTablesPerTier   int
	config             *config.Config
}

type immutableMemtable struct {
	mt      memtable.Memtable
	walPath string
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
		walCounter:       new(atomic.Uint64),
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

	walFile, err := wal.NewWAL(dataDir+"/wal.log", e.config.WALFlushThreshold, e.config.WALFlushInterval)
	if err != nil {
		return err
	}

	entries, err := wal.ReplayDir(dataDir)
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
	e.wal = walFile

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
			return gerrors.Internal("invalid tier dir name", err)
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
		walPath := e.nextWalPath()
		sealedPath, err := e.wal.Seal(walPath)
		if err != nil {
			return err
		}

		immutable := immutableMemtable{
			mt:      e.memtable,
			walPath: sealedPath,
		}
		e.immutableMemtables = append(e.immutableMemtables, immutable)
		e.memtable = memtable.NewMemtable()
		immutableToFlush := e.immutableMemtables[len(e.immutableMemtables)-1]
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			if err := e.flushMemtable(immutableToFlush.mt, immutableToFlush.walPath); err != nil {
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

	// Check immutable memtables, newest to oldest so newer writes win.
	for i := len(e.immutableMemtables) - 1; i >= 0; i-- {
		mt := e.immutableMemtables[i].mt
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
func (e *Engine) flushMemtable(mt memtable.Memtable, walPath string) error {
	entries := mt.Entries()

	filename, writer, err := e.newFlushWriter()
	if err != nil {
		return err
	}

	if err := e.writeFlushEntries(writer, entries); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return gerrors.IO("failed to finish SSTable", err)
	}

	reader, err := sstable.NewReader(filename)
	if err != nil {
		return gerrors.IO("failed to open SSTable for reading", err)
	}

	shouldCompact := e.registerFlushedMemtable(mt, reader)
	e.maybeCompactT0(shouldCompact)
	e.removeWalSegment(walPath)

	return nil
}

func (e *Engine) newFlushWriter() (string, *sstable.Writer, error) {
	l0Dir := filepath.Join(e.dataDir, "sstables", "T0")
	if err := os.MkdirAll(l0Dir, 0755); err != nil {
		return "", nil, gerrors.IO("failed to create T0 directory", err)
	}

	filename := filepath.Join(l0Dir, fmt.Sprintf("%06d.sst", e.sstCounter.Add(1)))

	writer, err := sstable.NewWriter(filename, e.config.IndexInterval)
	if err != nil {
		return "", nil, err
	}

	return filename, writer, nil
}

func (e *Engine) writeFlushEntries(writer *sstable.Writer, entries []storage.Entry) error {
	for _, entry := range entries {
		if err := e.writeFlushEntry(writer, entry); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) writeFlushEntry(writer *sstable.Writer, entry storage.Entry) error {
	switch entry.Type {
	case storage.PutEntry:
		if err := writer.PutEntry(entry.Key, entry.Value); err != nil {
			return gerrors.IO("failed to put entry to SSTable", err)
		}
	case storage.DeleteEntry:
		if err := writer.DeleteEntry(entry.Key); err != nil {
			return gerrors.IO("failed to put entry to SSTable", err)
		}
	}
	return nil
}

func (e *Engine) registerFlushedMemtable(mt memtable.Memtable, reader *sstable.Reader) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.tiers) == 0 {
		e.tiers = append(e.tiers, []*sstable.Reader{})
	}

	e.tiers[0] = append(e.tiers[0], reader)
	e.removeImmutableMemtableLocked(mt)

	if e.compactionMgr == nil {
		return false
	}
	return e.compactionMgr.shouldCompactTier(0)
}

func (e *Engine) removeImmutableMemtableLocked(mt memtable.Memtable) {
	for i, immutable := range e.immutableMemtables {
		if immutable.mt == mt {
			e.immutableMemtables = append(e.immutableMemtables[:i], e.immutableMemtables[i+1:]...)
			return
		}
	}
}

func (e *Engine) maybeCompactT0(shouldCompact bool) {
	if !shouldCompact {
		return
	}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		if err := e.compactionMgr.compactTiers(0); err != nil {
			log.Printf("compaction error: %v", err)
		}
	}()
}

func (e *Engine) removeWalSegment(walPath string) {
	if walPath == "" {
		return
	}

	if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
		log.Printf("failed to remove WAL file %s: %v", walPath, err)
	}
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
			walPath := ""
			if e.wal != nil {
				path := e.nextWalPath()
				segment, err := e.wal.Seal(path)
				if err != nil {
					finalErr = gerrors.IO("failed to seal WAL before final flush", err)
				} else {
					walPath = segment
				}
			}
			e.memtable = memtable.NewMemtable()
			if err := e.flushMemtable(old, walPath); err != nil {
				finalErr = gerrors.IO("failed to flush final memtable", err)
			}
		}

		pending := append([]immutableMemtable(nil), e.immutableMemtables...)
		for _, immutable := range pending {
			if err := e.flushMemtable(immutable.mt, immutable.walPath); err != nil {
				finalErr = gerrors.IO("failed to flush immutable memtable", err)
			}
		}

		for _, tier := range e.tiers {
			for _, reader := range tier {
				_ = reader.Close()
			}
		}

		if e.wal != nil {
			if err := e.wal.Close(); err != nil {
				finalErr = gerrors.IO("failed to close WAL", err)
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

func (e *Engine) nextWalPath() string {
	return filepath.Join(e.dataDir, fmt.Sprintf("wal-%06d.log", e.walCounter.Add(1)))
}
