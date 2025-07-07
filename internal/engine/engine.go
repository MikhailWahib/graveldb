package engine

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/MikhailWahib/graveldb/internal/memtable"
	"github.com/MikhailWahib/graveldb/internal/shared"
	"github.com/MikhailWahib/graveldb/internal/sstable"
	"github.com/MikhailWahib/graveldb/internal/wal"
)

type Engine struct {
	dataDir       string
	memtable      memtable.Memtable
	wal           *wal.WAL
	levels        [][]*sstable.SSTable
	compactionMgr *CompactionManager
	sstCounter    *atomic.Uint64
}

func NewEngine(dataDir string) (*Engine, error) {
	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		return nil, err
	}

	wal, err := wal.NewWAL(dataDir + "/wal.log")
	if err != nil {
		return nil, err
	}

	mt := memtable.NewMemtable()

	entries, err := wal.Replay()
	if err != nil {
		return nil, err
	}

	for _, e := range entries {
		switch e.Type {
		case shared.PutEntry:
			mt.Put(e.Key, e.Value)
		case shared.DeleteEntry:
			mt.Delete(e.Key)
		}
	}

	var sstCount uint64

	atomic.StoreUint64(&sstCount, 0)

	engine := &Engine{
		memtable:   mt,
		wal:        wal,
		levels:     make([][]*sstable.SSTable, 0),
		dataDir:    dataDir,
		sstCounter: new(atomic.Uint64),
	}

	return engine, nil
}

func (e *Engine) OpenDB() error {
	err := e.parseLevels()
	if err != nil {
		return err
	}

	return nil
}

func (e *Engine) parseLevels() error {
	sstableDir := filepath.Join(e.dataDir, "sstables")
	subdirs, err := os.ReadDir(sstableDir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	var maxSSTNumber uint64

	for _, dir := range subdirs {
		if !dir.IsDir() || !strings.HasPrefix(dir.Name(), "L") {
			continue
		}

		levelStr := strings.TrimPrefix(dir.Name(), "L")
		level, err := strconv.Atoi(levelStr)
		if err != nil {
			return fmt.Errorf("invalid level dir name %q: %w", dir.Name(), err)
		}

		// Ensure levels slice is long enough
		for len(e.levels) <= level {
			e.levels = append(e.levels, nil)
		}

		sstDir := filepath.Join(sstableDir, fmt.Sprintf("L%d", level))
		files, err := os.ReadDir(sstDir)
		if err != nil {
			return err
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			// Extract the SSTable number from filename (assuming format like "0001.sst")
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
			e.levels[level] = append(e.levels[level], sst)
		}
	}

	e.sstCounter.Store(maxSSTNumber)

	return nil
}

func (e *Engine) Levels() [][]*sstable.SSTable {
	return e.levels
}

func (e *Engine) Put(key, value string) error {
	err := e.wal.AppendPut(key, value)
	if err != nil {
		return err
	}

	err = e.memtable.Put(key, value)
	if err != nil {
		return err
	}

	if e.memtable.Size() > MAX_MEMTABLE_SIZE {
		old := e.memtable
		e.memtable = memtable.NewMemtable()
		go e.flushMemtable(old)
	}

	return nil
}

func (e *Engine) Get(key string) (string, bool) {
	// First check memtable
	val, found := e.memtable.Get(key)
	if found {
		if val == memtable.TOMBSTONE {
			return "", false
		}
		return val, true
	}

	// Then search levels in order (newest first)
	for level := range e.levels {
		for _, sst := range e.levels[level] {
			if err := sst.OpenForRead(); err != nil {
				log.Printf("error opening sst: %s for read", sst.GetPath())
				return "", false
			}
			defer sst.Close()

			if val, err := sst.Lookup([]byte(key)); err == nil {
				if string(val) == memtable.TOMBSTONE {
					return "", false
				}

				return string(val), true
			}
		}
	}

	return "", false
}

func (e *Engine) Delete(key string) error {
	err := e.wal.AppendDelete(key)
	if err != nil {
		return err
	}

	err = e.memtable.Delete(key)
	if err != nil {
		return err
	}

	return nil
}

func (e *Engine) flushMemtable(mt memtable.Memtable) error {
	entries := mt.Entries()

	l0Dir := filepath.Join(e.dataDir, "sstables", "L0")
	if err := os.MkdirAll(l0Dir, 0755); err != nil {
		return fmt.Errorf("failed to create L0 directory: %w", err)
	}

	// Generate unique filename using atomic counter
	filename := fmt.Sprintf("%s/%06d.sst", l0Dir, e.sstCounter.Add(1))
	sst := sstable.NewSSTable(filename)

	// Handle potential error from OpenForWrite
	if err := sst.OpenForWrite(); err != nil {
		return fmt.Errorf("failed to open SSTable for writing: %w", err)
	}

	// Write all entries
	for _, entry := range entries {
		if err := sst.AppendPut([]byte(entry.Key), []byte(entry.Value)); err != nil {
			return fmt.Errorf("failed to append entry to SSTable: %w", err)
		}
	}

	if err := sst.Finish(); err != nil {
		return fmt.Errorf("failed to finish SSTable: %w", err)
	}

	if err := sst.Close(); err != nil {
		return fmt.Errorf("failed to close SSTable: %w", err)
	}

	// add the flushed sstable to levels
	// ensure L0 exists in levels slice
	if len(e.levels) == 0 {
		e.levels = append(e.levels, []*sstable.SSTable{})
	}

	// Add the new SSTable to L0
	e.levels[0] = append(e.levels[0], sst)

	return nil
}
