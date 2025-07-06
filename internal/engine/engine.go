package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/MikhailWahib/graveldb/internal/memtable"
	"github.com/MikhailWahib/graveldb/internal/shared"
	"github.com/MikhailWahib/graveldb/internal/sstable"
	"github.com/MikhailWahib/graveldb/internal/wal"
)

type Engine struct {
	dataDir       string
	memtable      memtable.Memtable
	immutable     []memtable.Memtable
	wal           *wal.WAL
	levels        [][]*sstable.SSTable
	compactionMgr *CompactionManager
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

	engine := &Engine{
		memtable:  mt,
		immutable: make([]memtable.Memtable, 0),
		wal:       wal,
		levels:    make([][]*sstable.SSTable, 0),
		dataDir:   dataDir,
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
			sst := sstable.NewSSTable(filepath.Join(sstDir, file.Name()))
			e.levels[level] = append(e.levels[level], sst)
		}
	}

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

	return nil
}

func (e *Engine) Get(key string) (string, bool) {
	val, found := e.memtable.Get(key)
	if !found || val == memtable.TOMBSTONE {
		return "", false
	}

	return val, true
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
