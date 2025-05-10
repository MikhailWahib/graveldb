package memtable

import (
	"fmt"

	"github.com/MikhailWahib/graveldb/internal/common"
	"github.com/MikhailWahib/graveldb/internal/diskmanager"
	"github.com/MikhailWahib/graveldb/internal/wal"
)

const TOMBSTONE = "TOMBSTONE"

type Memtable interface {
	// Put inserts a key-value pair into the memtable and appends the operation to the Write-Ahead Log (WAL).
	// Returns an error if the operation fails.
	Put(key, value string) error
	// Get retrieves the value associated with the key from the memtable.
	// Returns the value and a boolean indicating if the key was found.
	Get(key string) (string, bool)
	// Delete removes the key from the memtable and appends the delete operation to the Write-Ahead Log (WAL).
	// Returns an error if the operation fails.
	Delete(key string) error
	// Size returns the number of key-value pairs in the memtable.
	Size() int
}

type SkiplistMemtable struct {
	sl  *SkipList
	wal *wal.WAL
}

// NewMemtable creates a new Memtable instance with a Write-Ahead Log (WAL).
func NewMemtable(dm diskmanager.DiskManager, walPath string) (Memtable, error) {
	w, err := wal.NewWAL(dm, walPath)
	if err != nil {
		return nil, err
	}

	mt := &SkiplistMemtable{
		sl:  NewSkipList(),
		wal: w,
	}

	// Rebuild memtable from WAL
	entries, err := w.Replay()
	if err != nil {
		return nil, err
	}

	for _, e := range entries {
		switch e.Type {
		case common.PutEntry:
			mt.sl.Put(e.Key, e.Value)
		case common.DeleteEntry:
			mt.sl.Delete(e.Key)
		}
	}

	return mt, nil
}

func (m *SkiplistMemtable) Put(key, value string) error {
	err := m.wal.AppendPut(key, value)
	if err != nil {
		return err
	}

	m.sl.Put(key, value)
	return nil
}

func (m *SkiplistMemtable) Get(key string) (string, bool) {
	return m.sl.Get(key)
}

func (m *SkiplistMemtable) Delete(key string) error {
	err := m.wal.AppendDelete(key)
	if err != nil {
		return fmt.Errorf("failed to append delete operation to WAL: %v", err)
	}

	val, ok := m.sl.Get(key)

	// Ignore the case where the key is already deleted
	if val == TOMBSTONE {
		return nil
	}

	if ok {
		m.sl.Delete(key)
		return nil
	}

	m.sl.Put(key, TOMBSTONE)
	return nil
}

func (m *SkiplistMemtable) Size() int {
	return m.sl.Size()
}
