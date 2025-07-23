// Package memtable implements an in-memory table structure for the database,
// providing fast access to recently written data before it is persisted to disk.
package memtable

import (
	"github.com/MikhailWahib/graveldb/internal/storage"
)

// Memtable defines the interface for an in-memory table that supports basic operations
type Memtable interface {
	Entries() []storage.Entry
	Set(key, value []byte) error
	Get(key []byte) (storage.Entry, bool)
	Delete(key []byte) error
	Size() int
	Clear()
}

// SkiplistMemtable implements the Memtable interface using a skiplist
// data structure for efficient operations
type SkiplistMemtable struct {
	sl *SkipList
}

// NewMemtable creates a new Memtable instance.
func NewMemtable() Memtable {
	return &SkiplistMemtable{
		sl: NewSkipList(),
	}
}

// Entries returns all entries in the memtable memtable.
func (m *SkiplistMemtable) Entries() []storage.Entry {
	return m.sl.Entries()
}

// Set inserts or updates an entry in the memtable
func (m *SkiplistMemtable) Set(key, value []byte) error {

	m.sl.Set(storage.Entry{Type: storage.SetEntry, Key: key, Value: value})
	return nil
}

// Get retrieves an entry from the memtable by key
func (m *SkiplistMemtable) Get(key []byte) (storage.Entry, bool) {
	return m.sl.Get(key)
}

// Delete marks the given key as deleted
func (m *SkiplistMemtable) Delete(key []byte) error {
	err := m.sl.Delete(key)
	if err != nil {
		return err
	}
	return nil
}

// Size returns the size of entries in the memtable in bytes
func (m *SkiplistMemtable) Size() int {
	return m.sl.Size()
}

// Clear clears the memtable
func (m *SkiplistMemtable) Clear() {
	m.sl.Clear()
}
