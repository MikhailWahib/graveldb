// Package memtable implements an in-memory table structure for the database,
// providing fast access to recently written data before it is persisted to disk.
package memtable

import (
	"sync"

	"github.com/MikhailWahib/graveldb/internal/record"
)

// Memtable defines the interface for an in-memory table that supports basic operations
type Memtable interface {
	// Entries return all entries in the skiplist
	Entries() []record.Entry
	Put(key, value []byte) error
	Get(key []byte) (record.Entry, bool)
	// Delete marks the given key as deleted
	// Returns an error if the operation fails.
	Delete(key []byte) error
	// Size returns the size of the memtable in bytes.
	Size() int
	// Clear clears the memtable
	Clear()
}

// SkiplistMemtable implements the Memtable interface using a skiplist
// data structure for efficient operations
type SkiplistMemtable struct {
	mu sync.RWMutex

	sl *SkipList
}

// NewMemtable creates a new Memtable instance.
func NewMemtable() Memtable {
	return &SkiplistMemtable{
		sl: NewSkipList(),
	}
}

// Entries returns all entries in the skiplist memtable.
func (m *SkiplistMemtable) Entries() []record.Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.sl.Entries()
}

// Put inserts or updates an entry in the memtable
func (m *SkiplistMemtable) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.sl.Put(record.Entry{Type: record.PutEntry, Key: key, Value: value})
	return nil
}

// Get retrieves an entry from the memtable by key
func (m *SkiplistMemtable) Get(key []byte) (record.Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.sl.Get(key)
}

// Delete marks the given key as deleted
func (m *SkiplistMemtable) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := m.sl.Delete(key)
	if err != nil {
		return err
	}
	return nil
}

// Size returns the size of entries in the skiplist in bytes
func (m *SkiplistMemtable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.sl.Size()
}

// Clear clears the skiplist
func (m *SkiplistMemtable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.sl.Clear()
}
