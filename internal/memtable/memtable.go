// Package memtable implements an in-memory table structure for the database,
// providing fast access to recently written data before it is persisted to disk.
package memtable

// TOMBSTONE represents a deletion marker in the memtable
const TOMBSTONE = "TOMBSTONE"

// Memtable defines the interface for an in-memory table that supports
// basic key-value operations with durability guarantees
type Memtable interface {
	// Entries return all entries in the skiplist
	Entries() []Entry
	// Put inserts a key-value pair into the memtable and appends the operation to the Write-Ahead Log (WAL).
	// Returns an error if the operation fails.
	Put(key, value string) error
	// Get retrieves the value associated with the key from the memtable.
	// Returns the value and a boolean indicating if the key was found.
	Get(key string) (string, bool)
	// Delete removes the key from the memtable and appends the delete operation to the Write-Ahead Log (WAL).
	// Returns an error if the operation fails.
	Delete(key string) error
	// Size returns the size of the memtable in bytes.
	Size() int
	// Clear clears the memtable
	Clear()
}

// SkiplistMemtable implements the Memtable interface using a skiplist
// data structure for efficient operations
type SkiplistMemtable struct {
	sl *SkipList
}

// NewMemtable creates a new Memtable instance with a Write-Ahead Log (WAL).
func NewMemtable() Memtable {
	return &SkiplistMemtable{
		sl: NewSkipList(),
	}
}

func (m *SkiplistMemtable) Entries() []Entry {
	return m.sl.Entries()
}

// Put inserts or updates a key-value pair in the memtable
func (m *SkiplistMemtable) Put(key, value string) error {
	m.sl.Put(key, value)
	return nil
}

// Get retrieves a value from the memtable by key
func (m *SkiplistMemtable) Get(key string) (string, bool) {
	return m.sl.Get(key)
}

// Delete marks a key from the memtable as removed with TOMBSTONE
func (m *SkiplistMemtable) Delete(key string) error {
	err := m.sl.Delete(key)
	if err != nil {
		return err
	}
	return nil
}

// Size returns the size of entries in the skiplist in bytes
func (m *SkiplistMemtable) Size() int {
	return m.sl.Size()
}

// Clear clears the skiplist
func (m *SkiplistMemtable) Clear() {
	m.sl.Clear()
}
