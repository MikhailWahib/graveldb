package storage

// EntryType represents the type of entry stored in the database
type EntryType byte

const (
	// SetEntry indicates a key-value insertion operation
	SetEntry EntryType = iota
	// DeleteEntry indicates a key deletion operation
	DeleteEntry
	// IndexEntry indicates an index record in the SSTable
	IndexEntry
)

// Entry represents a database entry to be written to storage
type Entry struct {
	Type  EntryType
	Key   []byte
	Value []byte
}
