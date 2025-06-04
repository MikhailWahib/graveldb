package shared

// EntryType represents the type of entry stored in the database
type EntryType byte

const (
	// PutEntry indicates a key-value insertion operation
	PutEntry EntryType = iota
	// DeleteEntry indicates a key deletion operation
	DeleteEntry
)
