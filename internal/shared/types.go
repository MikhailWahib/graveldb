package shared

type EntryType byte

const (
	PutEntry EntryType = iota
	DeleteEntry
)
