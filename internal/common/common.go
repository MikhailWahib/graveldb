package common

type EntryType byte

const (
	PutEntry EntryType = iota
	DeleteEntry
)
