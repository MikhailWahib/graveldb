// Package record provides common types and utilities used across the database implementation.
package record

// EntryTypeSize is the size in bytes used to store an entry type marker
const EntryTypeSize = 1

// LengthSize is the size in bytes used to store length prefixes
const LengthSize = 4

// PrefixSize is the total size of entry metadata (type + key length + value length)
const PrefixSize = EntryTypeSize + (2 * LengthSize) // 9 bytes
