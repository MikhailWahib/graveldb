package shared

const (
	EntryTypeSize = 1
	LengthSize    = 4
	PrefixSize    = EntryTypeSize + (2 * LengthSize) // 9 bytes
)
