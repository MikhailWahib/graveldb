package engine

// MaxMemtableSize is the maximum size in bytes for a memtable before it is flushed to disk.
var MaxMemtableSize = 4 * 1024 * 1024

// MaxTablesPerTier is the maximum number of SSTables allowed per tier before compaction is triggered.
var MaxTablesPerTier = 4
