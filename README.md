# GravelDB

[![CI](https://github.com/MikhailWahib/graveldb/actions/workflows/ci.yml/badge.svg)](https://github.com/MikhailWahib/graveldb/actions/workflows/ci.yml)

GravelDB is an embedded key-value store in Go, implemented as an LSM-tree with:
- an in-memory memtable (skiplist)
- a write-ahead log (WAL)
- immutable SSTables with tiered compaction

The project is optimized for write-heavy workloads and low operational complexity.

## Requirements

- Go `1.21+`

## Installation

```bash
go get github.com/MikhailWahib/graveldb
```

## Quick Start

```go
package main

import (
	"log"

	"github.com/MikhailWahib/graveldb"
)

func main() {
	cfg := graveldb.DefaultConfig()

	db, err := graveldb.Open("/tmp/db", cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("foo"), []byte("bar")); err != nil {
		log.Fatal(err)
	}

	v, ok := db.Get([]byte("foo"))
	if ok {
		log.Printf("foo=%s", v)
	}

	if err := db.Delete([]byte("foo")); err != nil {
		log.Fatal(err)
	}
}
```

## Public API

```go
func Open(path string, cfg *graveldb.Config) (*DB, error)
func (db *DB) Put(key, value []byte) error
func (db *DB) Get(key []byte) ([]byte, bool)
func (db *DB) Delete(key []byte) error
func (db *DB) Close() error
```

Notes:
- Passing `nil` config to `Open` uses defaults.
- `Get` returns `([]byte, false)` when the key does not exist or is tombstoned.

## Architecture

### Write Path

1. Append operation to WAL buffer.
2. Insert/update entry in memtable.
3. When memtable size exceeds `MaxMemtableSize`, seal current WAL, rotate to a new WAL, and flush the sealed memtable to a new L0 SSTable.
4. If L0 table count exceeds `MaxTablesPerTier`, trigger background compaction.

### Read Path

Lookup order:
1. Active memtable
2. Immutable memtables (newest to oldest)
3. SSTables by tier, scanning newest tables first

Tombstones (deletes) shadow older values.

### Compaction Model

- Tiered compaction.
- A tier is compacted when `len(tier) > MaxTablesPerTier`.
- Compaction merges all SSTables in the tier into one SSTable in the next tier.
- Source SSTables are removed after successful merge.

## Durability and Recovery

- WAL is replayed at startup (`wal.log` and rotated `wal-*.log` files).
- WAL flush is controlled by:
  - `WALFlushThreshold` (bytes)
  - `WALFlushInterval` (duration)
- `Close()` seals/flushed remaining memtable data and waits for background work.

Durability implication:
- A successful `Put`/`Delete` means the entry is accepted into WAL memory buffer and memtable.
- Data is guaranteed on disk after WAL flush/sync or after flush to SSTable.
- Lower WAL thresholds/intervals reduce potential data loss window on crash.

## Configuration

`graveldb.Config`:

| Field | Type | Default | Effect |
| --- | --- | --- | --- |
| `MaxMemtableSize` | `int` | `32 * 1024 * 1024` | Higher values improve write throughput but use more memory and increase flush batch size. |
| `MaxTablesPerTier` | `int` | `4` | Lower values compact sooner (better read amplification, higher write amplification). |
| `IndexInterval` | `int` | `16` | Lower values create denser SST indexes (faster point lookups, larger index footprint). |
| `WALFlushThreshold` | `int` | `64 * 1024` | Larger threshold improves throughput, increases durability window. |
| `WALFlushInterval` | `time.Duration` | `10ms` | Shorter interval improves durability, may reduce throughput. |

Example tuning:

```go
cfg := graveldb.DefaultConfig()
cfg.MaxMemtableSize = 64 * 1024 * 1024
cfg.MaxTablesPerTier = 8
cfg.IndexInterval = 32
cfg.WALFlushThreshold = 128 * 1024
cfg.WALFlushInterval = 20 * time.Millisecond

// Zero values are auto-filled with defaults.
```

## Concurrency Semantics

- `DB` is safe for concurrent access.
- Writes are serialized behind a single engine mutex.
- Reads use a read lock and can proceed concurrently with other reads.
- Background flush/compaction is asynchronous; `Close()` waits for in-flight background tasks.

## On-Disk Layout

```text
<db-path>/
  wal.log
  wal-000001.log
  sstables/
    T0/
      000001.sst
    T1/
      000002.sst
```

## Development

```bash
git clone https://github.com/MikhailWahib/graveldb.git
cd graveldb
make test
# or
go test -race ./...
```

Benchmarks:

```bash
go test -bench=. ./internal/bench
```

## Project Structure

- `graveldb.go`: public API surface
- `internal/engine`: write/read orchestration, flushing, compaction
- `internal/memtable`: in-memory skiplist
- `internal/wal`: WAL append/flush/rotation/replay
- `internal/sstable`: SSTable writer/reader/merge
- `internal/storage`: binary entry encoding/decoding

## Current Scope

GravelDB currently focuses on core LSM primitives (put/get/delete + recovery).
Features such as transactions, snapshots, and external iteration APIs are not part of the public API.
