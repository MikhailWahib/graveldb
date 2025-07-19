# GravelDB
[![CI](https://github.com/MikhailWahib/graveldb/actions/workflows/ci.yml/badge.svg)](https://github.com/MikhailWahib/graveldb/actions/workflows/ci.yml) [![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/MikhailWahib/graveldb)

**GravelDB** is a lightweight, high-throughput key-value store written in Go, based on the LSM-tree (Log-Structured Merge-tree) architecture. It targets write-heavy workloads with strong durability guarantees and low disk overhead.

## Highlights

- ⚡ **Fast writes** via in-memory memtable and WAL  
- 🧱 **Immutable SSTables** for optimized disk I/O  
- 🔄 **Tiered compaction** for space efficiency  
- 🔒 **Thread-safe** by design  
- ⚙️ **Tunable performance knobs**  
---

## Installation

**Requires Go 1.21+**

To add GravelDB to your Go project:

```sh
go get github.com/MikhailWahib/graveldb
````

To test and build it locally:

```sh
git clone https://github.com/MikhailWahib/graveldb.git
cd graveldb
make test   # or: go test -race ./...
```

## Quickstart

```go
package main

import (
	"log"

	"github.com/MikhailWahib/graveldb"
)

func main() {
	db, err := graveldb.Open("/tmp/db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.Put([]byte("foo"), []byte("bar"))

	val, ok := db.Get([]byte("foo"))
	if ok {
		log.Printf("value: %s", val)
	}

	db.Delete([]byte("foo"))
}
```

## API Overview

* `Open(path string) (*DB, error)`
* `Put(key, value []byte) error`
* `Get(key []byte) ([]byte, bool)`
* `Delete(key []byte) error`
* `SetMaxMemtableSize(n int)` — default: 4MB
* `SetMaxTablesPerTier(n int)` — default: 4
* `Close() error`

## Performance Tuning

```go
db.SetMaxMemtableSize(8 * 1024 * 1024) // 8MB
db.SetMaxTablesPerTier(8)              // compaction threshold
```

## Project Structure

* `graveldb.go` — public API
* `internal/engine/` — core storage engine
* `internal/memtable/` — skiplist-based in-memory store
* `internal/sstable/` — sorted string table persistence
* `internal/wal/` — write-ahead log
* `internal/record/` — binary encoding for entry I/O
* `Makefile` — build/test targets

## Testing

```sh
make test
# or
go test -race ./...
```