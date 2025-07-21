# GravelDB

[![CI](https://github.com/MikhailWahib/graveldb/actions/workflows/ci.yml/badge.svg)](https://github.com/MikhailWahib/graveldb/actions/workflows/ci.yml)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/MikhailWahib/graveldb)

**GravelDB** is a lightweight, high-throughput key-value store written in Go. It’s built on the LSM-tree (Log-Structured Merge-tree) architecture and is optimized for write-heavy workloads with strong durability and low disk overhead.

---

## 🚀 Highlights

- ⚡ Fast writes via in-memory memtable + WAL  
- 🧱 Immutable SSTables for optimized reads  
- 🔄 Tiered compaction for efficient storage  
- 🔒 Thread-safe by default  
- ⚙️ Configurable tuning parameters  

---

## 📦 Package Usage

**Go 1.21+ required**

Add GravelDB to your Go project:

```sh
go get github.com/MikhailWahib/graveldb
````

### Quickstart

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

### API Overview

```go
Open(path string) (*DB, error)
Put(key, value []byte) error
Get(key []byte) ([]byte, bool)
Delete(key []byte) error
SetMaxMemtableSize(n int)        // Default: 4MB
SetMaxTablesPerTier(n int)       // Default: 4
Close() error
```

### Tuning Performance

```go
db.SetMaxMemtableSize(8 * 1024 * 1024) // 8MB
db.SetMaxTablesPerTier(8)              // Compaction threshold
```

---

## 🛠️ Local Development

To run or modify the code locally:

```sh
git clone https://github.com/MikhailWahib/graveldb.git
cd graveldb
make test   # or: go test -race ./...
```

### Project Layout

* `graveldb.go` – public-facing API
* `internal/engine/` – core engine logic
* `internal/memtable/` – in-memory skiplist
* `internal/sstable/` – disk-based SSTables
* `internal/wal/` – write-ahead log
* `internal/record/` – binary encoding
* `Makefile` – build/test commands

### Testing

```sh
make test
# or
go test -race ./...
```

---

GravelDB is designed for learning and experimentation. Contributions and feedback are welcome.