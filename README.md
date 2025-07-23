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
```

### Quickstart

```go
package main

import (
	"log"
	"github.com/MikhailWahib/graveldb"
)

func main() {
	cfg := graveldb.DefaultConfig()
	// Optionally customize config:
	// cfg.MaxMemtableSize = 8 * 1024 * 1024 // 8MB
	// cfg.MaxTablesPerTier = 8

	db, err := graveldb.Open("/tmp/db", cfg)
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
Open(path string, cfg *graveldb.Config) (*DB, error)
Put(key, value []byte) error
Get(key []byte) ([]byte, bool)
Delete(key []byte) error
Close() error
```

### Tuning Performance

You can tune GravelDB's performance by customizing the `graveldb.Config` struct.

#### Configuration Fields

Below are the available fields in `graveldb.Config` and their roles:

- **MaxMemtableSize** (`int`, default: `4 * 1024 * 1024`):  
  The maximum size (in bytes) of the in-memory memtable before it is flushed to disk as an SSTable.  
  _Higher values improve write throughput but use more memory._

- **MaxTablesPerTier** (`int`, default: `4`):  
  The maximum number of SSTables allowed per tier before compaction is triggered.  
  _Lower values trigger more frequent compactions, improving read performance at the cost of more write amplification._

- **IndexInterval** (`int`, default: `16`):  
  The number of entries between index points in each SSTable.  
  _Lower values make lookups faster but increase index size._

- **WALFlushThreshold** (`int`, default: `64 * 1024`):  
  The number of bytes written to the Write-Ahead Log (WAL) before it is flushed to disk.  
  _Higher values can improve write performance but increase the risk of data loss on crash._

- **WALFlushInterval** (`time.Duration`, default: `10ms`):  
  The maximum time between WAL flushes, even if the threshold is not reached.  
  _Lower values improve durability but may reduce throughput._

---

#### How to Set Config Values

There are **two ways** to set up your config:

##### 1. Start from Defaults and Override

```go
cfg := graveldb.DefaultConfig()
cfg.MaxMemtableSize = 8 * 1024 * 1024 // 8MB memtable flush threshold
cfg.MaxTablesPerTier = 8              // Compaction threshold per tier
cfg.IndexInterval = 32                // Sparse index interval for SSTables
cfg.WALFlushThreshold = 128 * 1024       // WAL flush threshold (bytes)
cfg.WALFlushInterval = 20 * time.Millisecond // WAL flush interval

db, err := graveldb.Open("/tmp/db", cfg)
```

##### 2. Manual Construction (Partial Fields)

You can set only the fields you care about. Any unset fields will be automatically set to their default values:

```go
cfg := &graveldb.Config{
    MaxMemtableSize:  8 * 1024 * 1024,
    MaxTablesPerTier: 8,
    // Other fields can be omitted
}

db, err := graveldb.Open("/tmp/db", cfg)
```

---

#### Config Fields

| Field            | Type          | Default           | Description                           |
| ---------------- | ------------- | ----------------- | ------------------------------------- |
| MaxMemtableSize  | int           | `4 * 1024 * 1024` | Memtable flush threshold (bytes)      |
| MaxTablesPerTier | int           | `4`               | SSTable compaction threshold per tier |
| IndexInterval    | int           | `16`              | Sparse index interval for SSTables    |
| WALFlushThreshold   | int           | `64 * 1024`       | WAL flush threshold (bytes)           |
| WALFlushInterval    | time.Duration | `10ms`            | WAL flush interval                    |

---

## 🛠️ Local Development

To run or modify the code locally:

```sh
git clone https://github.com/MikhailWahib/graveldb.git
cd graveldb
make test   # or: go test -race ./...
```

### Project Layout

- `graveldb.go` – public-facing API
- `internal/engine/` – core engine logic
- `internal/memtable/` – in-memory skiplist
- `internal/sstable/` – disk-based SSTables
- `internal/wal/` – write-ahead log
- `internal/storage/` – binary encoding
- `Makefile` – build/test commands

### Testing

```sh
make test
# or
go test -race ./...
```

---

GravelDB is designed for learning and experimentation. Contributions and feedback are welcome.
