# GravelDB
[![CI](https://github.com/MikhailWahib/graveldb/actions/workflows/ci.yml/badge.svg)](https://github.com/MikhailWahib/graveldb/actions/workflows/ci.yml) [![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/MikhailWahib/graveldb)

**GravelDB** is a simple, high-performance key-value store written in Go, based on the LSM-tree (Log-Structured Merge-tree) architecture. It is optimized for high write throughput and efficient disk usage, making it suitable for write-heavy workloads and embedded database scenarios.

## Features

- **LSM-tree Architecture**: Fast writes using an in-memory memtable, with periodic flushes to immutable SSTable files on disk.
- **Write-Ahead Logging (WAL)**: Ensures durability and crash recovery.
- **Tiered Compaction**: Automatic background compaction of SSTables for efficient reads and disk space management.
- **Basic CRUD Operations**: Simple API for storing, retrieving, and deleting key-value pairs.
- **Configurable Performance**: Tune memory usage and compaction behavior via exposed settings.
- **Thread-Safe**: Safe for concurrent use.
- **MIT Licensed**: Open source and free to use.

## Getting Started

### Requirements

- **Go 1.24+** (see `go.mod` for details)

### Installation

Clone the repository:

```sh
git clone https://github.com/MikhailWahib/graveldb.git
cd graveldb
```

### Building

To build and test the project:

```sh
go build ./...
go test -race ./...
```

Or use the provided `Makefile`:

```sh
make test      # Run all tests
make clean     # Clean up compiled test binaries
```

### Usage

GravelDB is designed to be embedded in your Go application. Here is a minimal example:

```go
package main

import (
    "log"
    "github.com/MikhailWahib/graveldb"
)

func main() {
    db, err := graveldb.Open("/path/to/database")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Put a key-value pair
    if err := db.Put([]byte("key"), []byte("value")); err != nil {
        log.Printf("Put failed: %v", err)
    }

    // Get a value
    value, exists := db.Get([]byte("key"))
    if exists {
        log.Printf("Value: %s", value)
    }

    // Delete a key
    if err := db.Delete([]byte("key")); err != nil {
        log.Printf("Delete failed: %v", err)
    }
}
```

### API

- `Open(path string) (*DB, error)`: Open or create a database at the given path.
- `Put(key, value []byte) error`: Store or update a key-value pair.
- `Get(key []byte) ([]byte, bool)`: Retrieve the value for a key.
- `Delete(key []byte) error`: Remove a key and its value.
- `SetMaxMemtableSize(sizeInBytes int)`: Set the memtable flush threshold (default: 4MB).
- `SetMaxTablesPerTier(n int)`: Set the SSTable compaction threshold per tier (default: 4).
- `Close() error`: Gracefully close the database, ensuring all data is persisted.

### Configuration

You can tune GravelDB's performance by adjusting:

- **Memtable Size**: Controls how much data is buffered in memory before being flushed to disk.

  ```go
  db.SetMaxMemtableSize(8 * 1024 * 1024) // 8MB
  ```

  Default: `4 * 1024 * 1024` (4MB)

- **SSTables Per Tier**: Controls how many SSTable files are allowed per tier before compaction is triggered.
  ```go
  db.SetMaxTablesPerTier(8)
  ```
  Default: `4`

### File Structure

- `graveldb.go`: Public API and entry point.
- `internal/engine/`: Core storage engine, compaction, and configuration.
- `internal/memtable/`: In-memory table implementation.
- `internal/sstable/`: SSTable (Sorted String Table) file format and operations.
- `internal/wal/`: Write-Ahead Log for durability.
- `Makefile`: Development and test commands.
- `.github/workflows/ci.yml`: Continuous integration setup.

### Testing

Run all tests with:

```sh
go test -race ./...
```

Or use the Makefile:

```sh
make test
```

### License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
