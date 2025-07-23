// Package graveldb is a simple key-value store based on LSM-tree architecture.
//
// GravelDB is optimized for high write throughput and efficient disk usage.
// It uses an in-memory memtable for fast writes and periodically flushes data
// to immutable SSTable files. The system supports basic CRUD operations and
// exposes knobs for tuning memory usage and compaction behavior.
//
// Example usage:
//
//	db, err := graveldb.Open("/path/to/database")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer db.Close()
//
//	err = db.Set([]byte("key"), []byte("value"))
//	if err != nil {
//		log.Printf("Set failed: %v", err)
//	}
//
//	value, exists := db.Get([]byte("key"))
//	if exists {
//		fmt.Printf("Value: %s\n", string(value))
//	}
//
//	err = db.Delete([]byte("key"))
//	if err != nil {
//		log.Printf("Delete failed: %v", err)
//	}
package graveldb

import (
	"github.com/MikhailWahib/graveldb/internal/config"
	"github.com/MikhailWahib/graveldb/internal/engine"
)

// Config is an alias for config.Config, re-exported for user convenience.
type Config = config.Config

// DefaultConfig returns a Config struct populated with default values. Re-exported for user convenience.
var DefaultConfig = config.DefaultConfig

// DB represents a thread-safe GravelDB instance.
// It provides methods for storing, retrieving, and deleting key-value pairs,
// as well as configuration options for tuning performance.
type DB struct {
	engine *engine.Engine
}

// Open opens or creates a Graveldb database at the specified path.
//
// The directory will be created if it doesn't exist. If the database exists,
// it will be opened with its data loaded.
//
// Returns a DB instance or an error if the database can't be opened.
func Open(path string, cfg *config.Config) (*DB, error) {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}
	e := engine.NewEngine(cfg)
	if err := e.OpenDB(path); err != nil {
		return nil, err
	}
	return &DB{engine: e}, nil
}

// Set writes a key-value pair to the database.
// Overwrites the value if the key already exists.
//
// Both key and value must be non-nil. Returns an error if the operation fails.
func (db *DB) Set(key, value []byte) error {
	return db.engine.Set(key, value)
}

// Get retrieves the value for a given key.
// Returns the value and true if found, or nil and false if the key doesn't exist.
func (db *DB) Get(key []byte) ([]byte, bool) {
	return db.engine.Get(key)
}

// Delete removes the key and its value from the database.
// Returns an error only if the deletion fails.
func (db *DB) Delete(key []byte) error {
	return db.engine.Delete(key)
}

// Close gracefully shuts down the database, ensuring all data is persisted.
// This method flushes any remaining memtable data to disk and closes all
// open files. After calling Close, the database should not be used for
// any operations.
//
// It's recommended to call Close when you're done with the database,
// typically using defer:
//
//	db, err := graveldb.Open("/path/to/database")
//	if err != nil {
//		return err
//	}
//	defer db.Close()
//
// Returns an error if any cleanup operation fails.
func (db *DB) Close() error {
	return db.engine.Close()
}
