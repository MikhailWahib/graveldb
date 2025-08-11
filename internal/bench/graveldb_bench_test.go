package bench

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/MikhailWahib/graveldb"
)

var writeCfg = &graveldb.Config{
	MaxMemtableSize:   32 * 1024 * 1024,
	MaxTablesPerTier:  6,
	IndexInterval:     32,
	WALFlushThreshold: 256 * 1024,
	WALFlushInterval:  50 * time.Millisecond,
}

var readCfg = &graveldb.Config{
	MaxMemtableSize:  64 * 1024 * 1024,
	MaxTablesPerTier: 4,
	IndexInterval:    64,
}

func setupBenchDB(b *testing.B, cfg *graveldb.Config) (*graveldb.DB, func()) {
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("graveldb_bench_%d", rand.Int63()))
	db, err := graveldb.Open(tmpDir, cfg)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}

	cleanup := func() {
		_ = db.Close()
		_ = os.RemoveAll(tmpDir)
	}

	return db, cleanup
}

func generateKey(i int) []byte {
	return fmt.Appendf(nil, "key_%010d", i)
}

func generateValue(size int) []byte {
	value := make([]byte, size)
	for i := range value {
		value[i] = byte(rand.Intn(256))
	}
	return value
}

func BenchmarkWrite(b *testing.B) {
	db, cleanup := setupBenchDB(b, writeCfg)
	defer cleanup()

	value := generateValue(1024)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := generateKey(i)
		err := db.Put(key, value)
		if err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	db, cleanup := setupBenchDB(b, readCfg)
	defer cleanup()

	// Pre-populate
	value := generateValue(1024)
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		key := generateKey(i)
		err := db.Put(key, value)
		if err != nil {
			b.Fatalf("Pre-populate put failed: %v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := generateKey(i % numKeys)
		_, found := db.Get(key)
		if !found {
			b.Fatalf("key not found")
		}
	}
}

func BenchmarkRandomRead(b *testing.B) {
	db, cleanup := setupBenchDB(b, readCfg)
	defer cleanup()

	// Pre-populate
	value := generateValue(1024)
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		key := generateKey(i)
		err := db.Put(key, value)
		if err != nil {
			b.Fatalf("Pre-populate put failed: %v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := generateKey(rand.Intn(numKeys))
		_, found := db.Get(key)
		if !found {
			b.Fatalf("key not found")
		}
	}
}

func BenchmarkConcurrentRead(b *testing.B) {
	db, cleanup := setupBenchDB(b, readCfg)
	defer cleanup()

	// Pre-populate
	value := generateValue(1024)
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		key := generateKey(i)
		err := db.Put(key, value)
		if err != nil {
			b.Fatalf("Pre-populate put failed: %v", err)
		}
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := generateKey(rand.Intn(numKeys))
			_, found := db.Get(key)
			if !found {
				b.Fatalf("key not found")
			}
		}
	})
}

func BenchmarkConcurrentWrite(b *testing.B) {
	db, cleanup := setupBenchDB(b, writeCfg)
	defer cleanup()

	value := generateValue(1024)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Use unique keys to avoid collisions across goroutines
			key := fmt.Appendf(nil, "key_%d_%d", rand.Int63(), i)
			err := db.Put(key, value)
			if err != nil {
				b.Fatalf("Put failed: %v", err)
			}
			i++
		}
	})
}
