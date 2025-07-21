package bench

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb"
)

func setupBenchDB(b *testing.B) (*graveldb.DB, func()) {
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("graveldb_bench_%d", rand.Int63()))

	db, err := graveldb.Open(tmpDir)
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
	db, cleanup := setupBenchDB(b)
	defer cleanup()

	db.SetMaxMemtableSize(64 * 1024 * 1024)

	value := generateValue(1024)

	for i := 0; b.Loop(); i++ {
		key := generateKey(i)
		err := db.Put(key, value)
		if err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	db, cleanup := setupBenchDB(b)
	defer cleanup()

	db.SetMaxMemtableSize(64 * 1024 * 1024)

	// Pre-populate
	value := generateValue(1024)
	numKeys := 10000
	for i := range numKeys {
		key := generateKey(i)
		_ = db.Put(key, value)
	}

	for i := 0; b.Loop(); i++ {
		key := generateKey(i % numKeys)
		db.Get(key)
	}
}

func BenchmarkRandomRead(b *testing.B) {
	db, cleanup := setupBenchDB(b)
	defer cleanup()

	db.SetMaxMemtableSize(64 * 1024 * 1024)

	// Pre-populate
	value := generateValue(1024)
	numKeys := 10000
	for i := range numKeys {
		key := generateKey(i)
		_ = db.Put(key, value)
	}

	for b.Loop() {
		key := generateKey(rand.Intn(numKeys))
		db.Get(key)
	}
}

func BenchmarkRandomWrite(b *testing.B) {
	db, cleanup := setupBenchDB(b)
	defer cleanup()

	db.SetMaxMemtableSize(64 * 1024 * 1024)

	value := generateValue(1024)

	for b.Loop() {
		key := fmt.Appendf(nil, "key_%d", rand.Int())
		err := db.Put(key, value)
		if err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

func BenchmarkConcurrentRead(b *testing.B) {
	db, cleanup := setupBenchDB(b)
	defer cleanup()

	db.SetMaxMemtableSize(64 * 1024 * 1024)

	// Pre-populate
	value := generateValue(1024)
	numKeys := 10000
	for i := range numKeys {
		key := generateKey(i)
		_ = db.Put(key, value)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := generateKey(rand.Intn(numKeys))
			db.Get(key)
		}
	})
}

func BenchmarkConcurrentWrite(b *testing.B) {
	db, cleanup := setupBenchDB(b)
	defer cleanup()

	db.SetMaxMemtableSize(64 * 1024 * 1024)

	value := generateValue(1024)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Appendf(nil, "key_%d_%d", b.N, i)
			err := db.Put(key, value)
			if err != nil {
				b.Fatalf("Put failed: %v", err)
			}
			i++
		}
	})
}
