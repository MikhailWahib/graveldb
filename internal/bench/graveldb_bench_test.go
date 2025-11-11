package graveldb_test

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/MikhailWahib/graveldb"
)

const (
	benchKeySize   = 16
	benchValueSize = 128
	benchNumKeys   = 10000
)

// Helper function to generate random bytes
func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func makeKey(i int) []byte {
	return []byte(fmt.Sprintf("key_%016d", i))
}

func makeValue(i int) []byte {
	return []byte(fmt.Sprintf("value_%0120d", i))
}

func setupBench(b *testing.B) (*graveldb.DB, string, func()) {
	dir, err := os.MkdirTemp("", "graveldb-bench-*")
	if err != nil {
		b.Fatal(err)
	}

	cfg := graveldb.DefaultConfig()
	db, err := graveldb.Open(dir, cfg)
	if err != nil {
		os.RemoveAll(dir)
		b.Fatal(err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}

	return db, dir, cleanup
}

func BenchmarkSequentialWrites(b *testing.B) {
	db, _, cleanup := setupBench(b)
	defer cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := makeKey(i)
		value := makeValue(i)
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	totalBytes := int64(b.N) * int64(benchKeySize+benchValueSize)
	b.SetBytes(totalBytes / int64(b.N))
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

func BenchmarkRandomWrites(b *testing.B) {
	db, _, cleanup := setupBench(b)
	defer cleanup()

	// Pre-generate random keys and values
	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = randomBytes(benchKeySize)
		values[i] = randomBytes(benchValueSize)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := db.Put(keys[i], values[i]); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	b.SetBytes(int64(benchKeySize + benchValueSize))
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

func BenchmarkSequentialReads(b *testing.B) {
	db, _, cleanup := setupBench(b)
	defer cleanup()

	// Pre-populate database
	numKeys := benchNumKeys
	for i := 0; i < numKeys; i++ {
		key := makeKey(i)
		value := makeValue(i)
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := makeKey(i % numKeys)
		_, found := db.Get(key)
		if !found {
			b.Fatalf("key not found: %s", key)
		}
	}

	b.StopTimer()
	b.SetBytes(int64(benchKeySize + benchValueSize))
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

func BenchmarkRandomReads(b *testing.B) {
	db, _, cleanup := setupBench(b)
	defer cleanup()

	// Pre-populate database
	numKeys := benchNumKeys
	for i := 0; i < numKeys; i++ {
		key := makeKey(i)
		value := makeValue(i)
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}

	// Pre-generate random indices
	indices := make([]int, b.N)
	for i := 0; i < b.N; i++ {
		indices[i] = rand.Intn(numKeys)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := makeKey(indices[i])
		_, found := db.Get(key)
		if !found {
			b.Fatalf("key not found: %s", key)
		}
	}

	b.StopTimer()
	b.SetBytes(int64(benchKeySize + benchValueSize))
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

func BenchmarkMixedReadWrite(b *testing.B) {
	db, _, cleanup := setupBench(b)
	defer cleanup()

	// Pre-populate database
	numKeys := benchNumKeys
	for i := 0; i < numKeys; i++ {
		key := makeKey(i)
		value := makeValue(i)
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			// Read operation
			key := makeKey(rand.Intn(numKeys))
			db.Get(key)
		} else {
			// Write operation
			key := makeKey(numKeys + i)
			value := makeValue(numKeys + i)
			if err := db.Put(key, value); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.StopTimer()
	b.SetBytes(int64(benchKeySize + benchValueSize))
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkConcurrentReads measures concurrent read throughput
func BenchmarkConcurrentReads(b *testing.B) {
	db, _, cleanup := setupBench(b)
	defer cleanup()

	// Pre-populate database
	numKeys := benchNumKeys
	for i := 0; i < numKeys; i++ {
		key := makeKey(i)
		value := makeValue(i)
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}

	concurrency := []int{1, 2, 4, 8, 16}
	for _, workers := range concurrency {
		b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			var wg sync.WaitGroup
			opsPerWorker := b.N / workers

			for w := range workers {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					for range opsPerWorker {
						key := makeKey(rand.Intn(numKeys))
						db.Get(key)
					}
				}(w)
			}

			wg.Wait()
			b.StopTimer()
			b.SetBytes(int64(benchKeySize + benchValueSize))
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}

// BenchmarkConcurrentWrites measures concurrent write throughput
func BenchmarkConcurrentWrites(b *testing.B) {
	db, _, cleanup := setupBench(b)
	defer cleanup()

	concurrency := []int{1, 2, 4, 8, 16}
	for _, workers := range concurrency {
		b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			var wg sync.WaitGroup
			opsPerWorker := b.N / workers

			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					for i := 0; i < opsPerWorker; i++ {
						key := makeKey(workerID*opsPerWorker + i)
						value := makeValue(workerID*opsPerWorker + i)
						if err := db.Put(key, value); err != nil {
							b.Error(err)
							return
						}
					}
				}(w)
			}

			wg.Wait()
			b.StopTimer()
			b.SetBytes(int64(benchKeySize + benchValueSize))
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}

// BenchmarkConcurrentMixed measures concurrent mixed read/write workload
func BenchmarkConcurrentMixed(b *testing.B) {
	db, _, cleanup := setupBench(b)
	defer cleanup()

	numKeys := benchNumKeys
	for i := 0; i < numKeys; i++ {
		key := makeKey(i)
		value := makeValue(i)
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}

	concurrency := []int{2, 4, 8, 16}
	for _, workers := range concurrency {
		b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			var wg sync.WaitGroup
			opsPerWorker := b.N / workers

			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					for i := 0; i < opsPerWorker; i++ {
						if i%2 == 0 {
							// Read
							key := makeKey(rand.Intn(numKeys))
							db.Get(key)
						} else {
							// Write
							key := makeKey(numKeys + workerID*opsPerWorker + i)
							value := makeValue(numKeys + workerID*opsPerWorker + i)
							if err := db.Put(key, value); err != nil {
								b.Error(err)
								return
							}
						}
					}
				}(w)
			}

			wg.Wait()
			b.StopTimer()
			b.SetBytes(int64(benchKeySize + benchValueSize))
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
