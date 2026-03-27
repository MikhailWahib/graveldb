package graveldb_test

import (
	"fmt"
	"math/rand"
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

func init() {
	rand.Seed(time.Now().UnixNano())
}

func writeBenchConfig() *graveldb.Config {
	return graveldb.DefaultConfig()
}

func readBenchConfig() *graveldb.Config {
	cfg := graveldb.DefaultConfig()
	cfg.MaxMemtableSize = 64 * 1024
	cfg.MaxTablesPerTier = 64
	return cfg
}

func openBenchDB(b *testing.B, dir string, cfg *graveldb.Config) *graveldb.DB {
	b.Helper()

	db, err := graveldb.Open(dir, cfg)
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Logf("close error: %v", err)
		}
	})

	return db
}

func makeDataset(count, start int) ([][]byte, [][]byte) {
	keys := make([][]byte, count)
	values := make([][]byte, count)

	for i := 0; i < count; i++ {
		idx := start + i
		keys[i] = []byte(fmt.Sprintf("key_%016d", idx))
		values[i] = []byte(fmt.Sprintf("value_%0120d", idx))
	}

	return keys, values
}

func makeRandomDataset(count int) ([][]byte, [][]byte) {
	keys := make([][]byte, count)
	values := make([][]byte, count)

	for i := 0; i < count; i++ {
		keys[i] = make([]byte, benchKeySize)
		values[i] = make([]byte, benchValueSize)
		_, _ = rand.Read(keys[i])
		_, _ = rand.Read(values[i])
	}

	return keys, values
}

func preloadReadDB(b *testing.B) (*graveldb.DB, [][]byte) {
	b.Helper()

	dir := b.TempDir()
	seedDB := openBenchDB(b, dir, readBenchConfig())
	keys, values := makeDataset(benchNumKeys, 0)

	for i := 0; i < benchNumKeys; i++ {
		if err := seedDB.Put(keys[i], values[i]); err != nil {
			b.Fatal(err)
		}
	}

	if err := seedDB.Close(); err != nil {
		b.Fatal(err)
	}

	db := openBenchDB(b, dir, readBenchConfig())
	return db, keys
}

func reportThroughput(b *testing.B) {
	b.Helper()
	b.SetBytes(int64(benchKeySize + benchValueSize))
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

func BenchmarkWrites(b *testing.B) {
	b.Run("Sequential", func(b *testing.B) {
		dir := b.TempDir()
		db := openBenchDB(b, dir, writeBenchConfig())
		keys, values := makeDataset(b.N, 0)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			if err := db.Put(keys[i], values[i]); err != nil {
				b.Fatal(err)
			}
		}

		reportThroughput(b)
	})

	b.Run("Random", func(b *testing.B) {
		dir := b.TempDir()
		db := openBenchDB(b, dir, writeBenchConfig())
		keys, values := makeRandomDataset(b.N)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			if err := db.Put(keys[i], values[i]); err != nil {
				b.Fatal(err)
			}
		}

		reportThroughput(b)
	})

	b.Run("Concurrent", func(b *testing.B) {
		for _, workers := range []int{1, 2, 4, 8, 16} {
			b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
				dir := b.TempDir()
				db := openBenchDB(b, dir, writeBenchConfig())

				opsPerWorker := b.N / workers
				keys := make([][][]byte, workers)
				values := make([][][]byte, workers)
				for worker := 0; worker < workers; worker++ {
					keys[worker], values[worker] = makeDataset(opsPerWorker, worker*opsPerWorker)
				}

				b.ResetTimer()
				b.ReportAllocs()

				var wg sync.WaitGroup
				for worker := 0; worker < workers; worker++ {
					wg.Add(1)
					go func(id int) {
						defer wg.Done()
						for i := 0; i < opsPerWorker; i++ {
							if err := db.Put(keys[id][i], values[id][i]); err != nil {
								b.Error(err)
								return
							}
						}
					}(worker)
				}

				wg.Wait()
				reportThroughput(b)
			})
		}
	})
}

func BenchmarkReads(b *testing.B) {
	b.Run("Sequential", func(b *testing.B) {
		db, keys := preloadReadDB(b)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			key := keys[i%len(keys)]
			if _, found := db.Get(key); !found {
				b.Fatalf("key not found: %s", key)
			}
		}

		reportThroughput(b)
	})

	b.Run("Random", func(b *testing.B) {
		db, keys := preloadReadDB(b)
		indices := make([]int, b.N)
		for i := 0; i < b.N; i++ {
			indices[i] = rand.Intn(len(keys))
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			if _, found := db.Get(keys[indices[i]]); !found {
				b.Fatalf("key not found: %s", keys[indices[i]])
			}
		}

		reportThroughput(b)
	})

	b.Run("Concurrent", func(b *testing.B) {
		for _, workers := range []int{1, 2, 4, 8, 16} {
			b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
				db, keys := preloadReadDB(b)

				opsPerWorker := b.N / workers
				indices := make([][]int, workers)
				for worker := 0; worker < workers; worker++ {
					indices[worker] = make([]int, opsPerWorker)
					for i := 0; i < opsPerWorker; i++ {
						indices[worker][i] = rand.Intn(len(keys))
					}
				}

				b.ResetTimer()
				b.ReportAllocs()

				var wg sync.WaitGroup
				for worker := 0; worker < workers; worker++ {
					wg.Add(1)
					go func(id int) {
						defer wg.Done()
						for i := 0; i < opsPerWorker; i++ {
							if _, found := db.Get(keys[indices[id][i]]); !found {
								b.Errorf("key not found: %s", keys[indices[id][i]])
								return
							}
						}
					}(worker)
				}

				wg.Wait()
				reportThroughput(b)
			})
		}
	})
}

func BenchmarkMixedWorkload(b *testing.B) {
	db, keys := preloadReadDB(b)
	writes, values := makeDataset(b.N/2+1, benchNumKeys)
	writeIdx := 0

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			_, _ = db.Get(keys[rand.Intn(len(keys))])
		} else {
			if err := db.Put(writes[writeIdx], values[writeIdx]); err != nil {
				b.Fatal(err)
			}
			writeIdx++
		}
	}

	reportThroughput(b)
}
