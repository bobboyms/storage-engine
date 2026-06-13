// Package bench holds the performance benchmark suite for the storage engine,
// exercised through the public SQL API (the surface consumers use). It is the
// project's equivalent of pgbench: run it on your own hardware to learn what
// the engine delivers there, and compare runs with benchstat when touching hot
// paths (WAL, B+ tree, buffer pool).
//
// Run with:
//
//	make bench
//	go test ./tests/bench -bench=. -benchmem -run='^$'
//
// Numbers from shared CI runners are too noisy to act on; benchmark on
// dedicated hardware and keep a baseline (see AGENTS.md guidance on suites).
package bench

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/sql"
)

// benchMasterKey is a fixed 32-byte KEK for the TDE variants. Benchmark-only.
var benchMasterKey = []byte("0123456789abcdef0123456789abcdef")

// openBenchDB opens a fresh database with background maintenance disabled so
// checkpoints never fire mid-measurement, optionally with TDE enabled.
func openBenchDB(b *testing.B, dir string, encrypted bool) *sql.Executor {
	b.Helper()
	opts := sql.OpenOptions{DisableMaintenance: true}
	if encrypted {
		opts.Encryption = &sql.EncryptionOptions{MasterKey: benchMasterKey}
	}
	db, err := sql.OpenDatabaseWithOptions(context.Background(), dir, opts)
	if err != nil {
		b.Fatalf("open database: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

func createUsersTable(b *testing.B, db *sql.Executor) {
	b.Helper()
	const ddl = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT INDEX)"
	if _, err := db.Exec(context.Background(), ddl); err != nil {
		b.Fatalf("create table: %v", err)
	}
}

// seedRows inserts n rows with ids [0, n) using batched transactions, so
// benchmark setup is not bound by one fsync per row.
func seedRows(b *testing.B, db *sql.Executor, n int) {
	b.Helper()
	ctx := context.Background()
	const batch = 500
	for lo := 0; lo < n; lo += batch {
		tx := db.Begin()
		for id := lo; id < lo+batch && id < n; id++ {
			if _, err := tx.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
				id, fmt.Sprintf("user-%d", id), id%100); err != nil {
				b.Fatalf("seed insert %d: %v", id, err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			b.Fatalf("seed commit at %d: %v", lo, err)
		}
	}
}

// BenchmarkInsertAutocommit measures the durable write path: each op is one
// INSERT that commits and fsyncs on its own. This is the floor for write
// latency-bound workloads; the tde variant adds AES-GCM on every flushed page
// and WAL entry.
func BenchmarkInsertAutocommit(b *testing.B) {
	for _, mode := range []struct {
		name      string
		encrypted bool
	}{{"plain", false}, {"tde", true}} {
		b.Run(mode.name, func(b *testing.B) {
			db := openBenchDB(b, b.TempDir(), mode.encrypted)
			createUsersTable(b, db)
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := db.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
					i, fmt.Sprintf("user-%d", i), i%100); err != nil {
					b.Fatalf("insert %d: %v", i, err)
				}
			}
		})
	}
}

// BenchmarkInsertAutocommitParallel measures aggregate write throughput with
// concurrent committers, which is where WAL group commit amortizes fsyncs:
// per-op time should drop well below the serial autocommit number as
// parallelism grows.
func BenchmarkInsertAutocommitParallel(b *testing.B) {
	for _, mode := range []struct {
		name      string
		encrypted bool
	}{{"plain", false}, {"tde", true}} {
		b.Run(mode.name, func(b *testing.B) {
			db := openBenchDB(b, b.TempDir(), mode.encrypted)
			createUsersTable(b, db)
			ctx := context.Background()
			var nextID atomic.Int64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := nextID.Add(1)
					if _, err := db.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
						id, fmt.Sprintf("user-%d", id), id%100); err != nil {
						b.Fatalf("insert %d: %v", id, err)
					}
				}
			})
		})
	}
}

// BenchmarkInsertTxBatch100 measures batching, the main write-throughput
// lever: 100 INSERTs staged in one transaction share a single commit fsync.
// Per-row cost = ns/op divided by 100.
func BenchmarkInsertTxBatch100(b *testing.B) {
	const batch = 100
	db := openBenchDB(b, b.TempDir(), false)
	createUsersTable(b, db)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.Begin()
		for j := 0; j < batch; j++ {
			id := i*batch + j
			if _, err := tx.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
				id, fmt.Sprintf("user-%d", id), id%100); err != nil {
				b.Fatalf("insert %d: %v", id, err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			b.Fatalf("commit batch %d: %v", i, err)
		}
	}
	b.ReportMetric(float64(batch)*float64(b.N)/b.Elapsed().Seconds(), "rows/sec")
}

// BenchmarkPointLookup measures an indexed primary-key read against a
// 10k-row table: B+ tree descent plus one heap fetch, no WAL involvement.
func BenchmarkPointLookup(b *testing.B) {
	const rows = 10_000
	for _, mode := range []struct {
		name      string
		encrypted bool
	}{{"plain", false}, {"tde", true}} {
		b.Run(mode.name, func(b *testing.B) {
			db := openBenchDB(b, b.TempDir(), mode.encrypted)
			createUsersTable(b, db)
			seedRows(b, db, rows)
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rs, err := db.Query(ctx, "SELECT name FROM users WHERE id = ?", i%rows)
				if err != nil {
					b.Fatalf("lookup %d: %v", i, err)
				}
				if len(rs.Rows) != 1 {
					b.Fatalf("lookup %d returned %d rows, want 1", i, len(rs.Rows))
				}
			}
		})
	}
}

// BenchmarkFullScan measures a full-table SELECT over 1k rows; the rows/sec
// metric is the table-scan drain rate (buffer pool + decode bound).
func BenchmarkFullScan(b *testing.B) {
	const rows = 1_000
	db := openBenchDB(b, b.TempDir(), false)
	createUsersTable(b, db)
	seedRows(b, db, rows)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := db.Query(ctx, "SELECT id, name FROM users")
		if err != nil {
			b.Fatalf("scan: %v", err)
		}
		if len(rs.Rows) != rows {
			b.Fatalf("scan returned %d rows, want %d", len(rs.Rows), rows)
		}
	}
	b.ReportMetric(float64(rows)*float64(b.N)/b.Elapsed().Seconds(), "rows/sec")
}

// BenchmarkCommitLatency reports the latency distribution (p50/p99) of a
// single-row transaction commit. Throughput benchmarks hide tail latency, and
// group commit explicitly trades individual latency for throughput, so both
// views matter.
func BenchmarkCommitLatency(b *testing.B) {
	db := openBenchDB(b, b.TempDir(), false)
	createUsersTable(b, db)
	ctx := context.Background()
	latencies := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.Begin()
		if _, err := tx.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
			i, fmt.Sprintf("user-%d", i), i%100); err != nil {
			b.Fatalf("insert %d: %v", i, err)
		}
		start := time.Now()
		if err := tx.Commit(ctx); err != nil {
			b.Fatalf("commit %d: %v", i, err)
		}
		latencies = append(latencies, time.Since(start))
	}
	b.StopTimer()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	b.ReportMetric(float64(latencies[len(latencies)/2]), "p50-ns")
	b.ReportMetric(float64(latencies[len(latencies)*99/100]), "p99-ns")
}

// BenchmarkRecovery measures crash recovery: opening a directory whose WAL
// holds committed rows that were never checkpointed, so the open must replay
// the log. This is the post-crash restart cost users see. Each iteration
// recovers a fresh copy of the crash-state directory; the copy itself runs
// outside the timer.
//
// At this volume the 16-page B+ tree buffer pools have already evicted, so
// the crash state also exercises the index-rebuild path of recovery (trees
// whose on-disk image took partial eviction flushes are rebuilt from the
// heap).
func BenchmarkRecovery(b *testing.B) {
	const rows = 5_000
	ctx := context.Background()

	// Build the crash-state seed: insert through a live handle and never close
	// it, so dirty pages are not flushed and the WAL stays the only durable
	// copy — exactly the state a kill -9 leaves behind.
	seedDir := b.TempDir()
	seedDB := openBenchDB(b, seedDir, false)
	createUsersTable(b, seedDB)
	seedRows(b, seedDB, rows)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		if err := os.CopyFS(dir, os.DirFS(seedDir)); err != nil {
			b.Fatalf("copy crash state: %v", err)
		}
		b.StartTimer()

		db, err := sql.OpenDatabaseWithOptions(ctx, dir, sql.OpenOptions{DisableMaintenance: true})
		if err != nil {
			b.Fatalf("recover: %v", err)
		}

		b.StopTimer()
		rs, err := db.Query(ctx, "SELECT id FROM users WHERE id = ?", rows-1)
		if err != nil || len(rs.Rows) != 1 {
			b.Fatalf("recovered data missing: rows=%v err=%v", rs, err)
		}
		if err := db.Close(); err != nil {
			b.Fatalf("close recovered db: %v", err)
		}
		b.StartTimer()
	}
}
