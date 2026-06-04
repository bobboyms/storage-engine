package sql

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSweepTempFiles(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a.tmp", "b.tmp", "data.heap", "schema.json"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	n, err := sweepTempFiles(dir, 0)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 2 {
		t.Fatalf("removed = %d, want 2", n)
	}
	// Non-temp files survive.
	if _, err := os.Stat(filepath.Join(dir, "data.heap")); err != nil {
		t.Fatalf("data.heap should survive: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "a.tmp")); !os.IsNotExist(err) {
		t.Fatalf("a.tmp should be removed")
	}
}

func TestSweepTempFilesMinAge(t *testing.T) {
	dir := t.TempDir()
	fresh := filepath.Join(dir, "fresh.tmp")
	if err := os.WriteFile(fresh, []byte("x"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	// A fresh temp file is not removed when a min age is required (avoids
	// racing an in-flight atomic write).
	if n, _ := sweepTempFiles(dir, time.Hour); n != 0 {
		t.Fatalf("removed = %d, want 0 (too fresh)", n)
	}
	// Age it past the threshold.
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(fresh, old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	if n, _ := sweepTempFiles(dir, time.Hour); n != 1 {
		t.Fatalf("removed = %d, want 1 (aged out)", n)
	}
}

func TestRunMaintenance(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabase(ctx, dir)
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY, v INT)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t (id, v) VALUES (1, 10)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Stale orphan temp file (as a crash would leave behind).
	orphan := filepath.Join(dir, "t.heap.tmp")
	if err := os.WriteFile(orphan, []byte("garbage"), 0o600); err != nil {
		t.Fatalf("write orphan: %v", err)
	}
	old := time.Now().Add(-time.Hour)
	_ = os.Chtimes(orphan, old, old)

	removed, err := db.RunMaintenance(ctx)
	if err != nil {
		t.Fatalf("RunMaintenance: %v", err)
	}
	if removed < 1 {
		t.Fatalf("removed = %d, want >= 1 (the orphan)", removed)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan temp file should be gone")
	}

	// Data remains intact and queryable after maintenance (which checkpointed
	// and pruned WAL segments).
	rs, err := db.Query(ctx, "SELECT v FROM t WHERE id = 1")
	if err != nil || len(rs.Rows) != 1 {
		t.Fatalf("query after maintenance: rows=%d err=%v", len(rs.Rows), err)
	}
}

func TestScheduledMaintenanceLifecycle(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabase(ctx, dir)
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	db.StartMaintenance(10 * time.Millisecond)
	// Starting twice is a no-op (does not start a second goroutine).
	db.StartMaintenance(10 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	// Close stops the scheduler cleanly (no hang / leaked goroutine).
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
