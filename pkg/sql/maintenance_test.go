package sql

import (
	"context"
	"fmt"
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
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
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

func TestOpenDatabaseAutoMaintenanceDefault(t *testing.T) {
	ctx := context.Background()

	db, err := OpenDatabase(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()
	if db.maint == nil {
		t.Fatal("expected maintenance to auto-start by default")
	}

	off, err := OpenDatabaseWithOptions(ctx, t.TempDir(), OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabaseWithOptions: %v", err)
	}
	defer off.Close()
	if off.maint != nil {
		t.Fatal("expected maintenance disabled when DisableMaintenance is set")
	}
}

func TestMaintenanceActivityGating(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	mustExec(t, db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10)")

	// A write happened, so the next pass checkpoints.
	if _, err := db.RunMaintenance(ctx); err != nil {
		t.Fatalf("RunMaintenance 1: %v", err)
	}
	c1 := db.engine.Stats().Checkpoints
	if c1 == 0 {
		t.Fatal("expected a checkpoint after writes")
	}

	// No writes since the last checkpoint -> the pass is gated (no checkpoint).
	if _, err := db.RunMaintenance(ctx); err != nil {
		t.Fatalf("RunMaintenance 2: %v", err)
	}
	if c2 := db.engine.Stats().Checkpoints; c2 != c1 {
		t.Fatalf("idle pass checkpointed: got %d, want %d", c2, c1)
	}

	// A new write re-arms the checkpoint.
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (2, 20)")
	if _, err := db.RunMaintenance(ctx); err != nil {
		t.Fatalf("RunMaintenance 3: %v", err)
	}
	if c3 := db.engine.Stats().Checkpoints; c3 != c1+1 {
		t.Fatalf("checkpoint after new write: got %d, want %d", c3, c1+1)
	}
}

func mustExec(t *testing.T, db *Executor, query string) {
	t.Helper()
	if _, err := db.Exec(context.Background(), query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func TestMaintenanceVacuumsAfterDelete(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	mustExec(t, db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	for i := 1; i <= 5; i++ {
		mustExec(t, db, "INSERT INTO t (id, v) VALUES ("+itoa(i)+", "+itoa(i*10)+")")
	}
	mustExec(t, db, "DELETE FROM t WHERE id <= 3")

	before := db.engine.Stats().VacuumRuns
	if _, err := db.RunMaintenance(ctx); err != nil {
		t.Fatalf("RunMaintenance: %v", err)
	}
	st := db.engine.Stats()
	if st.VacuumRuns <= before {
		t.Fatalf("VacuumRuns = %d, want > %d (delete created dead space)", st.VacuumRuns, before)
	}
	if st.VacuumReclaimed == 0 {
		t.Fatal("VacuumReclaimed = 0, want > 0 after deleting rows")
	}

	// Surviving rows are intact.
	rs, _ := db.Query(ctx, "SELECT id FROM t ORDER BY id")
	ids := intColumn(t, rs, "id")
	if len(ids) != 2 || ids[0] != 4 || ids[1] != 5 {
		t.Fatalf("ids after delete+vacuum = %v, want [4 5]", ids)
	}

	// A second pass with no new garbage does not vacuum again.
	runs := db.engine.Stats().VacuumRuns
	if _, err := db.RunMaintenance(ctx); err != nil {
		t.Fatalf("RunMaintenance 2: %v", err)
	}
	if db.engine.Stats().VacuumRuns != runs {
		t.Fatalf("vacuum ran with no new garbage: %d -> %d", runs, db.engine.Stats().VacuumRuns)
	}
}

func TestMaintenanceSkipsVacuumWhenInsertOnly(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	mustExec(t, db, "CREATE TABLE t (id INT PRIMARY KEY)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")

	if _, err := db.RunMaintenance(ctx); err != nil {
		t.Fatalf("RunMaintenance: %v", err)
	}
	if runs := db.engine.Stats().VacuumRuns; runs != 0 {
		t.Fatalf("VacuumRuns = %d, want 0 (insert-only has no dead space)", runs)
	}
}

func itoa(n int) string {
	return fmt.Sprintf("%d", n)
}
