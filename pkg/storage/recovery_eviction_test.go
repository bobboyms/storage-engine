package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// evictionKey returns a deliberately long primary key so B+ tree leaf pages
// fill (and the 16-page index buffer pool starts evicting) after a few
// hundred rows instead of tens of thousands.
func evictionKey(i int) string {
	return fmt.Sprintf("user-%05d-%s", i, strings.Repeat("k", 32))
}

func evictionName(i int) string {
	return fmt.Sprintf("name-%05d-%s", i, strings.Repeat("n", 32))
}

// openEvictionEngine builds (or reopens) a production engine over dir with a
// "users" table carrying a unique VARCHAR primary index and a VARCHAR
// secondary index, the topology where eviction-crash recovery used to break.
func openEvictionEngine(t *testing.T, dir string) *StorageEngine {
	t.Helper()
	meta := NewTableMenager()
	hm, err := NewHeapForTable(HeapFormatV2, filepath.Join(dir, "users.heap"))
	if err != nil {
		t.Fatalf("create heap: %v", err)
	}
	if err := meta.NewTable("users", []Index{
		{Name: "id", Primary: true, Type: TypeVarchar, Unique: true},
		{Name: "name", Type: TypeVarchar},
	}, 0, hm); err != nil {
		t.Fatalf("create table: %v", err)
	}
	walWriter, err := wal.NewWALWriter(filepath.Join(dir, "data.wal"), wal.DefaultOptions())
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}
	se, err := NewProductionStorageEngine(meta, walWriter)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	return se
}

func upsertEvictionRow(t *testing.T, se *StorageEngine, i int, name string) {
	t.Helper()
	id := evictionKey(i)
	doc := fmt.Sprintf(`{"id": "%s", "name": "%s"}`, id, name)
	keys := map[string]types.Comparable{
		"id":   types.VarcharKey(id),
		"name": types.VarcharKey(name),
	}
	if err := se.UpsertRow(context.Background(), "users", doc, keys); err != nil {
		t.Fatalf("upsert row %d: %v", i, err)
	}
}

// seedEvictionCrashState fills dir with enough index data to force buffer
// pool eviction (page-redo entries in the WAL, zero-filled holes in the tree
// files), then snapshots the directory WITHOUT closing the engine — byte for
// byte the state a kill -9 leaves behind. Returns the crash dir and the
// expected number of live rows.
func seedEvictionCrashState(t *testing.T, rows int) (string, int) {
	t.Helper()
	// These tests deliberately force buffer pool eviction at low row counts,
	// so they pin the small legacy pool sizes for the duration of the test
	// (restored at test end); the production default is much larger.
	t.Cleanup(SetBufferPoolPagesForTest(64, 16))
	seedDir := t.TempDir()
	se := openEvictionEngine(t, seedDir)
	t.Cleanup(func() { _ = se.Close() })

	ctx := context.Background()
	for i := 0; i < rows; i++ {
		upsertEvictionRow(t, se, i, evictionName(i))
	}
	// Updates create version chains; deletes create tombstones. Both shapes
	// must survive the rebuild.
	updated := rows / 10
	for i := 0; i < updated; i++ {
		upsertEvictionRow(t, se, i, evictionName(i)+"-v2")
	}
	deleted := rows / 20
	for i := rows - deleted; i < rows; i++ {
		if _, err := se.Del(ctx, "users", "id", types.VarcharKey(evictionKey(i))); err != nil {
			t.Fatalf("delete row %d: %v", i, err)
		}
	}

	evicted := false
	table := usersTable(t, se)
	for _, idx := range table.GetIndices() {
		if stats, ok := idx.Tree.(interface{ Path() string }); ok {
			fi, err := os.Stat(stats.Path())
			if err != nil {
				t.Fatalf("stat tree file: %v", err)
			}
			if fi.Size() > 0 {
				evicted = true
			}
		}
	}
	if !evicted {
		t.Fatal("setup did not trigger index buffer pool eviction; raise the row count")
	}

	crashDir := t.TempDir()
	if err := os.CopyFS(crashDir, os.DirFS(seedDir)); err != nil {
		t.Fatalf("snapshot crash state: %v", err)
	}
	return crashDir, rows - deleted
}

// TestRecoverAfterCrashWithIndexEviction pins the eviction-crash recovery
// gap: a crash after index buffer pool eviction (and before any covering
// checkpoint) leaves tree files with zero-filled hole pages and mixed-vintage
// pages. Recovery used to fail with "invalid magic" or, worse, rebuild a
// cyclic leaf chain that hangs every scan. It must instead recover all
// committed rows into structurally sound indexes.
func TestRecoverAfterCrashWithIndexEviction(t *testing.T) {
	crashDir, wantRows := seedEvictionCrashState(t, 1200)

	type result struct {
		se  *StorageEngine
		err error
	}
	done := make(chan result, 1)
	go func() {
		meta := NewTableMenager()
		hm, err := NewHeapForTable(HeapFormatV2, filepath.Join(crashDir, "users.heap"))
		if err != nil {
			done <- result{nil, fmt.Errorf("create heap: %w", err)}
			return
		}
		if err := meta.NewTable("users", []Index{
			{Name: "id", Primary: true, Type: TypeVarchar, Unique: true},
			{Name: "name", Type: TypeVarchar},
		}, 0, hm); err != nil {
			done <- result{nil, fmt.Errorf("create table: %w", err)}
			return
		}
		walWriter, err := wal.NewWALWriter(filepath.Join(crashDir, "data.wal"), wal.DefaultOptions())
		if err != nil {
			done <- result{nil, fmt.Errorf("create WAL: %w", err)}
			return
		}
		se, err := NewProductionStorageEngine(meta, walWriter)
		done <- result{se, err}
	}()

	var se *StorageEngine
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("recovery of eviction-crash state failed: %v", r.err)
		}
		se = r.se
	case <-time.After(120 * time.Second):
		t.Fatal("recovery of eviction-crash state hung (cyclic index chain?)")
	}
	t.Cleanup(func() { _ = se.Close() })

	// The scan below must terminate and see every committed row exactly once.
	counted := make(chan int, 1)
	go func() { counted <- countVisibleRows(t, se, "users") }()
	select {
	case got := <-counted:
		if got != wantRows {
			t.Fatalf("recovered %d visible rows, want %d", got, wantRows)
		}
	case <-time.After(120 * time.Second):
		t.Fatal("scan of recovered table hung (cyclic index chain?)")
	}

	// Spot-check point lookups across the key range, including an updated row.
	table := usersTable(t, se)
	for _, i := range []int{0, 1, wantRows / 2, wantRows - 1} {
		if _, found, err := table.Indices["id"].Tree.Get(types.VarcharKey(evictionKey(i))); err != nil || !found {
			t.Fatalf("recovered index lookup row %d: found=%v err=%v", i, found, err)
		}
	}

	// The recovered structures must pass the integrity scrub end to end.
	report := mustVerifyTables(t, se)
	if len(report.Findings) != 0 {
		t.Fatalf("scrub of recovered state reported findings: %v", report.Findings)
	}
}

// reopenEvictionEngine reopens dir capturing the RecoveryEvent fired by the
// implicit Recover.
func reopenEvictionEngine(t *testing.T, dir string) (*StorageEngine, RecoveryEvent) {
	t.Helper()
	meta := NewTableMenager()
	hm, err := NewHeapForTable(HeapFormatV2, filepath.Join(dir, "users.heap"))
	if err != nil {
		t.Fatalf("create heap: %v", err)
	}
	if err := meta.NewTable("users", []Index{
		{Name: "id", Primary: true, Type: TypeVarchar, Unique: true},
		{Name: "name", Type: TypeVarchar},
	}, 0, hm); err != nil {
		t.Fatalf("create table: %v", err)
	}
	walWriter, err := wal.NewWALWriter(filepath.Join(dir, "data.wal"), wal.DefaultOptions())
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}
	var event RecoveryEvent
	se, err := NewProductionStorageEngineWithOptions(meta, walWriter, Options{
		Listener: EventListener{OnRecoveryComplete: func(ev RecoveryEvent) { event = ev }},
	})
	if err != nil {
		t.Fatalf("reopen engine: %v", err)
	}
	t.Cleanup(func() { _ = se.Close() })
	return se, event
}

// TestCrashWithEvictionRebuildsIndexes pins the detection criterion from the
// crash side: the eviction-crash state must be recognized and both indexes
// of the table rebuilt from the heap.
func TestCrashWithEvictionRebuildsIndexes(t *testing.T) {
	crashDir, wantRows := seedEvictionCrashState(t, 1200)
	se, event := reopenEvictionEngine(t, crashDir)
	if event.IndexesRebuilt != 2 {
		t.Fatalf("RecoveryEvent.IndexesRebuilt = %d, want 2 (primary + secondary)", event.IndexesRebuilt)
	}
	if got := countVisibleRows(t, se, "users"); got != wantRows {
		t.Fatalf("recovered %d visible rows, want %d", got, wantRows)
	}
}

// TestCleanCloseAfterEvictionSkipsIndexRebuild pins the other side of the
// criterion: a clean Close flushes everything and records a checkpoint, so a
// reopen after eviction activity must NOT pay an index rebuild.
func TestCleanCloseAfterEvictionSkipsIndexRebuild(t *testing.T) {
	t.Cleanup(SetBufferPoolPagesForTest(64, 16))
	dir := t.TempDir()
	se := openEvictionEngine(t, dir)
	for i := 0; i < 1200; i++ {
		upsertEvictionRow(t, se, i, evictionName(i))
	}
	if err := se.Close(); err != nil {
		t.Fatalf("clean close: %v", err)
	}

	reopened, event := reopenEvictionEngine(t, dir)
	if event.IndexesRebuilt != 0 {
		t.Fatalf("RecoveryEvent.IndexesRebuilt = %d after clean close, want 0", event.IndexesRebuilt)
	}
	if got := countVisibleRows(t, reopened, "users"); got != 1200 {
		t.Fatalf("reopened table has %d visible rows, want 1200", got)
	}
}

// TestRecoverAfterCrashWithHeapEviction drives eviction through the heap (64
// pages / 512KB) with large documents: the crash state can hold heap files
// with zero-filled hole pages and live duplicate versions whose delete marks
// never flushed. Recovery must heal the holes, resolve the duplicates by
// CreateLSN, and end structurally sound.
func TestRecoverAfterCrashWithHeapEviction(t *testing.T) {
	t.Cleanup(SetBufferPoolPagesForTest(64, 16))
	seedDir := t.TempDir()
	se := openEvictionEngine(t, seedDir)
	t.Cleanup(func() { _ = se.Close() })

	rows := 400
	pad := strings.Repeat("x", 3000)
	put := func(i int, rev string) {
		t.Helper()
		id := fmt.Sprintf("user-%05d", i)
		name := fmt.Sprintf("name-%05d%s", i, rev)
		doc := fmt.Sprintf(`{"id": "%s", "name": "%s", "pad": "%s"}`, id, name, pad)
		keys := map[string]types.Comparable{
			"id":   types.VarcharKey(id),
			"name": types.VarcharKey(name),
		}
		if err := se.UpsertRow(context.Background(), "users", doc, keys); err != nil {
			t.Fatalf("upsert row %d: %v", i, err)
		}
	}
	for i := 0; i < rows; i++ {
		put(i, "")
	}
	for i := 0; i < rows/4; i++ {
		put(i, "-v2") // version chains crossing the eviction boundary
	}

	heapStat, err := os.Stat(filepath.Join(seedDir, "users.heap"))
	if err != nil {
		t.Fatalf("stat heap: %v", err)
	}
	if heapStat.Size() == 0 {
		t.Fatal("setup did not trigger heap eviction; raise the row count or doc size")
	}

	crashDir := t.TempDir()
	if err := os.CopyFS(crashDir, os.DirFS(seedDir)); err != nil {
		t.Fatalf("snapshot crash state: %v", err)
	}

	recovered, _ := reopenEvictionEngine(t, crashDir)
	if got := countVisibleRows(t, recovered, "users"); got != rows {
		t.Fatalf("recovered %d visible rows, want %d", got, rows)
	}
	report := mustVerifyTables(t, recovered)
	if len(report.Findings) != 0 {
		t.Fatalf("scrub of recovered state reported findings: %v", report.Findings)
	}
}
