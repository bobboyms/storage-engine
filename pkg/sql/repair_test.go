package sql

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"

	heapv2 "github.com/bobboyms/storage-engine/pkg/heap/v2"
	"github.com/bobboyms/storage-engine/pkg/pagestore"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// poisonDirWAL appends a sentinel-LSN page-redo entry to a closed database's
// WAL, the shape a pre-clamp binary left behind.
func poisonDirWAL(t *testing.T, dir string) {
	t.Helper()
	writer, err := wal.NewWALWriter(filepath.Join(dir, "data.wal"), wal.DefaultOptions())
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = wal.WALVersion
	entry.Header.EntryType = wal.EntryPageRedo
	entry.Header.LSN = math.MaxUint64
	payload := make([]byte, 2+8+pagestore.PageSize)
	entry.Header.PayloadLen = uint32(len(payload)) //nolint:gosec // bounded test payload
	entry.Header.CRC32 = wal.CalculateCRC32(payload)
	entry.Payload = append(entry.Payload, payload...)
	if err := writer.WriteEntry(entry); err != nil {
		t.Fatalf("write poisoned entry: %v", err)
	}
	wal.ReleaseEntry(entry)
	if err := writer.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}
}

// poisonHeapPage deletes one live row directly on the closed database's heap
// and vacuums it with the raw idle sentinel, stamping the page's PageLSN with
// MaxUint64 the way a pre-clamp vacuum did.
func poisonHeapPage(t *testing.T, dir, table string) {
	t.Helper()
	ctx := context.Background()
	h, err := heapv2.NewHeapV2(filepath.Join(dir, table+".heap"), 16, nil)
	if err != nil {
		t.Fatalf("open heap: %v", err)
	}
	defer func() { _ = h.Close() }()

	victim := int64(-1)
	err = h.ForEachRecord(ctx, func(rid int64, rh heapv2.RecordHeader, _ []byte) error {
		if victim == -1 && rh.Valid {
			victim = rid
		}
		return nil
	})
	if err != nil || victim == -1 {
		t.Fatalf("find victim row: rid=%d err=%v", victim, err)
	}
	if err := h.Delete(victim, math.MaxUint64-1); err != nil {
		t.Fatalf("tombstone victim: %v", err)
	}
	reclaimed, err := h.Vacuum(ctx, math.MaxUint64)
	if err != nil || reclaimed == 0 {
		t.Fatalf("vacuum: reclaimed=%d err=%v", reclaimed, err)
	}
	if err := h.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func TestRepairDir_FixesPoisonedWALMissingIndexAndStampedHeap(t *testing.T) {
	ctx := context.Background()
	dir := buildVerifyDir(t)

	poisonDirWAL(t, dir)
	poisonHeapPage(t, dir, "sessions")
	missingIndex := storage.IndexFilePath(filepath.Join(dir, "users.heap"), "users", "name")
	if err := os.Remove(missingIndex); err != nil {
		t.Fatalf("remove index file: %v", err)
	}

	report, err := RepairDir(ctx, dir, VerifyOptions{})
	if err != nil {
		t.Fatalf("RepairDir: %v", err)
	}
	if !report.Before.HasErrors() {
		t.Fatal("before-report has no errors; setup did not corrupt the directory")
	}
	if report.After.HasErrors() {
		t.Fatalf("after-report still has errors: %v", report.After.Findings)
	}
	if !report.Repaired() {
		t.Fatal("report.Repaired() = false")
	}
	if len(report.Actions) == 0 {
		t.Fatal("no repair actions recorded")
	}

	// The repaired directory must open and answer correctly: users intact (4
	// rows, secondary rebuilt), sessions lost one row to the direct delete.
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("open repaired database: %v", err)
	}
	defer func() { _ = db.Close() }()
	counts := map[string]int{"users": 4, "sessions": 2}
	for table, want := range counts {
		rs, err := db.Query(ctx, "SELECT id FROM "+table)
		if err != nil {
			t.Fatalf("select %s: %v", table, err)
		}
		if len(rs.Rows) != want {
			t.Fatalf("%s has %d rows after repair, want %d", table, len(rs.Rows), want)
		}
	}
	// The rebuilt secondary index must serve queries.
	rs, err := db.Query(ctx, "SELECT id FROM users WHERE name = 'renamed'")
	if err != nil {
		t.Fatalf("select by rebuilt index: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rebuilt index returned %d rows, want 1", len(rs.Rows))
	}
}

func TestRepairDir_CleanDirectoryIsNoop(t *testing.T) {
	dir := buildVerifyDir(t)
	report, err := RepairDir(context.Background(), dir, VerifyOptions{})
	if err != nil {
		t.Fatalf("RepairDir: %v", err)
	}
	if len(report.Actions) != 0 {
		t.Fatalf("clean directory triggered actions: %v", report.Actions)
	}
	if report.Before.HasErrors() || report.After.HasErrors() {
		t.Fatal("clean directory reported errors")
	}
	if report.Repaired() {
		t.Fatal("Repaired() = true for a directory that needed no repair")
	}
}

// TestRepairDir_MissingHeapIsUnrepairable pins the policy for lost heaps: the
// heap is the source of truth, so there is nothing to rebuild from — the
// repair must NOT fabricate an empty heap and silently declare success.
func TestRepairDir_MissingHeapIsUnrepairable(t *testing.T) {
	dir := buildVerifyDir(t)
	heapPath := filepath.Join(dir, "users.heap")
	if err := os.Remove(heapPath); err != nil {
		t.Fatalf("remove heap: %v", err)
	}

	report, err := RepairDir(context.Background(), dir, VerifyOptions{})
	if err != nil {
		t.Fatalf("RepairDir: %v", err)
	}
	if report.Repaired() {
		t.Fatal("Repaired() = true for an unrepairable directory")
	}
	if report.After.HasErrors() == false {
		t.Fatal("after-report lost the catalog_missing_file error")
	}
	if _, err := os.Stat(heapPath); !os.IsNotExist(err) {
		t.Fatalf("repair fabricated a heap file (stat err=%v)", err)
	}
}
