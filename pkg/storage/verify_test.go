package storage

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"testing"

	heapv2 "github.com/bobboyms/storage-engine/pkg/heap/v2"
	"github.com/bobboyms/storage-engine/pkg/pagestore"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func findingWithCode(findings []VerifyFinding, code string) (VerifyFinding, bool) {
	for _, f := range findings {
		if f.Code == code {
			return f, true
		}
	}
	return VerifyFinding{}, false
}

func TestVerifyWALFile_HealthyLogIsClean(t *testing.T) {
	dir := t.TempDir()
	se := openTwoTableEngine(t, dir)
	for i := 0; i < 3; i++ {
		insertVarcharRow(t, se, "a", fmt.Sprintf("a-%d", i))
	}
	if err := se.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	findings := VerifyWALFile(filepath.Join(dir, "data.wal"), nil)
	if len(findings) != 0 {
		t.Fatalf("healthy WAL reported findings: %v", findings)
	}
}

func TestVerifyWALFile_FlagsPoisonedEntryAndCheckpoint(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "data.wal")
	writer, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	payload, err := serializePageRedoPayload("ghost.heap", 1, &pagestore.Page{})
	if err != nil {
		t.Fatalf("build payload: %v", err)
	}
	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = wal.WALVersion
	entry.Header.EntryType = wal.EntryPageRedo
	entry.Header.LSN = math.MaxUint64
	entry.Header.PayloadLen = uint32(len(payload)) //nolint:gosec // small test payload
	entry.Header.CRC32 = wal.CalculateCRC32(payload)
	entry.Payload = append(entry.Payload, payload...)
	if err := writer.WriteEntry(entry); err != nil {
		t.Fatalf("write poisoned entry: %v", err)
	}
	wal.ReleaseEntry(entry)
	if err := writer.WriteCheckpointRecord(math.MaxUint64); err != nil {
		t.Fatalf("write poisoned checkpoint: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	findings := VerifyWALFile(walPath, nil)
	if f, ok := findingWithCode(findings, "wal_poisoned_lsn"); !ok {
		t.Fatalf("missing wal_poisoned_lsn finding, got %v", findings)
	} else if f.Severity != VerifyError {
		t.Fatalf("wal_poisoned_lsn severity = %s, want error", f.Severity)
	}
	if _, ok := findingWithCode(findings, "wal_checkpoint_poisoned"); !ok {
		t.Fatalf("missing wal_checkpoint_poisoned finding, got %v", findings)
	}
}

// newVerifyEngine builds an engine over dir with one table "users" carrying a
// primary index "id" and a secondary index "name", plus a few rows, an
// update, and a delete — the shapes the verifier must accept as healthy.
func newVerifyEngine(t *testing.T, dir string) *StorageEngine {
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
	t.Cleanup(func() { _ = se.Close() })

	ctx := context.Background()
	put := func(id, name string) {
		t.Helper()
		doc := fmt.Sprintf(`{"id": "%s", "name": "%s"}`, id, name)
		keys := map[string]types.Comparable{
			"id":   types.VarcharKey(id),
			"name": types.VarcharKey(name),
		}
		if err := se.UpsertRow(ctx, "users", doc, keys); err != nil {
			t.Fatalf("upsert %s: %v", id, err)
		}
	}
	for i := 0; i < 4; i++ {
		put(fmt.Sprintf("u-%d", i), fmt.Sprintf("n-%d", i))
	}
	put("u-1", "n-1-renamed") // update: version chain + stale secondary entry
	if _, err := se.Del(ctx, "users", "id", types.VarcharKey("u-3")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	return se
}

func usersTable(t *testing.T, se *StorageEngine) *Table {
	t.Helper()
	table, err := se.TableMetaData.GetTableByName("users")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	return table
}

func mustVerifyTables(t *testing.T, se *StorageEngine) *VerifyReport {
	t.Helper()
	report, err := VerifyTables(context.Background(), se.TableMetaData, nil)
	if err != nil {
		t.Fatalf("VerifyTables: %v", err)
	}
	return report
}

func TestVerifyTables_HealthyEngineIsClean(t *testing.T) {
	se := newVerifyEngine(t, t.TempDir())
	report := mustVerifyTables(t, se)
	if len(report.Findings) != 0 {
		t.Fatalf("healthy engine reported findings: %v", report.Findings)
	}
	if report.Tables != 1 || report.Indexes != 2 {
		t.Fatalf("report counts = %d tables / %d indexes, want 1 / 2", report.Tables, report.Indexes)
	}
	if report.LiveRows != 3 {
		t.Fatalf("report.LiveRows = %d, want 3 (4 inserted, 1 deleted)", report.LiveRows)
	}
}

func TestVerifyTables_FlagsDanglingIndexPointer(t *testing.T) {
	se := newVerifyEngine(t, t.TempDir())
	table := usersTable(t, se)

	bogus := heapv2.EncodeRecordID(99, 3)
	if err := table.Indices["id"].Tree.Replace(types.VarcharKey("u-0"), bogus); err != nil {
		t.Fatalf("corrupt index pointer: %v", err)
	}

	report := mustVerifyTables(t, se)
	if _, ok := findingWithCode(report.Findings, "index_dangling_pointer"); !ok {
		t.Fatalf("missing index_dangling_pointer finding, got %v", report.Findings)
	}
	if !report.HasErrors() {
		t.Fatal("report.HasErrors() = false with a dangling pointer")
	}
}

func TestVerifyTables_FlagsKeyMismatch(t *testing.T) {
	se := newVerifyEngine(t, t.TempDir())
	table := usersTable(t, se)

	// Point u-0's primary entry at u-2's row: the resolved document no longer
	// matches the index key.
	otherOffset, found, err := table.Indices["id"].Tree.Get(types.VarcharKey("u-2"))
	if err != nil || !found {
		t.Fatalf("get u-2 offset: found=%v err=%v", found, err)
	}
	if err := table.Indices["id"].Tree.Replace(types.VarcharKey("u-0"), otherOffset); err != nil {
		t.Fatalf("corrupt index pointer: %v", err)
	}

	report := mustVerifyTables(t, se)
	if _, ok := findingWithCode(report.Findings, "index_key_mismatch"); !ok {
		t.Fatalf("missing index_key_mismatch finding, got %v", report.Findings)
	}
}

func TestVerifyTables_FlagsRowCountMismatchAcrossIndexes(t *testing.T) {
	se := newVerifyEngine(t, t.TempDir())
	table := usersTable(t, se)

	// Drop one live entry from the secondary index: it now sees fewer rows
	// than the primary.
	physical := types.NewCompositeKey(types.VarcharKey("n-0"), types.VarcharKey("u-0"))
	if _, err := table.Indices["name"].Tree.Remove(physical); err != nil {
		t.Fatalf("remove secondary entry: %v", err)
	}

	report := mustVerifyTables(t, se)
	if _, ok := findingWithCode(report.Findings, "index_row_count_mismatch"); !ok {
		t.Fatalf("missing index_row_count_mismatch finding, got %v", report.Findings)
	}
}

func TestVerifyWALFile_MissingFileIsWarning(t *testing.T) {
	findings := VerifyWALFile(filepath.Join(t.TempDir(), "data.wal"), nil)
	f, ok := findingWithCode(findings, "wal_missing")
	if !ok {
		t.Fatalf("missing wal_missing finding, got %v", findings)
	}
	if f.Severity != VerifyWarning {
		t.Fatalf("wal_missing severity = %s, want warning", f.Severity)
	}
}
