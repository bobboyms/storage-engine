package storage

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// buildWriteRowEngine creates (or reopens) an engine over the given paths with
// a users table indexed by id (primary) and age (secondary).
func buildWriteRowEngine(t *testing.T, heapPath, walPath string) *StorageEngine {
	t.Helper()
	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("heap: %v", err)
	}
	tm := NewTableMenager()
	if err := tm.NewTable("users", []Index{
		{Name: "id", Primary: true, Type: TypeInt},
		{Name: "age", Type: TypeInt},
	}, 3, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	opts := wal.DefaultOptions()
	opts.SyncPolicy = wal.SyncBatch
	ww, err := wal.NewWALWriter(walPath, opts)
	if err != nil {
		t.Fatalf("wal: %v", err)
	}
	se, err := NewStorageEngine(tm, ww)
	if err != nil {
		ww.Close()
		t.Fatalf("engine: %v", err)
	}
	return se
}

func writeRowKeys(id, age int) map[string]types.Comparable {
	return map[string]types.Comparable{"id": types.IntKey(id), "age": types.IntKey(age)}
}

func TestWriteRow_CommitVisibleOnAllIndexes(t *testing.T) {
	tmp := t.TempDir()
	se := buildWriteRowEngine(t, filepath.Join(tmp, "h"), filepath.Join(tmp, "w"))
	defer se.Close()

	ctx := context.Background()
	tx := se.BeginWriteTransaction()
	if err := tx.WriteRow(ctx, "users", `{"id":1,"name":"alice","age":30}`, writeRowKeys(1, 30), true); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Primary lookup.
	doc, found, err := getDocString(t, se, "users", "id", types.IntKey(1))
	if err != nil || !found {
		t.Fatalf("primary lookup: found=%v err=%v", found, err)
	}
	if doc == "" {
		t.Fatal("primary lookup returned empty doc")
	}

	// Secondary index must point at the same row.
	res, err := scanRangeDocs(t, se, "users", "age", types.IntKey(30), types.IntKey(30))
	if err != nil {
		t.Fatalf("secondary scan: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("secondary scan returned %d rows, want 1", len(res))
	}
}

func TestWriteRow_RollbackDiscards(t *testing.T) {
	tmp := t.TempDir()
	se := buildWriteRowEngine(t, filepath.Join(tmp, "h"), filepath.Join(tmp, "w"))
	defer se.Close()

	ctx := context.Background()
	tx := se.BeginWriteTransaction()
	if err := tx.WriteRow(ctx, "users", `{"id":2,"name":"bob","age":25}`, writeRowKeys(2, 25), true); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	if _, found, _ := getDocString(t, se, "users", "id", types.IntKey(2)); found {
		t.Fatal("row visible after rollback")
	}
}

func TestWriteRow_UpsertReplaces(t *testing.T) {
	tmp := t.TempDir()
	se := buildWriteRowEngine(t, filepath.Join(tmp, "h"), filepath.Join(tmp, "w"))
	defer se.Close()

	ctx := context.Background()
	tx1 := se.BeginWriteTransaction()
	if err := tx1.WriteRow(ctx, "users", `{"id":1,"v":"old","age":30}`, writeRowKeys(1, 30), true); err != nil {
		t.Fatalf("WriteRow old: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Commit old: %v", err)
	}

	tx2 := se.BeginWriteTransaction()
	if err := tx2.WriteRow(ctx, "users", `{"id":1,"v":"new","age":30}`, writeRowKeys(1, 30), false); err != nil {
		t.Fatalf("WriteRow new: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit new: %v", err)
	}

	doc, found, err := getDocString(t, se, "users", "id", types.IntKey(1))
	if err != nil || !found {
		t.Fatalf("lookup: found=%v err=%v", found, err)
	}
	if want := "new"; !strings.Contains(doc, want) {
		t.Fatalf("doc = %q, want it to contain %q", doc, want)
	}
}

func TestWriteRow_DuplicateKeyInsertOnly(t *testing.T) {
	tmp := t.TempDir()
	se := buildWriteRowEngine(t, filepath.Join(tmp, "h"), filepath.Join(tmp, "w"))
	defer se.Close()

	ctx := context.Background()
	tx1 := se.BeginWriteTransaction()
	if err := tx1.WriteRow(ctx, "users", `{"id":1,"age":30}`, writeRowKeys(1, 30), true); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	tx2 := se.BeginWriteTransaction()
	if err := tx2.WriteRow(ctx, "users", `{"id":1,"age":40}`, writeRowKeys(1, 40), true); err == nil {
		t.Fatal("expected duplicate key error for insertOnly on existing key")
	}
	_ = tx2.Rollback(ctx)
}

func TestWriteRow_RecoveryRedoesCommitted(t *testing.T) {
	tmp := t.TempDir()
	heapPath := filepath.Join(tmp, "h")
	walPath := filepath.Join(tmp, "w")

	se := buildWriteRowEngine(t, heapPath, walPath)
	ctx := context.Background()
	tx := se.BeginWriteTransaction()
	if err := tx.WriteRow(ctx, "users", `{"id":7,"name":"gwen","age":33}`, writeRowKeys(7, 33), true); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	_ = se.WAL.Sync()
	se.Close()

	se2 := buildWriteRowEngine(t, heapPath, walPath)
	defer se2.Close()
	if err := se2.Recover(ctx, walPath); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if _, found, err := getDocString(t, se2, "users", "id", types.IntKey(7)); err != nil || !found {
		t.Fatalf("committed row missing after recovery: found=%v err=%v", found, err)
	}
	res, err := scanRangeDocs(t, se2, "users", "age", types.IntKey(33), types.IntKey(33))
	if err != nil || len(res) != 1 {
		t.Fatalf("secondary index missing after recovery: rows=%d err=%v", len(res), err)
	}
}

func TestWriteRow_RecoveryIgnoresUncommitted(t *testing.T) {
	tmp := t.TempDir()
	heapPath := filepath.Join(tmp, "h")
	walPath := filepath.Join(tmp, "w")

	se := buildWriteRowEngine(t, heapPath, walPath)
	ctx := context.Background()
	tx := se.BeginWriteTransaction()
	if err := tx.WriteRow(ctx, "users", `{"id":8,"age":44}`, writeRowKeys(8, 44), true); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	// Intentionally do not commit; roll back so no COMMIT marker is written.
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	_ = se.WAL.Sync()
	se.Close()

	se2 := buildWriteRowEngine(t, heapPath, walPath)
	defer se2.Close()
	if err := se2.Recover(ctx, walPath); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if _, found, _ := getDocString(t, se2, "users", "id", types.IntKey(8)); found {
		t.Fatal("uncommitted row visible after recovery")
	}
}
