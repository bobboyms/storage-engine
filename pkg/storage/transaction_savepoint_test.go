package storage

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func newSavepointTestEngine(t *testing.T) *StorageEngine {
	t.Helper()
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("NewHeapForTable: %v", err)
	}
	tableMgr := NewTableMenager()
	if err := tableMgr.NewTable("users", []Index{{Name: "id", Primary: true, Type: TypeInt}}, 4, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	ww, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("NewWALWriter: %v", err)
	}
	se, err := NewStorageEngine(tableMgr, ww)
	if err != nil {
		_ = ww.Close()
		t.Fatalf("NewStorageEngine: %v", err)
	}
	t.Cleanup(func() { _ = se.Close() })
	return se
}

// TestWriteTransaction_RollbackToSavepoint verifies that work performed
// after a savepoint is discarded by RollbackToSavepoint, while work done
// before the savepoint survives and commits.
func TestWriteTransaction_RollbackToSavepoint(t *testing.T) {
	ctx := context.Background()
	se := newSavepointTestEngine(t)

	tx := se.BeginWriteTransaction()
	if err := tx.Put(ctx, "users", "id", types.IntKey(1), `{"id":1,"name":"keep"}`); err != nil {
		t.Fatalf("Put 1: %v", err)
	}
	if err := tx.Savepoint("sp1"); err != nil {
		t.Fatalf("Savepoint: %v", err)
	}
	if err := tx.Put(ctx, "users", "id", types.IntKey(2), `{"id":2,"name":"discard"}`); err != nil {
		t.Fatalf("Put 2: %v", err)
	}

	// Within the transaction key 2 is visible before rollback.
	if _, found, err := tx.GetBytes(ctx, "users", "id", types.IntKey(2)); err != nil {
		t.Fatalf("GetBytes 2 pre-rollback: %v", err)
	} else if !found {
		t.Fatalf("key 2 should be visible inside tx before rollback")
	}

	if err := tx.RollbackToSavepoint("sp1"); err != nil {
		t.Fatalf("RollbackToSavepoint: %v", err)
	}

	// After rollback to sp1, key 2 is gone but key 1 remains inside tx.
	if _, found, err := tx.GetBytes(ctx, "users", "id", types.IntKey(2)); err != nil {
		t.Fatalf("GetBytes 2 post-rollback: %v", err)
	} else if found {
		t.Fatalf("key 2 should be discarded after rollback to savepoint")
	}
	if _, found, err := tx.GetBytes(ctx, "users", "id", types.IntKey(1)); err != nil {
		t.Fatalf("GetBytes 1 post-rollback: %v", err)
	} else if !found {
		t.Fatalf("key 1 should survive rollback to savepoint")
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if val, found, _ := getDocString(t, se, "users", "id", types.IntKey(1)); !found || !strings.Contains(val, "keep") {
		t.Fatalf("key 1 should be committed, got found=%v val=%q", found, val)
	}
	if _, found, _ := getDocString(t, se, "users", "id", types.IntKey(2)); found {
		t.Fatalf("key 2 must not be committed after rollback to savepoint")
	}
}

// TestWriteTransaction_RollbackToUnknownSavepoint verifies a missing
// savepoint name is reported as an error rather than silently ignored.
func TestWriteTransaction_RollbackToUnknownSavepoint(t *testing.T) {
	ctx := context.Background()
	se := newSavepointTestEngine(t)

	tx := se.BeginWriteTransaction()
	defer func() { _ = tx.Rollback(ctx) }()
	if err := tx.Put(ctx, "users", "id", types.IntKey(1), `{"id":1}`); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := tx.RollbackToSavepoint("does-not-exist"); err == nil {
		t.Fatalf("expected error rolling back to unknown savepoint")
	}
}

// TestWriteTransaction_SavepointCommitsLaterRewrite verifies that a key
// rewritten after rollback-to-savepoint commits with the newest value,
// confirming the pending-write index is rebuilt correctly.
func TestWriteTransaction_SavepointCommitsLaterRewrite(t *testing.T) {
	ctx := context.Background()
	se := newSavepointTestEngine(t)

	tx := se.BeginWriteTransaction()
	if err := tx.Put(ctx, "users", "id", types.IntKey(1), `{"id":1,"name":"v1"}`); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if err := tx.Savepoint("sp"); err != nil {
		t.Fatalf("Savepoint: %v", err)
	}
	if err := tx.Put(ctx, "users", "id", types.IntKey(1), `{"id":1,"name":"v2"}`); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	if err := tx.RollbackToSavepoint("sp"); err != nil {
		t.Fatalf("RollbackToSavepoint: %v", err)
	}
	if err := tx.Put(ctx, "users", "id", types.IntKey(1), `{"id":1,"name":"v3"}`); err != nil {
		t.Fatalf("Put v3: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	val, found, _ := getDocString(t, se, "users", "id", types.IntKey(1))
	if !found || !strings.Contains(val, "v3") {
		t.Fatalf("expected committed value v3, got found=%v val=%q", found, val)
	}
}
