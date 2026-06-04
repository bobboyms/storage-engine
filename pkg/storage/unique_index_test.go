package storage

import (
	"context"
	stderrors "errors"
	"path/filepath"
	"testing"

	storageerrors "github.com/bobboyms/storage-engine/pkg/errors"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// newUniqueEngine builds an engine with a "users" table that has a unique
// secondary index on "email".
func newUniqueEngine(t *testing.T) *StorageEngine {
	t.Helper()
	dir := t.TempDir()
	hm, err := NewHeapForTable(HeapFormatV2, filepath.Join(dir, "users.heap"))
	if err != nil {
		t.Fatalf("heap: %v", err)
	}
	meta := NewTableMenager()
	if err := meta.NewTable("users", []Index{
		{Name: "id", Primary: true, Type: TypeInt},
		{Name: "email", Type: TypeVarchar, Unique: true},
	}, 0, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	ww, err := wal.NewWALWriter(filepath.Join(dir, "users.wal"), wal.DefaultOptions())
	if err != nil {
		t.Fatalf("wal: %v", err)
	}
	se, err := NewProductionStorageEngine(meta, ww)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	t.Cleanup(func() { _ = se.Close() })
	return se
}

func uniqueKeys(id int64, email string) map[string]types.Comparable {
	return map[string]types.Comparable{
		"id":    types.IntKey(id),
		"email": types.VarcharKey(email),
	}
}

func TestStorageUniqueIndexRejectsDuplicate(t *testing.T) {
	ctx := context.Background()
	se := newUniqueEngine(t)

	if err := se.InsertRow(ctx, "users", `{"id":1,"email":"a@x.com"}`, uniqueKeys(1, "a@x.com")); err != nil {
		t.Fatalf("first insert: %v", err)
	}

	err := se.InsertRow(ctx, "users", `{"id":2,"email":"a@x.com"}`, uniqueKeys(2, "a@x.com"))
	var dup *storageerrors.DuplicateKeyError
	if !stderrors.As(err, &dup) {
		t.Fatalf("expected DuplicateKeyError, got %v", err)
	}

	// A different value is accepted.
	if err := se.InsertRow(ctx, "users", `{"id":3,"email":"b@x.com"}`, uniqueKeys(3, "b@x.com")); err != nil {
		t.Fatalf("non-conflicting insert: %v", err)
	}
}

func TestStorageUniqueIndexAllowsDeleteThenReinsert(t *testing.T) {
	ctx := context.Background()
	se := newUniqueEngine(t)

	if err := se.InsertRow(ctx, "users", `{"id":1,"email":"a@x.com"}`, uniqueKeys(1, "a@x.com")); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := se.Del(ctx, "users", "id", types.IntKey(1)); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := se.InsertRow(ctx, "users", `{"id":2,"email":"a@x.com"}`, uniqueKeys(2, "a@x.com")); err != nil {
		t.Fatalf("reinsert after delete should be allowed: %v", err)
	}
}
