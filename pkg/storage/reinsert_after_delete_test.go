package storage_test

import (
	"context"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// TestStorageReinsertSamePrimaryKeyAfterDelete guards that a DELETE which
// tombstones a row frees its primary key for reuse. The primary-key duplicate
// check must be MVCC-visibility-aware (like the UNIQUE constraint check): a
// tombstoned head version is not a live duplicate, so re-inserting the same
// primary key has to succeed.
func TestStorageReinsertSamePrimaryKeyAfterDelete(t *testing.T) {
	ctx := context.Background()
	se, cleanup := newMultiIndexEngine(t)
	defer cleanup()

	keys := func(id int64, email string) map[string]types.Comparable {
		return map[string]types.Comparable{"id": types.IntKey(id), "email": types.VarcharKey(email)}
	}

	if err := se.InsertRow(ctx, "users", `{"id":1,"email":"a@x.com","name":"A"}`, keys(1, "a@x.com")); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := se.Del(ctx, "users", "id", types.IntKey(1)); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// The deleted row must no longer be visible.
	if _, found, err := getDocStringExt(t, se, "users", "id", types.IntKey(1)); err != nil || found {
		t.Fatalf("after delete: found=%v err=%v, want not found", found, err)
	}

	// Re-inserting the SAME primary key must be allowed now that the row is gone.
	if err := se.InsertRow(ctx, "users", `{"id":1,"email":"b@x.com","name":"B"}`, keys(1, "b@x.com")); err != nil {
		t.Fatalf("reinsert of a deleted primary key should be allowed: %v", err)
	}

	got, found, err := getDocStringExt(t, se, "users", "id", types.IntKey(1))
	if err != nil || !found || got != `{"id":1,"email":"b@x.com","name":"B"}` {
		t.Fatalf("after reinsert: found=%v got=%q err=%v", found, got, err)
	}
}
