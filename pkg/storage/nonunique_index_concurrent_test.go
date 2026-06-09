package storage_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// countByEmail counts the rows visible under a non-unique secondary index key.
func countByEmail(t *testing.T, se *storage.StorageEngine, email string) int {
	t.Helper()
	it, err := se.NewIterator(context.Background(), "users", "email", storage.IterOptions{
		Lower: types.VarcharKey(email),
		Upper: types.VarcharKey(email),
	})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	defer it.Close()
	n := 0
	for it.Next() {
		n++
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator err: %v", err)
	}
	return n
}

// TestNonUniqueIndexSurvivesVacuumAfterUpdates guards the report that a
// non-unique secondary index loses sibling rows after repeated UPDATEs to a
// non-indexed column followed by a vacuum. The siblings (never updated) keep
// exactly one live version each, and the hammered row keeps its newest version,
// so the index must continue to return every member of the duplicate group.
func TestNonUniqueIndexSurvivesVacuumAfterUpdates(t *testing.T) {
	se, cleanup := newMultiIndexEngine(t)
	defer cleanup()

	ctx := context.Background()
	const shared = "shared@example.com"
	const rows = 3
	for i := 1; i <= rows; i++ {
		doc := fmt.Sprintf(`{"id":%d,"email":%q,"name":"n%d"}`, i, shared, i)
		if err := se.InsertRow(ctx, "users", doc, map[string]types.Comparable{
			"id":    types.IntKey(int64(i)),
			"email": types.VarcharKey(shared),
		}); err != nil {
			t.Fatalf("InsertRow %d: %v", i, err)
		}
	}

	if got := countByEmail(t, se, shared); got != rows {
		t.Fatalf("baseline count = %d, want %d", got, rows)
	}

	// Hammer row 1 with non-indexed updates (email unchanged); each creates a
	// dead version that vacuum can reclaim.
	for i := range 30 {
		doc := fmt.Sprintf(`{"id":1,"email":%q,"name":"v%d"}`, shared, i)
		if err := se.UpsertRow(ctx, "users", doc, map[string]types.Comparable{
			"id":    types.IntKey(1),
			"email": types.VarcharKey(shared),
		}); err != nil {
			t.Fatalf("UpsertRow %d: %v", i, err)
		}
	}

	if got := countByEmail(t, se, shared); got != rows {
		t.Fatalf("after updates, before vacuum: count = %d, want %d", got, rows)
	}

	if err := se.Vacuum(ctx, "users"); err != nil {
		t.Fatalf("Vacuum: %v", err)
	}

	if got := countByEmail(t, se, shared); got != rows {
		t.Fatalf("after vacuum: index lost rows: count = %d, want %d", got, rows)
	}
}

// TestNonUniqueIndexSurvivesEvictionUnderUpdates drives the secondary index past
// its buffer-pool capacity (forcing page eviction + reload) while one duplicate
// group is hammered with non-indexed updates, then vacuumed.
func TestNonUniqueIndexSurvivesEvictionUnderUpdates(t *testing.T) {
	se, cleanup := newMultiIndexEngine(t)
	defer cleanup()

	ctx := context.Background()
	const shared = "shared@example.com"
	const group = 5
	for i := 1; i <= group; i++ {
		doc := fmt.Sprintf(`{"id":%d,"email":%q,"name":"n%d"}`, i, shared, i)
		if err := se.InsertRow(ctx, "users", doc, map[string]types.Comparable{
			"id":    types.IntKey(int64(i)),
			"email": types.VarcharKey(shared),
		}); err != nil {
			t.Fatalf("InsertRow %d: %v", i, err)
		}
	}
	// Distinct secondary keys to push the index past its 16-page buffer pool.
	const filler = 2500
	for i := range filler {
		id := group + 1 + i
		email := fmt.Sprintf("user%05d@example.com", i)
		doc := fmt.Sprintf(`{"id":%d,"email":%q,"name":"f%d"}`, id, email, id)
		if err := se.InsertRow(ctx, "users", doc, map[string]types.Comparable{
			"id":    types.IntKey(int64(id)),
			"email": types.VarcharKey(email),
		}); err != nil {
			t.Fatalf("InsertRow filler %d: %v", id, err)
		}
	}

	if got := countByEmail(t, se, shared); got != group {
		t.Fatalf("baseline shared count = %d, want %d", got, group)
	}

	for cycle := range 40 {
		doc := fmt.Sprintf(`{"id":1,"email":%q,"name":"v%d"}`, shared, cycle)
		if err := se.UpsertRow(ctx, "users", doc, map[string]types.Comparable{
			"id":    types.IntKey(1),
			"email": types.VarcharKey(shared),
		}); err != nil {
			t.Fatalf("UpsertRow cycle %d: %v", cycle, err)
		}
		// Touch far-away keys to churn the buffer pool and evict the hot leaf.
		_ = countByEmail(t, se, fmt.Sprintf("user%05d@example.com", cycle%filler))

		if got := countByEmail(t, se, shared); got != group {
			t.Fatalf("after update cycle %d: shared count = %d, want %d", cycle, got, group)
		}
	}

	if err := se.Vacuum(ctx, "users"); err != nil {
		t.Fatalf("Vacuum: %v", err)
	}
	if got := countByEmail(t, se, shared); got != group {
		t.Fatalf("after vacuum: shared count = %d, want %d", got, group)
	}
}
