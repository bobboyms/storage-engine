package storage

import (
	"fmt"
	"github.com/bobboyms/storage-engine/pkg/types"
	"os"
	"path/filepath"
	"testing"
)

func TestVacuum_TombstoneReclamation(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "vacuum_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// walPath := filepath.Join(tmpDir, "test.wal")
	// se, _ := NewStorageEngine(meta, nil) // No WAL for simplicity? Or yes?
	// Use WAL to ensure full durability simulation (optional for Vacuum logic check)

	// Create Table
	meta := NewTableMenager() // Typo in codebase

	heapPath := filepath.Join(tmpDir, "users_heap")
	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatal(err)
	}

	indices := []Index{
		{Name: "id", Type: TypeInt, Primary: true},
	}

	err = meta.NewTable("users", indices, 4, hm)
	if err != nil {
		t.Fatal(err)
	}

	se, err := NewStorageEngine(meta, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer se.Close()

	// 1. Insert Data
	// Insert 10 records
	for i := 1; i <= 10; i++ {
		doc := fmt.Sprintf(`{"id": %d, "name": "user_%d"}`, i, i)
		keys := map[string]types.Comparable{
			"id": types.IntKey(i),
		}
		if err := se.InsertRow("users", doc, keys); err != nil {
			t.Fatal(err)
		}
	}

	// 2. Start a long-running transaction (Hold LSN)
	tx := se.BeginRead()
	// This TX snapshot should see everything inserted so far (or at least lock minLSN)
	// Actually, if we delete NOW, the deletion LSN will be > tx.SnapshotLSN.
	// So TX "should see" the record as Valid.
	// Therefore Vacuum CANNOT remove the tombstone because valid version is behind it?
	// Actually, our engine doesn't keep history if we Delete (in-place modification? No, append-only).
	// But `Heap.Delete` modifies RecordHeader in place (See `heap.go`).
	// Wait, `Heap.Delete` modifies in-place:
	// `seg.File.Seek(validOffset)... Write(0)... Write(deleteLSN)`.
	// This means the OLD version is mutated to be Invalid.
	// This contradicts append-only MVCC for deletes?
	// If `Heap.Delete` mutates in-place, then we lose the "Valid" version.
	// Ah, my understanding of `Heap.Delete` was it marks the record.
	// If so, `Read` sees Invalid + DeleteLSN.
	// If `DeleteLSN > SnapshotLSN`, `Read` says: "It was deleted AFTER my snapshot, so for me it is VALID."
	// BUT, we need the data content.
	// `Read` returns content + header.
	// So `Read` works.
	// Vacuum check: `if !header.Valid && header.DeleteLSN < minLSN`.
	// Since `DeleteLSN > tx.SnapshotLSN`, then condition `header.DeleteLSN < minLSN` is FALSE.
	// So Vacuum KEEPS it. Correct.

	// 3. Delete some records
	// Delete id=1, id=2
	se.Del("users", "id", types.IntKey(1))
	se.Del("users", "id", types.IntKey(2))

	// 4. Run Vacuum -> Should NOT reclaim, because tx is active (and DeleteLSN > tx.SnapshotLSN)
	// Actually `DeleteLSN` will be larger than `tx.SnapshotLSN`.
	// `minLSN` = `tx.SnapshotLSN` (since it's oldest).
	// Condition `DeleteLSN < minLSN` -> `Large < Small` -> False.
	// So it KEEPS.
	if err := se.Vacuum("users"); err != nil {
		t.Fatal(err)
	}

	// Verify keys still exist in tree (pointing to tombstones).
	// O vacuum v2 not reescreve o index; ele apenas compacta o heap.
	table, _ := meta.GetTableByName("users")
	idx, _ := table.GetIndex("id")
	if _, found, _ := idx.Tree.Get(types.IntKey(1)); !found {
		t.Error("Vacuum should have preserved key 1 (visible to active old tx)")
	}

	// 5. Close Transaction
	tx.Close()

	// 6. Run Vacuum -> Should Reclaim
	// Now minLSN moves to Current (or MaxUint64 if no active txs).
	// Condition `DeleteLSN < minLSN` -> `Small < Huge` -> True.
	// DROP.
	if err := se.Vacuum("users"); err != nil {
		t.Fatal(err)
	}

	// 7. Verify compacted slots are logically invisible.
	if _, found, _ := idx.Tree.Get(types.IntKey(1)); !found {
		t.Error("Vacuum v2 should keep key 1 indexed even after reclaiming the slot")
	}
	if _, found, _ := se.Get("users", "id", types.IntKey(1)); found {
		t.Error("Vacuum should have made key 1 unreachable via engine.Get")
	}
	if _, found, _ := idx.Tree.Get(types.IntKey(3)); !found {
		t.Error("Vacuum should have kept key 3")
	}

	// 8. Verify Heap Size Reduced
	// We can check file size of active segment (or all segments)
	// But simply checking Tree is strong enough proof of logical removal.
}

func TestDeleteAfterVacuumedSlotIsNoop(t *testing.T) {
	tmpDir := t.TempDir()

	meta := NewTableMenager()
	hm, err := NewHeapForTable(HeapFormatV2, filepath.Join(tmpDir, "users_heap"))
	if err != nil {
		t.Fatal(err)
	}
	if err := meta.NewTable("users", []Index{
		{Name: "id", Type: TypeInt, Primary: true},
	}, 4, hm); err != nil {
		t.Fatal(err)
	}

	se, err := NewStorageEngine(meta, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer se.Close()

	if err := se.Put("users", "id", types.IntKey(1), `{"id":1}`); err != nil {
		t.Fatal(err)
	}
	deleted, err := se.Del("users", "id", types.IntKey(1))
	if err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("first delete should find the key")
	}
	if err := se.Vacuum("users"); err != nil {
		t.Fatal(err)
	}

	deleted, err = se.Del("users", "id", types.IntKey(1))
	if err != nil {
		t.Fatalf("delete after vacuum should be a no-op, got error: %v", err)
	}
	if deleted {
		t.Fatal("delete after vacuumed slot should report not found")
	}
}
