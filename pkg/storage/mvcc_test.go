package storage_test

import (
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/heap"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// TestMVCC_SnapshotRead verifies that a transaction sees a consistent snapshot
// ignoring new inserts that happen after the transaction started.
func TestMVCC_SnapshotRead(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("mvcc_test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)

	hm, err := heap.NewHeapManager(heapPath)
	if err != nil {
		t.Fatalf("Failed to create heap: %v", err)
	}
	walWriter, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	se, err := storage.NewStorageEngine(tableMgr, walWriter, hm)
	if err != nil {
		walWriter.Close()
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer se.Close()

	// 1. Setup initial state (LSN 1)
	// Put increments LSN. Initial LSN is 0. Put -> 1.
	err = se.Put("mvcc_test", "id", types.IntKey(1), `{"id":1}`)
	if err != nil {
		t.Fatalf("Put 1 failed: %v", err)
	}

	// 2. Start Transaction 1 (Snapshot at ~LSN 1)
	tx1 := se.BeginRead()

	// 3. Perform a write that advances LSN (LSN 2)
	err = se.Put("mvcc_test", "id", types.IntKey(2), `{"id":2}`)
	if err != nil {
		t.Fatalf("Put 2 failed: %v", err)
	}

	// 4. Tx1 Reads Key 2 -> Should NOT see it (Created at LSN 2 > Snapshot LSN 1)
	_, found, err := tx1.Get("mvcc_test", "id", types.IntKey(2))
	if err != nil {
		t.Fatalf("Tx1 Get error: %v", err)
	}
	if found {
		t.Error("Tx1 should NOT see key 2 (Snapshot Isolation failed)")
	}

	// 5. Normal Get (Implicit Tx/New Snapshot) -> Should see Key 2
	_, found, err = se.Get("mvcc_test", "id", types.IntKey(2))
	if err != nil {
		t.Fatalf("Engine Get error: %v", err)
	}
	if !found {
		t.Error("Engine should see key 2")
	}

	// 6. Tx1 Reads Key 1 -> Should see it
	_, found, err = tx1.Get("mvcc_test", "id", types.IntKey(1))
	if err != nil {
		t.Fatalf("Tx1 Get Key 1 error: %v", err)
	}
	if !found {
		t.Error("Tx1 should see key 1")
	}
}

func TestMVCC_Update_TimeTravel(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("mvcc_update", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3)
	hm, _ := heap.NewHeapManager(heapPath)
	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(tableMgr, walWriter, hm)
	defer se.Close()

	// 1. Insert Initial (LSN 1)
	if err := se.Put("mvcc_update", "id", types.IntKey(1), `{"id":1,"val":"v1"}`); err != nil {
		t.Fatalf("Put 1 failed: %v", err)
	}

	// 2. Start Tx (Snapshot LSN 1)
	tx := se.BeginRead()

	// 3. Update (LSN 2)
	if err := se.Put("mvcc_update", "id", types.IntKey(1), `{"id":1,"val":"v2"}`); err != nil {
		t.Fatalf("Put 2 failed: %v", err)
	}

	// 4. Update Again (LSN 3)
	if err := se.Put("mvcc_update", "id", types.IntKey(1), `{"id":1,"val":"v3"}`); err != nil {
		t.Fatalf("Put 3 failed: %v", err)
	}

	// 5. Tx Should see v1
	val, found, _ := tx.Get("mvcc_update", "id", types.IntKey(1))
	if !found || val != `{"id":1,"val":"v1"}` {
		t.Errorf("Tx expected v1, got %v (found=%v)", val, found)
	}

	// 6. Engine (New Tx) Should see v3
	val, found, _ = se.Get("mvcc_update", "id", types.IntKey(1))
	if !found || val != `{"id":1,"val":"v3"}` {
		t.Errorf("Engine expected v3, got %v", val)
	}
}

func TestMVCC_Delete_TimeTravel(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("mvcc_del", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3)
	hm, _ := heap.NewHeapManager(heapPath)
	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(tableMgr, walWriter, hm)
	defer se.Close()

	// 1. Insert (LSN 1)
	if err := se.Put("mvcc_del", "id", types.IntKey(1), `{"id":1,"val":"exist"}`); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 2. Start Tx (Snapshot LSN 1)
	tx := se.BeginRead()

	// 3. Delete (LSN 2)
	se.Del("mvcc_del", "id", types.IntKey(1))

	// 4. Tx Should see "exist" (DeleteLSN 2 > Snapshot 1)
	val, found, _ := tx.Get("mvcc_del", "id", types.IntKey(1))
	if !found {
		t.Error("Tx should still see deleted record")
	}
	if val != `{"id":1,"val":"exist"}` {
		t.Errorf("Tx expected 'exist', got %v", val)
	}

	// 5. Engine Should NOT see it
	_, found, _ = se.Get("mvcc_del", "id", types.IntKey(1))
	if found {
		t.Error("Engine should NOT find deleted record")
	}
}

func TestMVCC_IsolationLevels(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("iso_test", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3)
	hm, _ := heap.NewHeapManager(heapPath)
	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(tableMgr, walWriter, hm)
	defer se.Close()

	// Initial Insert (LSN 1)
	if err := se.Put("iso_test", "id", types.IntKey(1), `{"id":1,"val":"initial"}`); err != nil {
		t.Fatal(err)
	}

	// 1. Repeatable Read (Default)
	// Tx1 Starts (Snapshot LSN 1)
	txRR := se.BeginTransaction(storage.RepeatableRead)

	// Update (LSN 2)
	if err := se.Put("iso_test", "id", types.IntKey(1), `{"id":1,"val":"updated"}`); err != nil {
		t.Fatal(err)
	}

	// Tx1 Get -> Should see "initial" (Old Version)
	val, found, _ := txRR.Get("iso_test", "id", types.IntKey(1))
	if !found || val != `{"id":1,"val":"initial"}` {
		t.Errorf("RR expected 'initial', got %v", val)
	}

	// 2. Read Committed
	// Tx2 Starts (Snapshot LSN 2) - But we will update again to test dynamic snapshot
	txRC := se.BeginTransaction(storage.ReadCommitted)

	// Tx2 Get -> Should see "updated" (Current)
	val, found, _ = txRC.Get("iso_test", "id", types.IntKey(1))
	if !found || val != `{"id":1,"val":"updated"}` {
		t.Errorf("RC expected 'updated', got %v", val)
	}

	// Update Again (LSN 3)
	if err := se.Put("iso_test", "id", types.IntKey(1), `{"id":1,"val":"updated_again"}`); err != nil {
		t.Fatal(err)
	}

	// TxRC Get AGAIN -> Should refresh snapshot and see "updated_again"
	val, found, _ = txRC.Get("iso_test", "id", types.IntKey(1))
	if !found || val != `{"id":1,"val":"updated_again"}` {
		t.Errorf("RC expected 'updated_again' after refresh, got %v", val)
	}

	// TxRR Get AGAIN -> Should STILL see "initial"
	val, found, _ = txRR.Get("iso_test", "id", types.IntKey(1))
	if !found || val != `{"id":1,"val":"initial"}` {
		t.Errorf("RR expected 'initial' (unchanged), got %v", val)
	}
}
