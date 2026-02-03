package storage_test

import (
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/heap"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func TestInsertRow_FullFlow(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	hm, err := heap.NewHeapManager(heapPath)
	if err != nil {
		t.Fatalf("Failed to create heap: %v", err)
	}

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
		{Name: "email", Primary: false, Type: storage.TypeVarchar},
	}, 3, hm)

	walWriter, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	se, err := storage.NewStorageEngine(tableMgr, walWriter)
	if err != nil {
		walWriter.Close()
		t.Fatalf("Failed to create engine: %v", err)
	}

	doc := `{"id": 1, "email": "test@example.com", "name": "Test User"}`
	keys := map[string]types.Comparable{
		"id":    types.IntKey(1),
		"email": types.VarcharKey("test@example.com"),
	}

	// 1. InsertRow
	if err := se.InsertRow("users", doc, keys); err != nil {
		t.Fatalf("InsertRow failed: %v", err)
	}

	// 2. Verify in both indices
	valId, found, _ := se.Get("users", "id", types.IntKey(1))
	if !found || valId == "" {
		t.Errorf("Document not found in primary index")
	}

	valEmail, found, _ := se.Get("users", "email", types.VarcharKey("test@example.com"))
	if !found || valEmail == "" {
		t.Errorf("Document not found in secondary index")
	}

	if valId != valEmail {
		t.Errorf("Different results from indices: %s vs %s", valId, valEmail)
	}

	// 3. Duplicate Key check (Primary Key)
	err = se.InsertRow("users", doc, keys)
	if err == nil {
		t.Errorf("Expected error for duplicate key, but got nil")
	}

	se.Close()

	// 4. Recovery Test
	hm2, err := heap.NewHeapManager(heapPath)
	if err != nil {
		t.Fatalf("Failed to create heap for restart: %v", err)
	}

	walWriter2, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to create WAL 2: %v", err)
	}

	// Notice we need to redefine table schema for restart
	// But in this test scope we can reuse tableMgr if we want, but Engine takes it.
	// Actually engine takes it.
	// But we need to update NewTable calls if we recreated it.
	// In the original test, it reused tableMgr. But tableMgr has handles to old `hm` (if we passed it).
	// tableMgr entries have `Table` which has `Heap`.
	// The `hm` is closed?
	// `se.Close()` closes `wal` and `heap`.
	// `hm2` is new.
	// We should probably recreate tableMgr to attach `hm2`.

	tableMgr2 := storage.NewTableMenager()
	tableMgr2.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
		{Name: "email", Primary: false, Type: storage.TypeVarchar},
	}, 3, hm2)

	se2, err := storage.NewStorageEngine(tableMgr2, walWriter2)
	if err != nil {
		walWriter2.Close()
		t.Fatalf("Failed to restart engine: %v", err)
	}
	defer se2.Close()

	if err := se2.Recover(walPath); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	valRec, found, _ := se2.Get("users", "email", types.VarcharKey("test@example.com"))
	if !found || valRec == "" {
		t.Errorf("Recovered document not found in index")
	}
}

func TestRecover_CorruptedMultiInsert(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	hm, _ := heap.NewHeapManager(heapPath)
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm)

	// Write a MultiInsert entry with corrupted payload
	w, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	entry := &wal.WALEntry{
		Header: wal.WALHeader{
			Magic:      wal.WALMagic,
			Version:    1,
			EntryType:  wal.EntryMultiInsert,
			LSN:        1,
			PayloadLen: 4,
		},
		Payload: []byte("junk"),
	}
	w.WriteEntry(entry)
	w.Close()

	// New Heap/WAL for validation
	// Note: We use existing tableMgr which has `hm` attached.
	// But `hm` was created above. It wasn't closed explicitly but NewHeapManager opens it.
	// `se` will check it.
	// Actually `se` constructor doesn't take `hm` anymore.

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(tableMgr, walWriter)
	defer se.Close()

	if err := se.Recover(walPath); err == nil {
		t.Error("Expected error when recovering corrupted MultiInsert payload")
	}
}

func TestRecover_MultiInsertMissingTable(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	// 1. Create entry for table "ghost"
	hm1, _ := heap.NewHeapManager(heapPath)
	mgr1 := storage.NewTableMenager()
	mgr1.NewTable("ghost", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm1)

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(mgr1, walWriter)

	se.InsertRow("ghost", `{"id":1}`, map[string]types.Comparable{"id": types.IntKey(1)})
	se.Close()

	// 2. Restart with NO tables defined
	// hm2 unused in this test
	mgr2 := storage.NewTableMenager()
	// No tables

	walWriter2, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se2, _ := storage.NewStorageEngine(mgr2, walWriter2)
	defer se2.Close()

	// Should skip the entry gracefully
	if err := se2.Recover(walPath); err != nil {
		t.Errorf("Recover should skip missing table in MultiInsert, but got error: %v", err)
	}
}

func TestInsertRow_InvalidDoc(t *testing.T) {
	tmpDir := t.TempDir()
	// Heap first
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap"))

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm)

	se, _ := storage.NewStorageEngine(tableMgr, nil)
	defer se.Close()

	// Missing "id" in doc
	doc := `{"email": "no-id"}`
	keys := map[string]types.Comparable{"id": types.IntKey(1)}
	err := se.InsertRow("users", doc, keys)
	if err == nil {
		t.Error("Expected error for document missing index key")
	}
}
