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

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
		{Name: "email", Primary: false, Type: storage.TypeVarchar},
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

	se2, err := storage.NewStorageEngine(tableMgr, walWriter2, hm2)
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

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3)

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

	hm, _ := heap.NewHeapManager(heapPath)
	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(tableMgr, walWriter, hm)
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
	mgr1 := storage.NewTableMenager()
	mgr1.NewTable("ghost", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3)
	hm1, _ := heap.NewHeapManager(heapPath)

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(mgr1, walWriter, hm1)

	se.InsertRow("ghost", `{"id":1}`, map[string]types.Comparable{"id": types.IntKey(1)})
	se.Close()

	// 2. Restart with NO tables defined
	mgr2 := storage.NewTableMenager()
	hm2, _ := heap.NewHeapManager(heapPath)

	walWriter2, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se2, _ := storage.NewStorageEngine(mgr2, walWriter2, hm2)
	defer se2.Close()

	// Should skip the entry gracefully
	if err := se2.Recover(walPath); err != nil {
		t.Errorf("Recover should skip missing table in MultiInsert, but got error: %v", err)
	}
}

func TestInsertRow_InvalidDoc(t *testing.T) {
	tmpDir := t.TempDir()
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3)
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap"))
	se, _ := storage.NewStorageEngine(tableMgr, nil, hm)
	defer se.Close()

	// Missing "id" in doc
	doc := `{"email": "no-id"}`
	keys := map[string]types.Comparable{"id": types.IntKey(1)}
	err := se.InsertRow("users", doc, keys)
	if err == nil {
		t.Error("Expected error for document missing index key")
	}
}
