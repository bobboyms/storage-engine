package storage

import (
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/heap"
	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestStorageEngine_Durability(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_durability.wal")
	heapPath := filepath.Join(tmpDir, "test_durability.heap")

	tableMgr := NewTableMenager()
	tableMgr.NewTable("users", []Index{
		{Name: "id", Primary: true, Type: TypeInt},
	}, 3)

	// 1. Inicia Engine com WAL
	// Nota: NewStorageEngine usa SyncBatch por padrão, mas Close() garante flush.
	hm, err := heap.NewHeapManager(heapPath)
	if err != nil {
		t.Fatalf("Failed to create heap: %v", err)
	}
	se, err := NewStorageEngine(tableMgr, walPath, hm)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	// 2. Insere dados
	doc1 := "user_1"
	doc2 := "user_2"
	se.Put("users", "id", types.IntKey(1), doc1)
	se.Put("users", "id", types.IntKey(2), doc2)
	se.Close()

	// 3. Simula Crash/Restart (Recria Engine vazio)
	tableMgr2 := NewTableMenager()
	tableMgr2.NewTable("users", []Index{
		{Name: "id", Primary: true, Type: TypeInt},
	}, 3)

	// Re-open with same paths
	hm2, err := heap.NewHeapManager(heapPath)
	if err != nil {
		t.Fatalf("Failed to create heap: %v", err)
	}
	se2, err := NewStorageEngine(tableMgr2, walPath, hm2)
	if err != nil {
		t.Fatalf("Failed to create engine 2: %v", err)
	}

	// 4. Executa Recovery
	if err := se2.Recover(walPath); err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// 5. Verifica se dados voltaram
	doc, found, err := se2.Get("users", "id", types.IntKey(1))
	if err != nil || !found {
		t.Error("Key 1 not found after recovery")
	} else {
		if doc != doc1 {
			t.Errorf("Value for key 1 mismatch. Got %s, want %s", doc, doc1)
		}
	}

	// Testando RangeScan
	res, err := se2.RangeScan("users", "id", types.IntKey(1), types.IntKey(1))
	if err != nil || len(res) != 1 || res[0] != doc1 {
		t.Errorf("Value for key 1 mismatch in scan. Got %v, want [%s]", res, doc1)
	}

	res, err = se2.RangeScan("users", "id", types.IntKey(2), types.IntKey(2))
	if err != nil || len(res) != 1 || res[0] != doc2 {
		t.Errorf("Value for key 2 mismatch in scan. Got %v, want [%s]", res, doc2)
	}
}
