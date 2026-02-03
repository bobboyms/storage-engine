package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/heap"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func TestStorageEngine_Durability(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_durability.wal")
	heapPath := filepath.Join(tmpDir, "test_durability.heap")

	// Usando tabela unica para evitar conflitos de checkpoint fantasma
	tableName := "users_durability_unique"

	hm, err := heap.NewHeapManager(heapPath)
	if err != nil {
		t.Fatalf("Failed to create heap: %v", err)
	}

	tableMgr := NewTableMenager()
	tableMgr.NewTable(tableName, []Index{
		{Name: "id", Primary: true, Type: TypeInt},
	}, 3, hm)

	opts := wal.DefaultOptions()
	opts.SyncPolicy = wal.SyncBatch
	walWriter, err := wal.NewWALWriter(walPath, opts)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	se, err := NewStorageEngine(tableMgr, walWriter)
	if err != nil {
		walWriter.Close()
		t.Fatalf("Failed to create engine: %v", err)
	}

	// 2. Insere dados
	doc1 := "user_1"
	doc2 := "user_2"
	se.Put(tableName, "id", types.IntKey(1), doc1)
	se.Put(tableName, "id", types.IntKey(2), doc2)
	se.WAL.Sync() // Force sync
	se.Close()

	if info, err := os.Stat(walPath); err == nil {
		t.Logf("WAL file size: %d", info.Size())
	} else {
		t.Logf("WAL file verify failed: %v", err)
	}

	// 3. Simula Crash/Restart
	hm2, err := heap.NewHeapManager(heapPath)
	if err != nil {
		t.Fatalf("Failed to create heap: %v", err)
	}

	tableMgr2 := NewTableMenager()
	tableMgr2.NewTable(tableName, []Index{
		{Name: "id", Primary: true, Type: TypeInt},
	}, 3, hm2)

	walWriter2, err := wal.NewWALWriter(walPath, opts)
	if err != nil {
		t.Fatalf("Failed to create WAL 2: %v", err)
	}

	se2, err := NewStorageEngine(tableMgr2, walWriter2)
	if err != nil {
		walWriter2.Close()
		t.Fatalf("Failed to create engine 2: %v", err)
	}
	defer se2.Close()

	// 4. Executa Recovery
	// Deve recuperar do WAL, pois nao houve CreateCheckpoint explicitamente
	if err := se2.Recover(walPath); err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// 5. Verifica se dados voltaram
	doc, found, err := se2.Get(tableName, "id", types.IntKey(1))
	if err != nil || !found {
		t.Error("Key 1 not found after recovery")
	} else {
		if doc != doc1 {
			t.Errorf("Value for key 1 mismatch. Got %s, want %s", doc, doc1)
		}
	}

	res, err := se2.RangeScan(tableName, "id", types.IntKey(2), types.IntKey(2))
	if err != nil || len(res) != 1 || res[0] != doc2 {
		t.Errorf("Value for key 2 mismatch in scan. Got %v, want [%s]", res, doc2)
	}
}
