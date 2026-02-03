package storage_test

import (
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/heap"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func TestDurability_WithCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")
	// CheckpointDir é automatico (dir do walPath)

	// 1. Inicia Engine
	hm, err := heap.NewHeapManager(heapPath)
	if err != nil {
		t.Fatalf("Failed to create heap: %v", err)
	}

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm)

	walWriter, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	se, err := storage.NewStorageEngine(tableMgr, walWriter)
	if err != nil {
		walWriter.Close()
		t.Fatalf("NewStorageEngine failed: %v", err)
	}

	// 2. Insere dados e cria Checkpoint
	se.Put("users", "id", types.IntKey(1), "doc1") // LSN 1
	se.Put("users", "id", types.IntKey(2), "doc2") // LSN 2

	if err := se.CreateCheckpoint(); err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}
	// Checkpoint deve conter LSN 2

	// 3. Insere mais dados (apenas no WAL, pós-checkpoint)
	se.Put("users", "id", types.IntKey(3), "doc3") // LSN 3
	se.Close()

	// 4. Recovery

	hm2, err := heap.NewHeapManager(heapPath)
	if err != nil {
		t.Fatalf("Failed to create heap: %v", err)
	}

	tableMgr2 := storage.NewTableMenager()
	tableMgr2.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm2)

	walWriter2, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to create WAL 2: %v", err)
	}
	se2, err := storage.NewStorageEngine(tableMgr2, walWriter2)
	if err != nil {
		walWriter2.Close()
		t.Fatalf("NewStorageEngine 2 failed: %v", err)
	}

	if err := se2.Recover(walPath); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// 5. Verifica dados
	// Key 1 e 2 devem vir do Checkpoint
	// Key 3 deve vir do WAL replay

	checkKey := func(k int, expectedDoc string) {
		doc, found, err := se2.Get("users", "id", types.IntKey(int64(k)))
		if err != nil {
			t.Errorf("Get(%d) error: %v", k, err)
		}
		if !found {
			t.Errorf("Get(%d) not found", k)
		}
		if doc != expectedDoc {
			t.Errorf("Get(%d) = %s, want %s", k, doc, expectedDoc)
		}
	}

	checkKey(1, "doc1")
	checkKey(2, "doc2")
	checkKey(3, "doc3")
}
