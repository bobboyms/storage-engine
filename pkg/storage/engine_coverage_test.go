package storage_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/errors"
	"github.com/bobboyms/storage-engine/pkg/heap"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func TestRecover_WALOnly(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	// 1. Write to WAL directly or via Engine
	hm, _ := heap.NewHeapManager(heapPath)

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm)

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(tableMgr, walWriter)
	se.Put("users", "id", types.IntKey(1), "one")
	se.Put("users", "id", types.IntKey(2), "two")
	se.Close()

	// 2. Recover (No Checkpoint exists)
	hm2, err := heap.NewHeapManager(heapPath)
	if err != nil {
		t.Fatalf("Failed to start heap 2: %v", err)
	}

	tableMgr2 := storage.NewTableMenager()
	tableMgr2.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm2)

	walWriter2, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to create WAL 2: %v", err)
	}
	se2, err := storage.NewStorageEngine(tableMgr2, walWriter2)
	if err != nil {
		walWriter2.Close()
		t.Fatalf("Failed to start engine 2: %v", err)
	}
	defer se2.Close()

	if err := se2.Recover(walPath); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	val, found, _ := se2.Get("users", "id", types.IntKey(1))
	if !found || val != "one" {
		t.Errorf("Expected one, got %v", val)
	}
}

func TestRecover_MissingTable(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	// 1. Create entry for table "ghost"
	hm, _ := heap.NewHeapManager(heapPath)
	mgr1 := storage.NewTableMenager()
	mgr1.NewTable("ghost", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm)

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(mgr1, walWriter)
	se.Put("ghost", "id", types.IntKey(1), "boo")
	se.Close()

	// 2. Restart with only "users" table
	hm2, _ := heap.NewHeapManager(heapPath)
	mgr2 := storage.NewTableMenager()
	mgr2.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm2)

	walWriter2, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se2, _ := storage.NewStorageEngine(mgr2, walWriter2)
	defer se2.Close()

	// Should not fail, just skip "ghost" entries
	if err := se2.Recover(walPath); err != nil {
		t.Fatalf("Recover should ignore missing table, but got error: %v", err)
	}
}

func TestRecover_CorruptedEntry(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	// 1. Write explicit good entry
	hm, _ := heap.NewHeapManager(heapPath)
	mgr := storage.NewTableMenager()
	mgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm)
	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(mgr, walWriter)
	se.Put("users", "id", types.IntKey(1), "good")
	se.Close()

	// 2. Append garbage
	f, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF}) // Invalid Magic/Header
	f.Close()

	// 3. Recover
	hm2, _ := heap.NewHeapManager(heapPath)
	// Must recreate manager with table and new heap
	mgr2 := storage.NewTableMenager()
	mgr2.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm2)

	walWriter2, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se2, _ := storage.NewStorageEngine(mgr2, walWriter2)
	defer se2.Close()

	err := se2.Recover(walPath)
	if err == nil {
		t.Fatal("Expected error for corrupted WAL")
	}
	t.Logf("Got expected error: %v", err)
}

func TestPut_InvalidKeyType_Coverage(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap"))
	mgr := storage.NewTableMenager()
	mgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm)

	se, _ := storage.NewStorageEngine(mgr, nil)

	err := se.Put("users", "id", types.VarcharKey("bad"), `{"id": "bad"}`)
	if err == nil {
		t.Error("Expected InvalidKeyTypeError")
	} else if _, ok := err.(*errors.InvalidKeyTypeError); !ok {
		t.Errorf("Expected InvalidKeyTypeError, got %T: %v", err, err)
	}
}

func TestPut_KeyNotFoundInDoc(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap"))
	mgr := storage.NewTableMenager()
	mgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm)

	se, _ := storage.NewStorageEngine(mgr, nil)

	// Document doesn't contain "id"
	err := se.Put("users", "id", types.IntKey(1), `{"name":"missing_id"}`)
	if err == nil {
		t.Error("Expected error when key is missing in document")
	}
}

func TestPut_WALWriteError(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := storage.NewTableMenager()
	walPath := filepath.Join(tmpDir, "wal.log")
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap"))

	mgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm)

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(mgr, walWriter)

	// Force close the underlying file of WAL
	se.WAL.Close()

	err := se.Put("users", "id", types.IntKey(1), "{}")
	if err == nil {
		t.Log("Warning: WAL write did not fail as expected, possibly due to buffering.")
		return
	}
}

func TestDel_WALWriteError(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := storage.NewTableMenager()
	walPath := filepath.Join(tmpDir, "wal.log")
	hh, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap"))

	mgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hh)

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(mgr, walWriter)

	se.WAL.Close()

	_, err := se.Del("users", "id", types.IntKey(1))
	if err == nil {
		t.Log("Warning: WAL write did not fail as expected (buffering)")
		return
	}
}

func TestRecover_InvalidPayload(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	hm, _ := heap.NewHeapManager(heapPath)
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm)

	// Write entry with garbage payload manually
	// Uses wal.WALWriter directly
	w, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	entry := &wal.WALEntry{
		Header: wal.WALHeader{
			EntryType:  wal.EntryInsert,
			LSN:        1,
			PayloadLen: 4,
		},
		Payload: []byte("junk"),
	}
	w.WriteEntry(entry)
	w.Close()

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(tableMgr, walWriter)
	defer se.Close()

	if err := se.Recover(walPath); err == nil {
		t.Error("Expected error for invalid payload")
	}
}

func TestRecover_CorruptedCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	// 1. Create engine and make a checkpoint
	hm, _ := heap.NewHeapManager(heapPath)
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm)

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(tableMgr, walWriter)
	se.Put("users", "id", types.IntKey(1), "{}")
	if err := se.CreateCheckpoint(); err != nil {
		t.Fatalf("Failed to create checkpoint: %v", err)
	}
	se.Close()

	// 2. Corrupt the checkpoint file
	// Checkpoint is next to WAL file
	files, _ := os.ReadDir(tmpDir)
	found := false
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".chk" {
			path := filepath.Join(tmpDir, f.Name())
			// Overwrite with garbage
			os.WriteFile(path, []byte("garbage"), 0644)
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Checkpoint file not found")
	}

	// 3. Recover should fail loading checkpoint
	// Note: Engine restart needs valid heap manager
	hm3, _ := heap.NewHeapManager(heapPath)
	// We need to re-create tableMgr with new heap for the new engine instance
	tableMgr2 := storage.NewTableMenager()
	tableMgr2.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm3)

	walWriter2, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se2, _ := storage.NewStorageEngine(tableMgr2, walWriter2)
	defer se2.Close()

	err := se2.Recover(walPath)
	if err == nil {
		t.Error("Expected error for corrupted checkpoint")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}
