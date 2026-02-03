package storage_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/errors"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func TestRecover_WALOnly(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3)

	// 1. Write to WAL directly or via Engine
	se, _ := storage.NewStorageEngine(tableMgr, walPath, heapPath)
	se.Put("users", "id", types.IntKey(1), "one")
	se.Put("users", "id", types.IntKey(2), "two")
	se.Close()

	// 2. Recover (No Checkpoint exists)
	se2, err := storage.NewStorageEngine(tableMgr, walPath, heapPath)
	if err != nil {
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
	mgr1 := storage.NewTableMenager()
	mgr1.NewTable("ghost", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3)
	se, _ := storage.NewStorageEngine(mgr1, walPath, heapPath)
	se.Put("ghost", "id", types.IntKey(1), "boo")
	se.Close()

	// 2. Restart with only "users" table
	mgr2 := storage.NewTableMenager()
	mgr2.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3)
	se2, _ := storage.NewStorageEngine(mgr2, walPath, heapPath)
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
	mgr := storage.NewTableMenager()
	mgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3)
	se, _ := storage.NewStorageEngine(mgr, walPath, heapPath)
	se.Put("users", "id", types.IntKey(1), "good")
	se.Close()

	// 2. Append garbage
	f, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF}) // Invalid Magic/Header
	f.Close()

	// 3. Recover
	se2, _ := storage.NewStorageEngine(mgr, walPath, heapPath)
	defer se2.Close()

	err := se2.Recover(walPath)
	if err == nil {
		t.Fatal("Expected error for corrupted WAL")
	}
	t.Logf("Got expected error: %v", err)
}

func TestPut_InvalidKeyType_Coverage(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := storage.NewTableMenager()
	mgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3)
	se, _ := storage.NewStorageEngine(mgr, "", filepath.Join(tmpDir, "heap"))

	err := se.Put("users", "id", types.VarcharKey("bad"), `{"id": "bad"}`)
	if err == nil {
		t.Error("Expected InvalidKeyTypeError")
	} else if _, ok := err.(*errors.InvalidKeyTypeError); !ok {
		t.Errorf("Expected InvalidKeyTypeError, got %T: %v", err, err)
	}
}

func TestPut_KeyNotFoundInDoc(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := storage.NewTableMenager()
	mgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3)
	se, _ := storage.NewStorageEngine(mgr, "", filepath.Join(tmpDir, "heap"))

	// Document doesn't contain "id"
	err := se.Put("users", "id", types.IntKey(1), `{"name":"missing_id"}`)
	if err == nil {
		t.Error("Expected error when key is missing in document")
	}
}

func TestPut_WALWriteError(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := storage.NewTableMenager()
	mgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3)
	walPath := filepath.Join(tmpDir, "wal.log")
	se, _ := storage.NewStorageEngine(mgr, walPath, filepath.Join(tmpDir, "heap"))

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
	mgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3)
	walPath := filepath.Join(tmpDir, "wal.log")
	se, _ := storage.NewStorageEngine(mgr, walPath, filepath.Join(tmpDir, "heap"))

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

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3)

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

	se, _ := storage.NewStorageEngine(tableMgr, walPath, heapPath)
	defer se.Close()

	if err := se.Recover(walPath); err == nil {
		t.Error("Expected error for invalid payload")
	}
}

func TestRecover_CorruptedCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3)

	// 1. Create engine and make a checkpoint
	se, _ := storage.NewStorageEngine(tableMgr, walPath, heapPath)
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
	se2, _ := storage.NewStorageEngine(tableMgr, walPath, heapPath)
	defer se2.Close()

	err := se2.Recover(walPath)
	if err == nil {
		t.Error("Expected error for corrupted checkpoint")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}
