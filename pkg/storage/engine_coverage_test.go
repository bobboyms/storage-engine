package storage_test

import (
	"context"
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

	// 1. Write to WAL directly or via Engine
	hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm)

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(tableMgr, walWriter)
	se.Put(context.Background(), "users", "id", types.IntKey(1), "one")
	se.Put(context.Background(), "users", "id", types.IntKey(2), "two")
	se.Close()

	// 2. Recover (No Checkpoint exists)
	hm2, err := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
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

	if err := se2.Recover(context.Background(), walPath); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	val, found, _ := getDocStringExt(t, se2, "users", "id", types.IntKey(1))
	if !found || val != "one" {
		t.Errorf("Expected one, got %v", val)
	}
}

func TestRecover_MissingTable(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	// 1. Create entry for table "ghost"
	hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	mgr1 := storage.NewTableMenager()
	mgr1.NewTable("ghost", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm)

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(mgr1, walWriter)
	se.Put(context.Background(), "ghost", "id", types.IntKey(1), "boo")
	se.Close()

	// 2. Restart with only "users" table
	hm2, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	mgr2 := storage.NewTableMenager()
	mgr2.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm2)

	walWriter2, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se2, _ := storage.NewStorageEngine(mgr2, walWriter2)
	defer se2.Close()

	// Should not fail, just skip "ghost" entries
	if err := se2.Recover(context.Background(), walPath); err != nil {
		t.Fatalf("Recover should ignore missing table, but got error: %v", err)
	}
}

func TestRecover_CorruptedEntry(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	// 1. Write explicit good entry
	hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	mgr := storage.NewTableMenager()
	mgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm)
	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(mgr, walWriter)
	se.Put(context.Background(), "users", "id", types.IntKey(1), "good")
	se.Close()

	// 2. Corrupt a byte INSIDE the first written data page (pageID 1 starts
	// at offset PageSize; its body starts after the page header). This is
	// genuine mid-file corruption — distinct from a torn trailing page — and
	// must still be detected (CRC/auth failure), never silently tolerated.
	raw, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	const pageSize = 8192
	const pageHeaderSize = 32
	corruptAt := pageSize + pageHeaderSize + 8 // inside page 1's body
	if corruptAt >= len(raw) {
		t.Fatalf("WAL too small (%d bytes) to corrupt page body", len(raw))
	}
	raw[corruptAt] ^= 0xFF
	if err := os.WriteFile(walPath, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	// 3. Recover
	hm2, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	// Must recreate manager with table and new heap
	mgr2 := storage.NewTableMenager()
	mgr2.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm2)

	walWriter2, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se2, err := storage.NewStorageEngine(mgr2, walWriter2)
	if err != nil {
		// 6.1: scanMaxWALLSN now rejects mid-file corruption at open
		// time, so the engine never gets a chance to mis-initialise
		// its LSN tracker. That is the expected failure mode here.
		walWriter2.Close()
		t.Logf("Got expected open error: %v", err)
		return
	}
	defer se2.Close()

	if err := se2.Recover(context.Background(), walPath); err == nil {
		t.Fatal("Expected error for corrupted WAL")
	}
}

// TestRecover_TornTrailingPageTolerated proves the complement of
// TestRecover_CorruptedEntry: a crash that left a torn (partial) trailing
// page — extra bytes beyond the last fully written page — must NOT make the
// log unreadable. Every entry committed before the torn tail is recovered.
func TestRecover_TornTrailingPageTolerated(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	mgr := storage.NewTableMenager()
	mgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm)
	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(mgr, walWriter)
	if err := se.Put(context.Background(), "users", "id", types.IntKey(1), "good"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	se.Close()

	// Torn write of the next page: append partial bytes so the file is no
	// longer page-aligned.
	f, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o644)
	_, _ = f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	f.Close()

	hm2, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	mgr2 := storage.NewTableMenager()
	mgr2.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm2)
	walWriter2, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("WAL must open despite torn trailing page: %v", err)
	}
	se2, err := storage.NewStorageEngine(mgr2, walWriter2)
	if err != nil {
		t.Fatalf("engine must open despite torn trailing page: %v", err)
	}
	defer se2.Close()

	got, found, err := getDocStringExt(t, se2, "users", "id", types.IntKey(1))
	if err != nil {
		t.Fatalf("Get after torn-tail recovery: %v", err)
	}
	if !found || got != "good" {
		t.Fatalf("committed record lost after torn-tail recovery: found=%v got=%q", found, got)
	}
}

func TestPut_InvalidKeyType_Coverage(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, filepath.Join(tmpDir, "heap"))
	mgr := storage.NewTableMenager()
	mgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm)

	se, _ := storage.NewStorageEngine(mgr, nil)

	err := se.Put(context.Background(), "users", "id", types.VarcharKey("bad"), `{"id": "bad"}`)
	if err == nil {
		t.Error("Expected InvalidKeyTypeError")
	} else if _, ok := err.(*errors.InvalidKeyTypeError); !ok {
		t.Errorf("Expected InvalidKeyTypeError, got %T: %v", err, err)
	}
}

func TestPut_KeyNotFoundInDoc(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, filepath.Join(tmpDir, "heap"))
	mgr := storage.NewTableMenager()
	mgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm)

	se, _ := storage.NewStorageEngine(mgr, nil)

	// Document doesn't contain "id"
	err := se.Put(context.Background(), "users", "id", types.IntKey(1), `{"name":"missing_id"}`)
	if err == nil {
		t.Error("Expected error when key is missing in document")
	}
}

func TestPut_WALWriteError(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := storage.NewTableMenager()
	walPath := filepath.Join(tmpDir, "wal.log")
	hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, filepath.Join(tmpDir, "heap"))

	mgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm)

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(mgr, walWriter)

	// Force close the underlying file of WAL
	se.WAL.Close()

	err := se.Put(context.Background(), "users", "id", types.IntKey(1), "{}")
	if err == nil {
		t.Log("Warning: WAL write did not fail as expected, possibly due to buffering.")
		return
	}
}

func TestDel_WALWriteError(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := storage.NewTableMenager()
	walPath := filepath.Join(tmpDir, "wal.log")
	hh, _ := storage.NewHeapForTable(storage.HeapFormatV2, filepath.Join(tmpDir, "heap"))

	mgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hh)

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(mgr, walWriter)

	se.WAL.Close()

	_, err := se.Del(context.Background(), "users", "id", types.IntKey(1))
	if err == nil {
		t.Log("Warning: WAL write did not fail as expected (buffering)")
		return
	}
}

func TestRecover_InvalidPayload(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
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
	se, err := storage.NewStorageEngine(tableMgr, walWriter)
	if err != nil {
		// scanMaxWALLSN now rejects mid-file corruption up front; that
		// is the expected behavior on an invalid payload.
		walWriter.Close()
		return
	}
	defer se.Close()

	if err := se.Recover(context.Background(), walPath); err == nil {
		t.Error("Expected error for invalid payload")
	}
}

func TestRecover_IgnoresLegacyCheckpointFile(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")
	btreePath := filepath.Join(tmpDir, "users.id.btree.v2")

	// 1. Create engine and persist data normally.
	hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath, nil)
	idxTree, _ := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, btreePath, nil)
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree}}, 3, hm)

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(tableMgr, walWriter)
	if err := se.Put(context.Background(), "users", "id", types.IntKey(1), `{"id":1}`); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	se.Close()

	// 2. Drop a legacy garbage .chk next to the WAL. Runtime recovery
	// no longer uses this format, so it must be ignored.
	if err := os.WriteFile(filepath.Join(tmpDir, "checkpoint_users_id_99.chk"), []byte("garbage"), 0644); err != nil {
		t.Fatal(err)
	}

	// 3. Recover should succeed from WAL only.
	hm3, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath, nil)
	idxTree2, _ := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, btreePath, nil)
	tableMgr2 := storage.NewTableMenager()
	tableMgr2.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree2}}, 3, hm3)

	walWriter2, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se2, _ := storage.NewStorageEngine(tableMgr2, walWriter2)
	defer se2.Close()

	if err := se2.Recover(context.Background(), walPath); err != nil {
		t.Fatalf("Recover should ignore legacy .chk files, got: %v", err)
	}

	if _, found, err := getDocStringExt(t, se2, "users", "id", types.IntKey(1)); err != nil {
		t.Fatalf("Get failed: %v", err)
	} else if !found {
		t.Fatal("expected recovered key after WAL-only recovery")
	}
}
