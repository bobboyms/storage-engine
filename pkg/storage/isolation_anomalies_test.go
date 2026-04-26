package storage

import (
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/query"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func openAnomalyTestEngine(t *testing.T) *StorageEngine {
	t.Helper()

	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("new heap: %v", err)
	}

	tableMgr := NewTableMenager()
	if err := tableMgr.NewTable("items", []Index{{Name: "id", Primary: true, Type: TypeInt}}, 4, hm); err != nil {
		t.Fatalf("new table items: %v", err)
	}

	walWriter, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}

	se, err := NewStorageEngine(tableMgr, walWriter)
	if err != nil {
		t.Fatalf("new storage engine: %v", err)
	}
	return se
}

func TestIsolation_DirtyReadPrevented(t *testing.T) {
	se := openAnomalyTestEngine(t)
	defer se.Close()

	txw := se.BeginWriteTransaction()
	if err := txw.Put("items", "id", types.IntKey(1), `{"id":1,"value":"pending"}`); err != nil {
		t.Fatalf("write tx put: %v", err)
	}

	txRC := se.BeginTransaction(ReadCommitted)
	defer txRC.Close()
	_, found, err := txRC.Get("items", "id", types.IntKey(1))
	if err != nil {
		t.Fatalf("read committed get: %v", err)
	}
	if found {
		t.Fatal("read committed observed uncommitted write")
	}

	txRR := se.BeginTransaction(RepeatableRead)
	defer txRR.Close()
	_, found, err = txRR.Get("items", "id", types.IntKey(1))
	if err != nil {
		t.Fatalf("repeatable read get: %v", err)
	}
	if found {
		t.Fatal("repeatable read observed uncommitted write")
	}
}

func TestIsolation_ReadCommittedAllowsNonRepeatableRead(t *testing.T) {
	se := openAnomalyTestEngine(t)
	defer se.Close()

	if err := se.Put("items", "id", types.IntKey(1), `{"id":1,"value":"v1"}`); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	tx := se.BeginTransaction(ReadCommitted)
	defer tx.Close()

	doc, found, err := tx.Get("items", "id", types.IntKey(1))
	if err != nil || !found || doc != `{"id":1,"value":"v1"}` {
		t.Fatalf("first read: found=%v doc=%q err=%v", found, doc, err)
	}

	if err := se.Put("items", "id", types.IntKey(1), `{"id":1,"value":"v2"}`); err != nil {
		t.Fatalf("concurrent update: %v", err)
	}

	doc, found, err = tx.Get("items", "id", types.IntKey(1))
	if err != nil || !found || doc != `{"id":1,"value":"v2"}` {
		t.Fatalf("second read: found=%v doc=%q err=%v", found, doc, err)
	}
}

func TestIsolation_RepeatableReadPreventsNonRepeatableRead(t *testing.T) {
	se := openAnomalyTestEngine(t)
	defer se.Close()

	if err := se.Put("items", "id", types.IntKey(1), `{"id":1,"value":"v1"}`); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	tx := se.BeginTransaction(RepeatableRead)
	defer tx.Close()

	doc, found, err := tx.Get("items", "id", types.IntKey(1))
	if err != nil || !found || doc != `{"id":1,"value":"v1"}` {
		t.Fatalf("first read: found=%v doc=%q err=%v", found, doc, err)
	}

	if err := se.Put("items", "id", types.IntKey(1), `{"id":1,"value":"v2"}`); err != nil {
		t.Fatalf("concurrent update: %v", err)
	}

	doc, found, err = tx.Get("items", "id", types.IntKey(1))
	if err != nil || !found || doc != `{"id":1,"value":"v1"}` {
		t.Fatalf("second read should stay on snapshot: found=%v doc=%q err=%v", found, doc, err)
	}
}

func TestIsolation_ReadCommittedAllowsPhantom(t *testing.T) {
	se := openAnomalyTestEngine(t)
	defer se.Close()

	if err := se.Put("items", "id", types.IntKey(1), `{"id":1,"value":"a"}`); err != nil {
		t.Fatalf("seed put 1: %v", err)
	}
	if err := se.Put("items", "id", types.IntKey(2), `{"id":2,"value":"b"}`); err != nil {
		t.Fatalf("seed put 2: %v", err)
	}

	tx := se.BeginTransaction(ReadCommitted)
	defer tx.Close()

	rows, err := tx.Scan("items", "id", query.Between(types.IntKey(1), types.IntKey(10)))
	if err != nil {
		t.Fatalf("first scan: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows before phantom, got %d", len(rows))
	}

	if err := se.Put("items", "id", types.IntKey(3), `{"id":3,"value":"c"}`); err != nil {
		t.Fatalf("concurrent insert: %v", err)
	}

	rows, err = tx.Scan("items", "id", query.Between(types.IntKey(1), types.IntKey(10)))
	if err != nil {
		t.Fatalf("second scan: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected phantom under read committed, got %d rows", len(rows))
	}
}

func TestIsolation_RepeatableReadPreventsPhantomRead(t *testing.T) {
	se := openAnomalyTestEngine(t)
	defer se.Close()

	if err := se.Put("items", "id", types.IntKey(1), `{"id":1,"value":"a"}`); err != nil {
		t.Fatalf("seed put 1: %v", err)
	}
	if err := se.Put("items", "id", types.IntKey(2), `{"id":2,"value":"b"}`); err != nil {
		t.Fatalf("seed put 2: %v", err)
	}

	tx := se.BeginTransaction(RepeatableRead)
	defer tx.Close()

	rows, err := tx.Scan("items", "id", query.Between(types.IntKey(1), types.IntKey(10)))
	if err != nil {
		t.Fatalf("first scan: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows before concurrent insert, got %d", len(rows))
	}

	if err := se.Put("items", "id", types.IntKey(3), `{"id":3,"value":"c"}`); err != nil {
		t.Fatalf("concurrent insert: %v", err)
	}

	rows, err = tx.Scan("items", "id", query.Between(types.IntKey(1), types.IntKey(10)))
	if err != nil {
		t.Fatalf("second scan: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("repeatable read should keep same snapshot, got %d rows", len(rows))
	}
}
