package storage

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func openIsolationTestEngine(t *testing.T) *StorageEngine {
	t.Helper()

	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("new heap: %v", err)
	}

	tableMgr := NewTableMenager()
	if err := tableMgr.NewTable("accounts", []Index{{Name: "id", Primary: true, Type: TypeInt}}, 4, hm); err != nil {
		t.Fatalf("new table accounts: %v", err)
	}
	if err := tableMgr.NewTable("shifts", []Index{{Name: "id", Primary: true, Type: TypeInt}}, 4, hm); err != nil {
		t.Fatalf("new table shifts: %v", err)
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

func TestWriteTransaction_ReadsOwnPendingWrites(t *testing.T) {
	se := openIsolationTestEngine(t)
	defer se.Close()

	tx := se.BeginWriteTransaction()
	if err := tx.Put("accounts", "id", types.IntKey(1), `{"id":1,"balance":100}`); err != nil {
		t.Fatalf("tx put: %v", err)
	}

	doc, found, err := tx.Get("accounts", "id", types.IntKey(1))
	if err != nil {
		t.Fatalf("tx get own pending write: %v", err)
	}
	if !found || doc != `{"id":1,"balance":100}` {
		t.Fatalf("expected own pending write, found=%v doc=%q", found, doc)
	}

	if err := tx.Del("accounts", "id", types.IntKey(1)); err != nil {
		t.Fatalf("tx delete own pending write: %v", err)
	}

	doc, found, err = tx.Get("accounts", "id", types.IntKey(1))
	if err != nil {
		t.Fatalf("tx get own pending delete: %v", err)
	}
	if found || doc != "" {
		t.Fatalf("expected own pending delete to hide row, found=%v doc=%q", found, doc)
	}
}

func TestWriteTransaction_PreventsLostUpdateAfterStaleRead(t *testing.T) {
	se := openIsolationTestEngine(t)
	defer se.Close()

	if err := se.Put("accounts", "id", types.IntKey(1), `{"id":1,"balance":100}`); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	tx1 := se.BeginWriteTransactionWithIsolation(RepeatableRead)
	tx2 := se.BeginWriteTransactionWithIsolation(RepeatableRead)

	doc, found, err := tx1.Get("accounts", "id", types.IntKey(1))
	if err != nil || !found || doc != `{"id":1,"balance":100}` {
		t.Fatalf("tx1 initial read: found=%v doc=%q err=%v", found, doc, err)
	}
	doc, found, err = tx2.Get("accounts", "id", types.IntKey(1))
	if err != nil || !found || doc != `{"id":1,"balance":100}` {
		t.Fatalf("tx2 initial read: found=%v doc=%q err=%v", found, doc, err)
	}

	if err := tx1.Put("accounts", "id", types.IntKey(1), `{"id":1,"balance":150}`); err != nil {
		t.Fatalf("tx1 put: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("tx1 commit: %v", err)
	}

	err = tx2.Put("accounts", "id", types.IntKey(1), `{"id":1,"balance":50}`)
	if !errors.Is(err, ErrSerializationConflict) {
		t.Fatalf("expected serialization conflict, got %v", err)
	}

	doc, found, err = se.Get("accounts", "id", types.IntKey(1))
	if err != nil || !found || doc != `{"id":1,"balance":150}` {
		t.Fatalf("final state mismatch: found=%v doc=%q err=%v", found, doc, err)
	}
}

func TestWriteTransaction_RepeatableReadStillAllowsWriteSkew(t *testing.T) {
	se := openIsolationTestEngine(t)
	defer se.Close()

	if err := se.Put("shifts", "id", types.IntKey(1), `{"id":1,"on_call":true}`); err != nil {
		t.Fatalf("seed shift 1: %v", err)
	}
	if err := se.Put("shifts", "id", types.IntKey(2), `{"id":2,"on_call":true}`); err != nil {
		t.Fatalf("seed shift 2: %v", err)
	}

	tx1 := se.BeginWriteTransactionWithIsolation(RepeatableRead)
	tx2 := se.BeginWriteTransactionWithIsolation(RepeatableRead)

	doc1a, found, err := tx1.Get("shifts", "id", types.IntKey(1))
	if err != nil || !found {
		t.Fatalf("tx1 read row1: found=%v err=%v", found, err)
	}
	doc1b, found, err := tx1.Get("shifts", "id", types.IntKey(2))
	if err != nil || !found {
		t.Fatalf("tx1 read row2: found=%v err=%v", found, err)
	}
	doc2a, found, err := tx2.Get("shifts", "id", types.IntKey(1))
	if err != nil || !found {
		t.Fatalf("tx2 read row1: found=%v err=%v", found, err)
	}
	doc2b, found, err := tx2.Get("shifts", "id", types.IntKey(2))
	if err != nil || !found {
		t.Fatalf("tx2 read row2: found=%v err=%v", found, err)
	}

	if doc1a != `{"id":1,"on_call":true}` || doc1b != `{"id":2,"on_call":true}` {
		t.Fatalf("tx1 unexpected snapshot: %q / %q", doc1a, doc1b)
	}
	if doc2a != `{"id":1,"on_call":true}` || doc2b != `{"id":2,"on_call":true}` {
		t.Fatalf("tx2 unexpected snapshot: %q / %q", doc2a, doc2b)
	}

	if err := tx1.Put("shifts", "id", types.IntKey(1), `{"id":1,"on_call":false}`); err != nil {
		t.Fatalf("tx1 put row1: %v", err)
	}
	if err := tx2.Put("shifts", "id", types.IntKey(2), `{"id":2,"on_call":false}`); err != nil {
		t.Fatalf("tx2 put row2: %v", err)
	}

	if err := tx1.Commit(); err != nil {
		t.Fatalf("tx1 commit: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("tx2 commit: %v", err)
	}

	doc1a, found, err = se.Get("shifts", "id", types.IntKey(1))
	if err != nil || !found {
		t.Fatalf("final get row1: found=%v err=%v", found, err)
	}
	doc1b, found, err = se.Get("shifts", "id", types.IntKey(2))
	if err != nil || !found {
		t.Fatalf("final get row2: found=%v err=%v", found, err)
	}

	if doc1a != `{"id":1,"on_call":false}` || doc1b != `{"id":2,"on_call":false}` {
		t.Fatalf("expected write skew final state, got %q / %q", doc1a, doc1b)
	}
}
