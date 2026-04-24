package storage_test

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func newMultiIndexEngine(t *testing.T) (*storage.StorageEngine, func()) {
	t.Helper()

	dir := t.TempDir()
	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, filepath.Join(dir, "users.heap"))
	if err != nil {
		t.Fatalf("NewHeapForTable: %v", err)
	}
	tm := storage.NewTableMenager()
	if err := tm.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
		{Name: "email", Primary: false, Type: storage.TypeVarchar},
	}, 0, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	ww, err := wal.NewWALWriter(filepath.Join(dir, "users.wal"), wal.DefaultOptions())
	if err != nil {
		t.Fatalf("NewWALWriter: %v", err)
	}
	se, err := storage.NewStorageEngine(tm, ww)
	if err != nil {
		_ = ww.Close()
		t.Fatalf("NewStorageEngine: %v", err)
	}
	return se, func() { _ = se.Close() }
}

func TestUpsertRowMaintainsSecondaryIndexesWhenIndexedFieldChanges(t *testing.T) {
	se, cleanup := newMultiIndexEngine(t)
	defer cleanup()

	if err := se.InsertRow("users", `{"id":1,"email":"old@example.com","name":"Alice"}`, map[string]types.Comparable{
		"id":    types.IntKey(1),
		"email": types.VarcharKey("old@example.com"),
	}); err != nil {
		t.Fatalf("InsertRow: %v", err)
	}

	if err := se.UpsertRow("users", `{"id":1,"email":"new@example.com","name":"Alice Updated"}`, map[string]types.Comparable{
		"id":    types.IntKey(1),
		"email": types.VarcharKey("new@example.com"),
	}); err != nil {
		t.Fatalf("UpsertRow: %v", err)
	}

	got, found, err := se.Get("users", "id", types.IntKey(1))
	if err != nil || !found || got != `{"id":1,"email":"new@example.com","name":"Alice Updated"}` {
		t.Fatalf("primary lookup: found=%v got=%q err=%v", found, got, err)
	}

	got, found, err = se.Get("users", "email", types.VarcharKey("new@example.com"))
	if err != nil || !found || got != `{"id":1,"email":"new@example.com","name":"Alice Updated"}` {
		t.Fatalf("new secondary lookup: found=%v got=%q err=%v", found, got, err)
	}

	if got, found, err = se.Get("users", "email", types.VarcharKey("old@example.com")); err != nil || found {
		t.Fatalf("old secondary key should not be visible: found=%v got=%q err=%v", found, got, err)
	}
}

func TestPutWithFullJSONMaintainsAllIndexes(t *testing.T) {
	se, cleanup := newMultiIndexEngine(t)
	defer cleanup()

	if err := se.Put("users", "id", types.IntKey(7), `{"id":7,"email":"one@example.com","name":"One"}`); err != nil {
		t.Fatalf("Put insert: %v", err)
	}
	if err := se.Put("users", "id", types.IntKey(7), `{"id":7,"email":"two@example.com","name":"Two"}`); err != nil {
		t.Fatalf("Put update: %v", err)
	}

	if _, found, err := se.Get("users", "email", types.VarcharKey("one@example.com")); err != nil || found {
		t.Fatalf("old email should not be visible after Put update: found=%v err=%v", found, err)
	}
	got, found, err := se.Get("users", "email", types.VarcharKey("two@example.com"))
	if err != nil || !found || got != `{"id":7,"email":"two@example.com","name":"Two"}` {
		t.Fatalf("new email lookup: found=%v got=%q err=%v", found, got, err)
	}
}

func TestInsertRowDuplicatePrimaryKeyRace(t *testing.T) {
	se, cleanup := newMultiIndexEngine(t)
	defer cleanup()

	const workers = 16
	var successes int32
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			err := se.InsertRow("users", `{"id":99,"email":"race@example.com","name":"Race"}`, map[string]types.Comparable{
				"id":    types.IntKey(99),
				"email": types.VarcharKey("race@example.com"),
			})
			if err == nil {
				atomic.AddInt32(&successes, 1)
			}
		}()
	}
	wg.Wait()

	if successes != 1 {
		t.Fatalf("expected exactly one successful insert, got %d", successes)
	}
	got, found, err := se.Get("users", "id", types.IntKey(99))
	if err != nil || !found || got == "" {
		t.Fatalf("inserted row missing: found=%v got=%q err=%v", found, got, err)
	}
}

func TestMultiIndexUpsertRecoveryMaintainsChangedSecondaryKey(t *testing.T) {
	dir := t.TempDir()
	heapPath := filepath.Join(dir, "users.heap")
	walPath := filepath.Join(dir, "users.wal")

	open := func(t *testing.T) *storage.StorageEngine {
		t.Helper()
		hm, err := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
		if err != nil {
			t.Fatalf("NewHeapForTable: %v", err)
		}
		tm := storage.NewTableMenager()
		if err := tm.NewTable("users", []storage.Index{
			{Name: "id", Primary: true, Type: storage.TypeInt},
			{Name: "email", Primary: false, Type: storage.TypeVarchar},
		}, 0, hm); err != nil {
			t.Fatalf("NewTable: %v", err)
		}
		ww, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
		if err != nil {
			t.Fatalf("NewWALWriter: %v", err)
		}
		se, err := storage.NewProductionStorageEngine(tm, ww)
		if err != nil {
			_ = ww.Close()
			t.Fatalf("NewProductionStorageEngine: %v", err)
		}
		return se
	}

	se := open(t)
	if err := se.InsertRow("users", `{"id":5,"email":"before@example.com","name":"Before"}`, map[string]types.Comparable{
		"id":    types.IntKey(5),
		"email": types.VarcharKey("before@example.com"),
	}); err != nil {
		t.Fatalf("InsertRow: %v", err)
	}
	if err := se.UpsertRow("users", `{"id":5,"email":"after@example.com","name":"After"}`, map[string]types.Comparable{
		"id":    types.IntKey(5),
		"email": types.VarcharKey("after@example.com"),
	}); err != nil {
		t.Fatalf("UpsertRow: %v", err)
	}
	if err := se.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	recovered := open(t)
	defer recovered.Close()

	if _, found, err := recovered.Get("users", "email", types.VarcharKey("before@example.com")); err != nil || found {
		t.Fatalf("old secondary key visible after recovery: found=%v err=%v", found, err)
	}
	got, found, err := recovered.Get("users", "email", types.VarcharKey("after@example.com"))
	if err != nil || !found || got != `{"id":5,"email":"after@example.com","name":"After"}` {
		t.Fatalf("new secondary key after recovery: found=%v got=%q err=%v", found, got, err)
	}
}
