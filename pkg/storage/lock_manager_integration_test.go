package storage

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func TestWriteTransaction_DeadlockVictimReleasesLocks(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("new heap: %v", err)
	}

	tableMgr := NewTableMenager()
	if err := tableMgr.NewTable("users", []Index{{Name: "id", Primary: true, Type: TypeInt}}, 4, hm); err != nil {
		t.Fatalf("new table: %v", err)
	}

	walWriter, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}

	se, err := NewStorageEngine(tableMgr, walWriter)
	if err != nil {
		t.Fatalf("new storage engine: %v", err)
	}
	defer se.Close()

	se.LockManager = NewLockManager(LockManagerConfig{
		WaitTimeout: 250 * time.Millisecond,
	})

	tx1 := se.BeginWriteTransaction()
	tx2 := se.BeginWriteTransaction()

	if err := tx1.Put("users", "id", types.IntKey(1), `{"id":1,"owner":"tx1"}`); err != nil {
		t.Fatalf("tx1 put key1: %v", err)
	}
	if err := tx2.Put("users", "id", types.IntKey(2), `{"id":2,"owner":"tx2"}`); err != nil {
		t.Fatalf("tx2 put key2: %v", err)
	}

	tx1ErrCh := make(chan error, 1)
	go func() {
		tx1ErrCh <- tx1.Put("users", "id", types.IntKey(2), `{"id":2,"owner":"tx1"}`)
	}()

	time.Sleep(20 * time.Millisecond)

	tx2Err := tx2.Put("users", "id", types.IntKey(1), `{"id":1,"owner":"tx2"}`)
	if !errors.Is(tx2Err, ErrDeadlockVictim) {
		t.Fatalf("expected deadlock victim error, got %v", tx2Err)
	}

	select {
	case err := <-tx1ErrCh:
		if err != nil {
			t.Fatalf("tx1 should survive deadlock, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("tx1 did not acquire second key after deadlock victim abort")
	}

	if err := tx1.Commit(); err != nil {
		t.Fatalf("tx1 commit: %v", err)
	}
	if err := tx2.Commit(); !errors.Is(err, ErrDeadlockVictim) {
		t.Fatalf("expected tx2 commit to report deadlock victim, got %v", err)
	}

	doc, found, err := se.Get("users", "id", types.IntKey(1))
	if err != nil {
		t.Fatalf("get key1: %v", err)
	}
	if !found || doc != `{"id":1,"owner":"tx1"}` {
		t.Fatalf("unexpected key1 state: found=%v doc=%q", found, doc)
	}

	doc, found, err = se.Get("users", "id", types.IntKey(2))
	if err != nil {
		t.Fatalf("get key2: %v", err)
	}
	if !found || doc != `{"id":2,"owner":"tx1"}` {
		t.Fatalf("unexpected key2 state: found=%v doc=%q", found, doc)
	}

	tx3 := se.BeginWriteTransaction()
	if err := tx3.Put("users", "id", types.IntKey(1), `{"id":1,"owner":"tx3"}`); err != nil {
		t.Fatalf("tx3 should acquire key1 after victim cleanup: %v", err)
	}
	if err := tx3.Rollback(); err != nil {
		t.Fatalf("tx3 rollback: %v", err)
	}
}

func TestStorageEngine_PutWaitsForLogicalKeyLock(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("new heap: %v", err)
	}

	tableMgr := NewTableMenager()
	if err := tableMgr.NewTable("users", []Index{{Name: "id", Primary: true, Type: TypeInt}}, 4, hm); err != nil {
		t.Fatalf("new table: %v", err)
	}

	walWriter, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}

	se, err := NewStorageEngine(tableMgr, walWriter)
	if err != nil {
		t.Fatalf("new storage engine: %v", err)
	}
	defer se.Close()

	se.LockManager = NewLockManager(LockManagerConfig{
		WaitTimeout: 250 * time.Millisecond,
	})

	resource, err := lockResourceForKey("users", "id", types.IntKey(1))
	if err != nil {
		t.Fatalf("resource key: %v", err)
	}
	if err := se.LockManager.Acquire(9001, resource); err != nil {
		t.Fatalf("pre-acquire logical key lock: %v", err)
	}

	putDone := make(chan error, 1)
	go func() {
		putDone <- se.Put("users", "id", types.IntKey(1), `{"id":1,"owner":"autocommit"}`)
	}()

	select {
	case err := <-putDone:
		t.Fatalf("Put returned while logical lock was still held: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	se.LockManager.ReleaseAll(9001)

	select {
	case err := <-putDone:
		if err != nil {
			t.Fatalf("Put failed after lock release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Put stayed blocked after logical lock release")
	}
}
