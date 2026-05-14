package storage_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func newIteratorEngine(t *testing.T) (*storage.StorageEngine, string) {
	t.Helper()
	tmpDir := t.TempDir()
	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, filepath.Join(tmpDir, "heap.data"))
	if err != nil {
		t.Fatalf("heap: %v", err)
	}
	tm := storage.NewTableMenager()
	if err := tm.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	ww, err := wal.NewWALWriter(filepath.Join(tmpDir, "wal.log"), wal.DefaultOptions())
	if err != nil {
		t.Fatalf("wal: %v", err)
	}
	se, err := storage.NewStorageEngine(tm, ww)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	t.Cleanup(func() { _ = se.Close() })
	return se, tmpDir
}

func TestIterator_FullScanReturnsAllVisibleRowsInKeyOrder(t *testing.T) {
	se, _ := newIteratorEngine(t)
	for i := 1; i <= 5; i++ {
		doc := `{"id":` + itoa(i) + `,"name":"u` + itoa(i) + `"}`
		if err := se.Put(context.Background(), "users", "id", types.IntKey(i), doc); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	tx := se.BeginRead()
	defer tx.Close()

	it, err := tx.NewIterator(context.Background(), "users", "id", storage.IterOptions{})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	defer it.Close()

	var keys []int
	for it.Next() {
		k, ok := it.Key().(types.IntKey)
		if !ok {
			t.Fatalf("unexpected key type %T", it.Key())
		}
		keys = append(keys, int(k))
		if len(it.Value()) == 0 {
			t.Fatalf("Value at key %d returned empty bytes", k)
		}
		if it.LSN() == 0 {
			t.Fatalf("LSN at key %d returned 0", k)
		}
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}

	want := []int{1, 2, 3, 4, 5}
	if len(keys) != len(want) {
		t.Fatalf("got %v want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("pos %d: got %d want %d", i, keys[i], want[i])
		}
	}
}

func TestIterator_RangeRespectsLowerUpperInclusive(t *testing.T) {
	se, _ := newIteratorEngine(t)
	for i := 1; i <= 10; i++ {
		doc := `{"id":` + itoa(i) + `}`
		if err := se.Put(context.Background(), "users", "id", types.IntKey(i), doc); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	tx := se.BeginRead()
	defer tx.Close()

	it, err := tx.NewIterator(context.Background(), "users", "id", storage.IterOptions{
		Lower: types.IntKey(3),
		Upper: types.IntKey(7),
	})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	defer it.Close()

	var keys []int
	for it.Next() {
		keys = append(keys, int(it.Key().(types.IntKey)))
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}

	want := []int{3, 4, 5, 6, 7}
	if len(keys) != len(want) {
		t.Fatalf("got %v want %v", keys, want)
	}
}

// TestIterator_SnapshotIsolation locks in the MVCC contract: the iterator
// captures the transaction's snapshot when it is created; concurrent
// commits after Begin must NOT appear in the iterator's results.
func TestIterator_SnapshotIsolation(t *testing.T) {
	se, _ := newIteratorEngine(t)
	if err := se.Put(context.Background(), "users", "id", types.IntKey(1), `{"id":1}`); err != nil {
		t.Fatalf("Put 1: %v", err)
	}

	tx := se.BeginRead()
	defer tx.Close()

	// Insert a new row AFTER the snapshot is captured.
	if err := se.Put(context.Background(), "users", "id", types.IntKey(2), `{"id":2}`); err != nil {
		t.Fatalf("Put 2: %v", err)
	}

	it, err := tx.NewIterator(context.Background(), "users", "id", storage.IterOptions{})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	defer it.Close()

	var keys []int
	for it.Next() {
		keys = append(keys, int(it.Key().(types.IntKey)))
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}

	if len(keys) != 1 || keys[0] != 1 {
		t.Fatalf("snapshot leak: got %v want [1]", keys)
	}
}

func TestIterator_CloseIsIdempotentAndAllowsConcurrentWrites(t *testing.T) {
	se, _ := newIteratorEngine(t)
	for i := 1; i <= 3; i++ {
		_ = se.Put(context.Background(), "users", "id", types.IntKey(i), `{"id":`+itoa(i)+`}`)
	}

	tx := se.BeginRead()
	defer tx.Close()

	it, err := tx.NewIterator(context.Background(), "users", "id", storage.IterOptions{})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	if !it.Next() {
		t.Fatalf("Next: false, err=%v", it.Err())
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Double-close must not error.
	if err := it.Close(); err != nil {
		t.Fatalf("Close again: %v", err)
	}
	// A writer can immediately make progress now that the iterator
	// dropped its leaf latch.
	if err := se.Put(context.Background(), "users", "id", types.IntKey(99), `{"id":99}`); err != nil {
		t.Fatalf("Put after iterator close: %v", err)
	}
}

// TestIterator_TransactionCloseForceClosesOpenIterators is the defensive
// contract: a forgotten Close on the iterator is reclaimed when the owning
// transaction is closed (see spec: "Transaction.Close() força Close() em
// todos os iteradores ainda abertos").
func TestIterator_TransactionCloseForceClosesOpenIterators(t *testing.T) {
	se, _ := newIteratorEngine(t)
	if err := se.Put(context.Background(), "users", "id", types.IntKey(1), `{"id":1}`); err != nil {
		t.Fatalf("Put: %v", err)
	}

	tx := se.BeginRead()
	it, err := tx.NewIterator(context.Background(), "users", "id", storage.IterOptions{})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	if !it.Next() {
		t.Fatalf("Next: false, err=%v", it.Err())
	}

	// Caller "forgets" Close — transaction Close should rescue.
	tx.Close()

	// Now a writer on the same leaf must not deadlock.
	if err := se.Put(context.Background(), "users", "id", types.IntKey(2), `{"id":2}`); err != nil {
		t.Fatalf("Put after tx close: %v", err)
	}
}

func TestIterator_ValueReturnsRawHeapBytesNotJSONRoundTrip(t *testing.T) {
	se, _ := newIteratorEngine(t)
	if err := se.Put(context.Background(), "users", "id", types.IntKey(1), `{"id":1,"x":42}`); err != nil {
		t.Fatalf("Put: %v", err)
	}

	tx := se.BeginRead()
	defer tx.Close()

	it, err := tx.NewIterator(context.Background(), "users", "id", storage.IterOptions{})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	defer it.Close()
	if !it.Next() {
		t.Fatalf("Next: false, err=%v", it.Err())
	}

	// Value() returns the raw heap payload (BSON for the default codec).
	// We don't assert on the exact format — just that it isn't the JSON
	// round-trip a legacy Get would have produced.
	raw := it.Value()
	if strings.HasPrefix(string(raw), `{"`) {
		t.Fatalf("Value returned JSON text; expected raw heap bytes: %q", raw)
	}
}
