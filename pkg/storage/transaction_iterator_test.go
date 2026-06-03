package storage

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// collectTxScan returns the (key,doc) pairs a write transaction sees when
// scanning indexName over [lo, hi].
func collectTxScan(t *testing.T, tx *WriteTransaction, table, indexName string, lo, hi types.Comparable) ([]int64, []string) {
	t.Helper()
	it, err := tx.NewIterator(context.Background(), table, indexName, IterOptions{Lower: lo, Upper: hi})
	if err != nil {
		t.Fatalf("tx.NewIterator: %v", err)
	}
	defer it.Close()
	var keys []int64
	var docs []string
	for it.Next() {
		k, ok := it.Key().(types.IntKey)
		if !ok {
			t.Fatalf("key = %T, want IntKey", it.Key())
		}
		keys = append(keys, int64(k))
		docs = append(docs, legacyDecode(t, it.Value()))
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator err: %v", err)
	}
	return keys, docs
}

func seedCommitted(t *testing.T, se *StorageEngine, id, age int, doc string) {
	t.Helper()
	if err := se.InsertRow(context.Background(), "users", doc, writeRowKeys(id, age)); err != nil {
		t.Fatalf("seed InsertRow id=%d: %v", id, err)
	}
}

func TestTxIterator_SeesBufferedInsert(t *testing.T) {
	tmp := t.TempDir()
	se := buildWriteRowEngine(t, filepath.Join(tmp, "h"), filepath.Join(tmp, "w"))
	defer se.Close()
	seedCommitted(t, se, 1, 30, `{"id":1,"age":30}`)

	tx := se.BeginWriteTransaction()
	defer tx.Rollback(context.Background())
	if err := tx.WriteRow(context.Background(), "users", `{"id":2,"age":40}`, writeRowKeys(2, 40), true); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}

	keys, _ := collectTxScan(t, tx, "users", "id", nil, nil)
	if len(keys) != 2 || keys[0] != 1 || keys[1] != 2 {
		t.Fatalf("keys = %v, want [1 2] (committed + buffered insert)", keys)
	}
}

func TestTxIterator_SeesBufferedUpdate(t *testing.T) {
	tmp := t.TempDir()
	se := buildWriteRowEngine(t, filepath.Join(tmp, "h"), filepath.Join(tmp, "w"))
	defer se.Close()
	seedCommitted(t, se, 1, 30, `{"id":1,"v":"old","age":30}`)

	tx := se.BeginWriteTransaction()
	defer tx.Rollback(context.Background())
	if err := tx.WriteRow(context.Background(), "users", `{"id":1,"v":"new","age":30}`, writeRowKeys(1, 30), false); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}

	keys, docs := collectTxScan(t, tx, "users", "id", nil, nil)
	if len(keys) != 1 || keys[0] != 1 {
		t.Fatalf("keys = %v, want [1]", keys)
	}
	if !strings.Contains(docs[0], "new") {
		t.Fatalf("doc = %q, want buffered value 'new'", docs[0])
	}
}

func TestTxIterator_ExcludesBufferedDelete(t *testing.T) {
	tmp := t.TempDir()
	se := buildWriteRowEngine(t, filepath.Join(tmp, "h"), filepath.Join(tmp, "w"))
	defer se.Close()
	seedCommitted(t, se, 1, 30, `{"id":1,"age":30}`)
	seedCommitted(t, se, 2, 40, `{"id":2,"age":40}`)

	tx := se.BeginWriteTransaction()
	defer tx.Rollback(context.Background())
	if err := tx.Del(context.Background(), "users", "id", types.IntKey(1)); err != nil {
		t.Fatalf("Del: %v", err)
	}

	keys, _ := collectTxScan(t, tx, "users", "id", nil, nil)
	if len(keys) != 1 || keys[0] != 2 {
		t.Fatalf("keys = %v, want [2] (1 deleted in tx)", keys)
	}
}

func TestTxIterator_Ordering(t *testing.T) {
	tmp := t.TempDir()
	se := buildWriteRowEngine(t, filepath.Join(tmp, "h"), filepath.Join(tmp, "w"))
	defer se.Close()
	seedCommitted(t, se, 1, 30, `{"id":1,"age":30}`)
	seedCommitted(t, se, 3, 50, `{"id":3,"age":50}`)

	tx := se.BeginWriteTransaction()
	defer tx.Rollback(context.Background())
	if err := tx.WriteRow(context.Background(), "users", `{"id":2,"age":40}`, writeRowKeys(2, 40), true); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}

	keys, _ := collectTxScan(t, tx, "users", "id", nil, nil)
	want := []int64{1, 2, 3}
	if len(keys) != len(want) {
		t.Fatalf("keys = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("keys = %v, want sorted %v", keys, want)
		}
	}
}

func TestTxIterator_Bounds(t *testing.T) {
	tmp := t.TempDir()
	se := buildWriteRowEngine(t, filepath.Join(tmp, "h"), filepath.Join(tmp, "w"))
	defer se.Close()
	seedCommitted(t, se, 1, 30, `{"id":1,"age":30}`)
	seedCommitted(t, se, 5, 70, `{"id":5,"age":70}`)

	tx := se.BeginWriteTransaction()
	defer tx.Rollback(context.Background())
	// Buffered inserts: one inside the bound, one outside.
	if err := tx.WriteRow(context.Background(), "users", `{"id":3,"age":40}`, writeRowKeys(3, 40), true); err != nil {
		t.Fatalf("WriteRow 3: %v", err)
	}
	if err := tx.WriteRow(context.Background(), "users", `{"id":9,"age":90}`, writeRowKeys(9, 90), true); err != nil {
		t.Fatalf("WriteRow 9: %v", err)
	}

	keys, _ := collectTxScan(t, tx, "users", "id", types.IntKey(2), types.IntKey(5))
	want := []int64{3, 5}
	if len(keys) != len(want) {
		t.Fatalf("keys = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("keys = %v, want %v", keys, want)
		}
	}
}
