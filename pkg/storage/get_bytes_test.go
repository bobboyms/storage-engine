package storage_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func newGetBytesEngine(t *testing.T) *storage.StorageEngine {
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
	return se
}

func TestGetBytes_ReturnsRawHeapPayload(t *testing.T) {
	se := newGetBytesEngine(t)
	if err := se.Put("users", "id", types.IntKey(1), `{"id":1,"name":"alice"}`); err != nil {
		t.Fatalf("Put: %v", err)
	}

	raw, found, err := se.GetBytes("users", "id", types.IntKey(1))
	if err != nil {
		t.Fatalf("GetBytes: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}
	if len(raw) == 0 {
		t.Fatal("expected non-empty bytes")
	}
	// The default codec stores BSON; raw bytes must not look like the
	// JSON round-trip that legacy Get returns.
	if strings.HasPrefix(string(raw), `{"`) {
		t.Fatalf("GetBytes returned JSON text; expected raw heap bytes: %q", raw)
	}
}

func TestGetBytes_MissingKeyReturnsFalseNoError(t *testing.T) {
	se := newGetBytesEngine(t)
	raw, found, err := se.GetBytes("users", "id", types.IntKey(42))
	if err != nil {
		t.Fatalf("GetBytes: %v", err)
	}
	if found {
		t.Fatal("expected found=false")
	}
	if raw != nil {
		t.Fatalf("expected nil bytes, got %q", raw)
	}
}

func TestGetBytes_TransactionRespectsSnapshot(t *testing.T) {
	se := newGetBytesEngine(t)
	if err := se.Put("users", "id", types.IntKey(1), `{"id":1,"v":"old"}`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tx := se.BeginRead()
	defer tx.Close()

	// Overwrite after snapshot capture.
	if err := se.Put("users", "id", types.IntKey(1), `{"id":1,"v":"new"}`); err != nil {
		t.Fatalf("overwrite: %v", err)
	}

	raw, found, err := tx.GetBytes("users", "id", types.IntKey(1))
	if err != nil || !found {
		t.Fatalf("GetBytes: found=%v err=%v", found, err)
	}
	if !strings.Contains(string(raw), "old") {
		t.Fatalf("snapshot leak: tx saw new value: %q", raw)
	}
}
