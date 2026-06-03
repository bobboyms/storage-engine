package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// TestUUIDKey_PrimaryIndexEndToEnd exercises a UUID-keyed primary index
// through write, read, and WAL replay on reopen.
func TestUUIDKey_PrimaryIndexEndToEnd(t *testing.T) {
	dir := t.TempDir()
	heapPath := filepath.Join(dir, "ent.heap")
	walPath := filepath.Join(dir, "ent.wal")
	ctx := context.Background()

	key, err := types.ParseUUID("550e8400-e29b-41d4-a716-446655440000")
	if err != nil {
		t.Fatalf("ParseUUID: %v", err)
	}
	doc := "uuid-keyed-payload-not-json"

	open := func() *StorageEngine {
		hm, err := NewHeapForTable(HeapFormatV2, heapPath)
		if err != nil {
			t.Fatalf("heap: %v", err)
		}
		meta := NewTableMenager()
		if err := meta.NewTable("ent", []Index{
			{Name: "id", Primary: true, Type: TypeUUID},
		}, 0, hm); err != nil {
			t.Fatalf("NewTable: %v", err)
		}
		ww, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
		if err != nil {
			t.Fatalf("wal: %v", err)
		}
		se, err := NewProductionStorageEngine(meta, ww)
		if err != nil {
			t.Fatalf("engine: %v", err)
		}
		return se
	}

	se := open()
	if err := se.Put(ctx, "ent", "id", key, doc); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, found, err := se.GetBytes(ctx, "ent", "id", key); err != nil || !found {
		t.Fatalf("Get before reopen: found=%v err=%v", found, err)
	}
	if err := se.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	se2 := open()
	defer se2.Close()
	raw, found, err := se2.GetBytes(ctx, "ent", "id", key)
	if err != nil || !found {
		t.Fatalf("Get after reopen: found=%v err=%v", found, err)
	}
	if got := legacyDecode(t, raw); got != doc {
		t.Fatalf("value mismatch after reopen: %q", got)
	}

	other, _ := types.ParseUUID("00000000-0000-0000-0000-000000000001")
	if _, found, _ := se2.GetBytes(ctx, "ent", "id", other); found {
		t.Fatalf("unexpected hit for absent UUID")
	}
}
