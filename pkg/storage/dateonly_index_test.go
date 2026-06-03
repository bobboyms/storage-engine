package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// TestDateOnlyKey_PrimaryIndexEndToEnd exercises a DATE (calendar-only)
// primary index through write, read, and WAL replay on reopen.
func TestDateOnlyKey_PrimaryIndexEndToEnd(t *testing.T) {
	dir := t.TempDir()
	heapPath := filepath.Join(dir, "evt.heap")
	walPath := filepath.Join(dir, "evt.wal")
	ctx := context.Background()

	key, err := types.ParseDateOnly("2024-06-01")
	if err != nil {
		t.Fatalf("ParseDateOnly: %v", err)
	}
	doc := "date-keyed-payload-not-json"

	open := func() *StorageEngine {
		hm, err := NewHeapForTable(HeapFormatV2, heapPath)
		if err != nil {
			t.Fatalf("heap: %v", err)
		}
		meta := NewTableMenager()
		if err := meta.NewTable("evt", []Index{
			{Name: "day", Primary: true, Type: TypeDateOnly},
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
	if err := se.Put(ctx, "evt", "day", key, doc); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := se.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	se2 := open()
	defer se2.Close()
	raw, found, err := se2.GetBytes(ctx, "evt", "day", key)
	if err != nil || !found {
		t.Fatalf("Get after reopen: found=%v err=%v", found, err)
	}
	if got := legacyDecode(t, raw); got != doc {
		t.Fatalf("value mismatch after reopen: %q", got)
	}
	other, _ := types.ParseDateOnly("2024-06-02")
	if _, found, _ := se2.GetBytes(ctx, "evt", "day", other); found {
		t.Fatalf("unexpected hit for absent date")
	}
}
