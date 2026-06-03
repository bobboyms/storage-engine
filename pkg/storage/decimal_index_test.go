package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// TestDecimalKey_PrimaryIndexEndToEnd exercises an exact-decimal primary
// index through write, read, and WAL replay on reopen.
func TestDecimalKey_PrimaryIndexEndToEnd(t *testing.T) {
	dir := t.TempDir()
	heapPath := filepath.Join(dir, "acct.heap")
	walPath := filepath.Join(dir, "acct.wal")
	ctx := context.Background()

	key, err := types.ParseDecimal("12345.6789")
	if err != nil {
		t.Fatalf("ParseDecimal: %v", err)
	}
	doc := "decimal-keyed-payload-not-json"

	open := func() *StorageEngine {
		hm, err := NewHeapForTable(HeapFormatV2, heapPath)
		if err != nil {
			t.Fatalf("heap: %v", err)
		}
		meta := NewTableMenager()
		if err := meta.NewTable("acct", []Index{
			{Name: "bal", Primary: true, Type: TypeDecimal},
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
	if err := se.Put(ctx, "acct", "bal", key, doc); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, found, err := se.GetBytes(ctx, "acct", "bal", key); err != nil || !found {
		t.Fatalf("Get before reopen: found=%v err=%v", found, err)
	}
	if err := se.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	se2 := open()
	defer se2.Close()
	raw, found, err := se2.GetBytes(ctx, "acct", "bal", key)
	if err != nil || !found {
		t.Fatalf("Get after reopen: found=%v err=%v", found, err)
	}
	if got := legacyDecode(t, raw); got != doc {
		t.Fatalf("value mismatch after reopen: %q", got)
	}

	// An equal decimal with different scale (12345.67890) must resolve to
	// the same row, proving exact-decimal key equality end-to-end.
	sameVal, _ := types.ParseDecimal("12345.67890")
	if _, found, _ := se2.GetBytes(ctx, "acct", "bal", sameVal); !found {
		t.Fatalf("scale-different equal decimal should hit the same key")
	}
	other, _ := types.ParseDecimal("0.01")
	if _, found, _ := se2.GetBytes(ctx, "acct", "bal", other); found {
		t.Fatalf("unexpected hit for absent decimal key")
	}
}
