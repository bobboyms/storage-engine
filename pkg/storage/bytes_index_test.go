package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// TestBytesKey_PrimaryIndexEndToEnd creates a table keyed by a binary
// (BytesKey) primary index, writes and reads a row, then reopens the
// engine to confirm the value survives WAL replay — exercising the btree
// codec, key serialization, and type resolution for the new key type.
func TestBytesKey_PrimaryIndexEndToEnd(t *testing.T) {
	dir := t.TempDir()
	heapPath := filepath.Join(dir, "blobs.heap")
	walPath := filepath.Join(dir, "blobs.wal")
	ctx := context.Background()

	key := types.BytesKey{0x00, 0xde, 0xad, 0xbe, 0xef}
	// A non-JSON payload so Put uses the explicit BytesKey directly
	// rather than extracting the key from a document field (binary
	// document-field extraction is a separate codec concern).
	doc := "deadbeef-blob-payload-not-json"

	open := func() *StorageEngine {
		hm, err := NewHeapForTable(HeapFormatV2, heapPath)
		if err != nil {
			t.Fatalf("heap: %v", err)
		}
		meta := NewTableMenager()
		if err := meta.NewTable("blobs", []Index{
			{Name: "id", Primary: true, Type: TypeBytes},
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
	if err := se.Put(ctx, "blobs", "id", key, doc); err != nil {
		t.Fatalf("Put: %v", err)
	}
	raw, found, err := se.GetBytes(ctx, "blobs", "id", key)
	if err != nil || !found {
		t.Fatalf("Get before reopen: found=%v err=%v", found, err)
	}
	if got := legacyDecode(t, raw); got != doc {
		t.Fatalf("value mismatch before reopen: %q", got)
	}
	if err := se.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen → WAL replay must restore the binary-keyed row.
	se2 := open()
	defer se2.Close()
	raw, found, err = se2.GetBytes(ctx, "blobs", "id", key)
	if err != nil || !found {
		t.Fatalf("Get after reopen: found=%v err=%v", found, err)
	}
	if got := legacyDecode(t, raw); got != doc {
		t.Fatalf("value mismatch after reopen: %q", got)
	}

	// A different binary key must not collide.
	if _, found, _ := se2.GetBytes(ctx, "blobs", "id", types.BytesKey{0x01}); found {
		t.Fatalf("unexpected hit for absent key")
	}
}
