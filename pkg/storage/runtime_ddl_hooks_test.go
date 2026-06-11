package storage

import (
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// pageRedoPathsInWAL scans every WAL segment and returns the set of file
// paths referenced by EntryPageRedo entries.
func pageRedoPathsInWAL(t *testing.T, walPath string) map[string]bool {
	t.Helper()
	reader, err := wal.NewWALReader(walPath)
	if err != nil {
		t.Fatalf("open WAL reader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	paths := make(map[string]bool)
	for {
		entry, err := reader.ReadEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read WAL entry: %v", err)
		}
		if entry.Header.EntryType == wal.EntryPageRedo {
			path, _, _, err := deserializePageRedoPayload(entry.Payload)
			if err != nil {
				t.Fatalf("deserialize page redo payload: %v", err)
			}
			paths[path] = true
		}
		wal.ReleaseEntry(entry)
	}
	return paths
}

func newRuntimeDDLEngine(t *testing.T, dir string) (*StorageEngine, *TableMetaData) {
	t.Helper()
	meta := NewTableMenager()
	hm, err := NewHeapForTable(HeapFormatV2, filepath.Join(dir, "a.heap"))
	if err != nil {
		t.Fatalf("create heap a: %v", err)
	}
	if err := meta.NewTable("a", []Index{
		{Name: "id", Primary: true, Type: TypeVarchar, Unique: true},
	}, 0, hm); err != nil {
		t.Fatalf("create table a: %v", err)
	}
	walWriter, err := wal.NewWALWriter(filepath.Join(dir, "data.wal"), wal.DefaultOptions())
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}
	se, err := NewProductionStorageEngine(meta, walWriter)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { _ = se.Close() })
	return se, meta
}

// TestRuntimeCreatedTableGetsPageRedoHooks pins that a table registered on a
// live engine (the pkg/sql CREATE TABLE path) participates in WAL-before-data
// page logging: flushing its dirty pages must emit EntryPageRedo records,
// exactly like tables present at engine construction. Before the topology
// callback, hooks were armed only once at construction, so runtime-created
// tables flushed silently until the next reopen.
func TestRuntimeCreatedTableGetsPageRedoHooks(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	se, meta := newRuntimeDDLEngine(t, dir)

	heapPath := filepath.Join(dir, "b.heap")
	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("create heap b: %v", err)
	}
	if err := meta.NewTable("b", []Index{
		{Name: "id", Primary: true, Type: TypeVarchar, Unique: true},
	}, 0, hm); err != nil {
		t.Fatalf("create table b at runtime: %v", err)
	}

	insertVarcharRow(t, se, "b", "b-0")
	if err := se.FuzzyCheckpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	paths := pageRedoPathsInWAL(t, filepath.Join(dir, "data.wal"))
	if !paths[heapPath] {
		t.Fatalf("no EntryPageRedo for runtime-created table heap %s; flush hooks were not armed (got %v)", heapPath, paths)
	}
}

// TestRuntimeAddedIndexGetsPageRedoHooks is the same guarantee for indexes
// added to a live engine (the pkg/sql CREATE INDEX path).
func TestRuntimeAddedIndexGetsPageRedoHooks(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	se, meta := newRuntimeDDLEngine(t, dir)

	if err := meta.AddIndex("a", Index{Name: "name", Type: TypeVarchar}); err != nil {
		t.Fatalf("add index at runtime: %v", err)
	}

	doc := `{"id": "a-0", "name": "n-0"}`
	keys := map[string]types.Comparable{
		"id":   types.VarcharKey("a-0"),
		"name": types.VarcharKey("n-0"),
	}
	if err := se.InsertRow(ctx, "a", doc, keys); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := se.FuzzyCheckpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	treePath := defaultV2IndexPath(filepath.Join(dir, "a.heap"), "a", "name")
	paths := pageRedoPathsInWAL(t, filepath.Join(dir, "data.wal"))
	if !paths[treePath] {
		t.Fatalf("no EntryPageRedo for runtime-added index tree %s; flush hooks were not armed (got %v)", treePath, paths)
	}
}
