package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// newSingleTableMeta builds a TableMetaData with one table "items" keyed by a
// primary INT index, returning the manager and the heap path for index-path
// assertions.
func newSingleTableMeta(t *testing.T) (*TableMetaData, string) {
	t.Helper()
	dir := t.TempDir()
	heapPath := filepath.Join(dir, "items.heap")
	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("heap: %v", err)
	}
	meta := NewTableMenager()
	if err := meta.NewTable("items", []Index{
		{Name: "id", Primary: true, Type: TypeInt},
	}, 0, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	return meta, heapPath
}

func TestAddIndexRegistersAndCreatesFile(t *testing.T) {
	meta, heapPath := newSingleTableMeta(t)

	if err := meta.AddIndex("items", Index{Name: "email", Type: TypeVarchar}); err != nil {
		t.Fatalf("AddIndex: %v", err)
	}

	if _, err := meta.GetIndexByName("items", "email"); err != nil {
		t.Fatalf("GetIndexByName after AddIndex: %v", err)
	}
	wantPath := defaultV2IndexPath(heapPath, "items", "email")
	if _, err := os.Stat(wantPath); err != nil {
		t.Fatalf("expected index file at %s: %v", wantPath, err)
	}
}

func TestAddIndexRejectsDuplicateAndPrimary(t *testing.T) {
	meta, _ := newSingleTableMeta(t)

	if err := meta.AddIndex("items", Index{Name: "email", Type: TypeVarchar}); err != nil {
		t.Fatalf("first AddIndex: %v", err)
	}
	if err := meta.AddIndex("items", Index{Name: "email", Type: TypeVarchar}); err == nil {
		t.Fatal("expected error adding a duplicate index")
	}
	if err := meta.AddIndex("items", Index{Name: "id2", Primary: true, Type: TypeInt}); err == nil {
		t.Fatal("expected error adding a primary index to an existing table")
	}
	if err := meta.AddIndex("missing", Index{Name: "x", Type: TypeInt}); err == nil {
		t.Fatal("expected error adding index to an unknown table")
	}
}

func TestDropIndexRemovesAndDeletesFile(t *testing.T) {
	meta, heapPath := newSingleTableMeta(t)
	if err := meta.AddIndex("items", Index{Name: "email", Type: TypeVarchar}); err != nil {
		t.Fatalf("AddIndex: %v", err)
	}
	idxPath := defaultV2IndexPath(heapPath, "items", "email")

	if err := meta.DropIndex("items", "email"); err != nil {
		t.Fatalf("DropIndex: %v", err)
	}
	if _, err := meta.GetIndexByName("items", "email"); err == nil {
		t.Fatal("expected GetIndexByName to fail after DropIndex")
	}
	if _, err := os.Stat(idxPath); !os.IsNotExist(err) {
		t.Fatalf("expected index file removed, stat err = %v", err)
	}
}

func TestDropIndexRejectsPrimaryAndMissing(t *testing.T) {
	meta, _ := newSingleTableMeta(t)

	if err := meta.DropIndex("items", "id"); err == nil {
		t.Fatal("expected error dropping the primary index")
	}
	if err := meta.DropIndex("items", "nope"); err == nil {
		t.Fatal("expected error dropping an unknown index")
	}
}
