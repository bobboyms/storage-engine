package storage_test

import (
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestEngine_GetAndDel(t *testing.T) {
	tmpDir := t.TempDir()
	heapPath := filepath.Join(tmpDir, "heap.data")

	tableMgr := storage.NewTableMenager()
	err := tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	se, err := storage.NewStorageEngine(tableMgr, "", heapPath) // Empty string = no WAL (memory only)
	if err != nil {
		t.Fatalf("NewStorageEngine failed: %v", err)
	}

	// Test Get on empty
	_, found, err := se.Get("users", "id", types.IntKey(10))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if found {
		t.Error("Expected found=false for missing key")
	}

	// Put data
	doc := "{\"id\":10,\"name\":\"Alice\"}"
	err = se.Put("users", "id", types.IntKey(10), doc)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get found
	gotDoc, found, err := se.Get("users", "id", types.IntKey(10))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Error("Expected found=true for existing key")
	}
	if gotDoc != doc {
		t.Errorf("Expected doc %q, got %q", doc, gotDoc)
	}

	// Test Del
	ok, err := se.Del("users", "id", types.IntKey(10))
	if err != nil {
		t.Fatalf("Del failed: %v", err)
	}
	if !ok {
		t.Error("Expected ok=true for deleting existing key")
	}

	// Verify deleted
	_, found, _ = se.Get("users", "id", types.IntKey(10))
	if found {
		t.Error("Expected found=false after deletion")
	}

	// Test Del missing
	ok, err = se.Del("users", "id", types.IntKey(99))
	if err != nil {
		t.Fatalf("Del failed: %v", err)
	}
	if ok {
		t.Error("Expected ok=false for deleting missing key")
	}
}
