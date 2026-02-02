package storage_test

import (
	"testing"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestEngine_GetAndDel(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	err := tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	se := storage.NewStorageEngine(tableMgr)

	// Test Get on empty
	_, found, err := se.Get("users", "id", types.IntKey(10))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if found {
		t.Error("Expected found=false for missing key")
	}

	// Put data
	err = se.Put("users", "id", types.IntKey(10), 100)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get found
	node, found, err := se.Get("users", "id", types.IntKey(10))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Error("Expected found=true for existing key")
	}
	if node == nil || node.DataPtrs[0] != 100 {
		t.Errorf("Expected value 100, got %v", node)
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
