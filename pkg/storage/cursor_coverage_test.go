package storage_test

import (
	"testing"

	"github.com/bobboyms/storage-engine/pkg/btree"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestCursor_SeekCoverage(t *testing.T) {
	// Setup tree with multiple leaves
	tree := btree.NewTree(3)
	for i := 0; i < 20; i++ {
		tree.Insert(types.IntKey(i*10), int64(i))
	}

	se, _ := storage.NewStorageEngine(storage.NewTableMenager(), nil)
	c := se.Cursor(tree)
	defer c.Close()

	// 1. Seek to existing key
	c.Seek(types.IntKey(10))
	if !c.Valid() || c.Key() != types.IntKey(10) {
		t.Error("Seek 10 failed")
	}

	// 2. Seek to non-existent (should land on next)
	c.Seek(types.IntKey(15))
	if !c.Valid() || c.Key() != types.IntKey(20) {
		t.Errorf("Seek 15 should land on 20, got %v", c.Key())
	}

	// 3. Seek to exact end of a leaf to force jump
	c.Seek(types.IntKey(9999))
	if c.Valid() {
		t.Error("Seek beyond max should be invalid")
	}
}

func TestCursor_NextCoverage(t *testing.T) {
	tree := btree.NewTree(3)
	for i := 0; i < 5; i++ {
		tree.Insert(types.IntKey(i), int64(i))
	}

	se, _ := storage.NewStorageEngine(storage.NewTableMenager(), nil)
	c := se.Cursor(tree)
	defer c.Close()

	c.Seek(nil) // Start
	count := 0
	for c.Valid() {
		count++
		c.Next()
	}
	if count != 5 {
		t.Errorf("Expected 5 items, got %d", count)
	}

	// Next when invalid
	if c.Next() {
		t.Error("Next on invalid cursor should return false")
	}
}

func TestCursor_SkipEmpty(t *testing.T) {
	// Manually construct a chain of leaves: [10] -> [] -> [30]
	leaf1 := btree.NewNode(3, true)
	// Manual insert [10]
	leaf1.Keys = append(leaf1.Keys, types.IntKey(10))
	leaf1.DataPtrs = append(leaf1.DataPtrs, 1)
	leaf1.N = 1

	leafEmpty := btree.NewNode(3, true)
	// Empty (N=0)

	leaf3 := btree.NewNode(3, true)
	// Manual insert [30]
	leaf3.Keys = append(leaf3.Keys, types.IntKey(30))
	leaf3.DataPtrs = append(leaf3.DataPtrs, 3)
	leaf3.N = 1

	// Link them
	leaf1.Next = leafEmpty
	leafEmpty.Next = leaf3

	// Create tree and hack Root to point to leaf1
	tree := btree.NewTree(3)
	tree.Root = leaf1

	se, _ := storage.NewStorageEngine(storage.NewTableMenager(), nil)
	c := se.Cursor(tree)
	defer c.Close()

	// Seek to 10
	c.Seek(types.IntKey(10))
	if !c.Valid() || c.Key() != types.IntKey(10) {
		t.Error("Should be at 10")
	}

	// Next should skip empty and go to 30
	c.Next()
	if !c.Valid() || c.Key() != types.IntKey(30) {
		t.Errorf("Should skip empty and go to 30, got valid=%v", c.Valid())
		if c.Valid() {
			t.Logf("Got key: %v", c.Key())
		}
	}
}

func TestCursor_EmptyTree(t *testing.T) {
	tree := btree.NewTree(3)
	se, _ := storage.NewStorageEngine(storage.NewTableMenager(), nil)
	c := se.Cursor(tree)
	defer c.Close()

	c.Seek(types.IntKey(1))
	if c.Valid() {
		t.Error("Cursor on empty tree should be invalid")
	}

	// Also test Next on empty
	if c.Next() {
		t.Error("Next on empty tree should be false")
	}
}
