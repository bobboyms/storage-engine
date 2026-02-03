package storage

import (
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/btree"
	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestSerializeDeserializeBPlusTree(t *testing.T) {
	// 1. Setup Tree with various data types
	tree := btree.NewTree(3)

	keys := []types.Comparable{
		types.IntKey(10),
		types.IntKey(20),
		types.IntKey(5),
		types.IntKey(15),
		types.IntKey(30),
		types.IntKey(25), // Trigger splits
	}

	for i, k := range keys {
		// Mock offsets
		tree.Insert(k, int64(i*100))
	}

	lastLSN := uint64(42)

	// 2. Serialize
	data, err := SerializeBPlusTree(tree, lastLSN)
	if err != nil {
		t.Fatalf("SerializeBPlusTree failed: %v", err)
	}

	// 3. Deserialize
	restoredTree, lsn, err := DeserializeBPlusTree(data)
	if err != nil {
		t.Fatalf("DeserializeBPlusTree failed: %v", err)
	}

	// 4. Verify Metadata
	if lsn != lastLSN {
		t.Errorf("Expected LSN %d, got %d", lastLSN, lsn)
	}
	if restoredTree.T != tree.T {
		t.Errorf("Expected T %d, got %d", tree.T, restoredTree.T)
	}

	// 5. Verify Content
	for i, k := range keys {
		node, found := restoredTree.Search(k)
		if !found {
			t.Errorf("Key %v not found in restored tree", k)
			continue
		}

		_, idx := node.FindLeafLowerBound(k)
		if node.DataPtrs[idx] != int64(i*100) {
			t.Errorf("Key %v offset mismatch. Got %d, want %d", k, node.DataPtrs[idx], int64(i*100))
		}
	}
}

func TestSerializeDeserialize_AllTypes(t *testing.T) {
	// Teste com todos os tipos suportados

	// Helper para testar um tipo específico
	testType := func(name string, key types.Comparable) {
		t.Run(name, func(t *testing.T) {
			tree := btree.NewTree(3)
			tree.Insert(key, 12345)

			data, err := SerializeBPlusTree(tree, 1)
			if err != nil {
				t.Fatalf("Serialize failed: %v", err)
			}

			restored, _, err := DeserializeBPlusTree(data)
			if err != nil {
				t.Fatalf("Deserialize failed: %v", err)
			}

			node, found := restored.Search(key)
			if !found {
				t.Fatalf("Key not found")
			}
			_, idx := node.FindLeafLowerBound(key)
			if node.Keys[idx].Compare(key) != 0 {
				t.Errorf("Key mismatch")
			}
		})
	}

	testType("Int", types.IntKey(1))
	testType("Varchar", types.VarcharKey("hello world"))
	testType("Bool", types.BoolKey(true))
	testType("Float", types.FloatKey(3.14159))
	testType("Date", types.DateKey(time.Now()))
}
