package storage

import (
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/btree"
	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestCheckpointManager_CreateAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	cm := NewCheckpointManager(tmpDir)

	tree := btree.NewTree(3)
	tree.Insert(types.IntKey(1), 100)
	tree.Insert(types.IntKey(2), 200)

	tableName := "users"
	indexName := "id"
	lsn := uint64(50)

	// 1. Create Checkpoint
	if err := cm.CreateCheckpoint(tableName, indexName, tree, lsn); err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}

	// 2. Verify File Exists
	matches, _ := filepath.Glob(filepath.Join(tmpDir, "checkpoint_users_id_50.chk"))
	if len(matches) != 1 {
		t.Errorf("Checkpoint file not found")
	}

	// 3. Load Checkpoint
	restored, restoredLSN, err := cm.LoadLatestCheckpoint(tableName, indexName)
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint failed: %v", err)
	}

	if restoredLSN != lsn {
		t.Errorf("Expected LSN %d, got %d", lsn, restoredLSN)
	}

	node, found := restored.Search(types.IntKey(1))
	if !found {
		t.Error("Key 1 not found in restored checkpoint")
	}
	_, idx := node.FindLeafLowerBound(types.IntKey(1))
	if node.DataPtrs[idx] != 100 {
		t.Errorf("DataPtr mismatch")
	}
}

func TestCheckpointManager_CleanupOldDetailed(t *testing.T) {
	// Teste se o sistema prioriza o LSN mais novo e limpa os antigos
	tmpDir := t.TempDir()
	cm := NewCheckpointManager(tmpDir)
	tree := btree.NewTree(3)

	tableName := "logs"
	indexName := "ts"

	// Cria checkpoint LSN 10
	if err := cm.CreateCheckpoint(tableName, indexName, tree, 10); err != nil {
		t.Fatalf("Create 10 failed: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	// Cria checkpoint LSN 20
	if err := cm.CreateCheckpoint(tableName, indexName, tree, 20); err != nil {
		t.Fatalf("Create 20 failed: %v", err)
	}

	// Verifica se LSN 10 foi deletado
	matches, _ := filepath.Glob(filepath.Join(tmpDir, "checkpoint_logs_ts_10.chk"))
	if len(matches) != 0 {
		t.Errorf("Old checkpoint (LSN 10) was not deleted")
	}

	// Carrega deve pegar o 20
	_, lsn, err := cm.LoadLatestCheckpoint(tableName, indexName)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if lsn != 20 {
		t.Errorf("Expected load LSN 20, got %d", lsn)
	}
}

func TestCheckpoint_SerializeFails(t *testing.T) {
	// Call internal serialize functions with invalid data for coverage
	_, err := deserializeKey([]byte{}) // Empty
	if err == nil {
		t.Error("Expected error for empty key data")
	}

	_, err = deserializeKey([]byte{99, 1, 2, 3}) // Unknown tag 99
	if err == nil {
		t.Error("Expected error for unknown tag")
	}

	// Partial read tests
	_, err = deserializeKey([]byte{1, 1}) // Int needs 8 bytes + 1 tag
	if err == nil {
		t.Error("Expected error for partial int")
	}

	_, err = deserializeKey([]byte{2, 5, 0}) // Varchar says 5 bytes but provided 0
	if err == nil {
		t.Error("Expected error for partial varchar")
	}
}

func TestCheckpoint_MultiLevel(t *testing.T) {
	// Create a tree with enough keys to force internal nodes
	tree := btree.NewTree(2) // T=2, max keys = 3
	for i := 1; i <= 10; i++ {
		tree.Insert(types.IntKey(i), int64(i*100))
	}

	// Serialize
	data, err := SerializeBPlusTree(tree, 99)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Deserialize
	restored, lsn, err := DeserializeBPlusTree(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if lsn != 99 {
		t.Errorf("Expected LSN 99, got %d", lsn)
	}

	// Verify data
	for i := 1; i <= 10; i++ {
		v, found := restored.Get(types.IntKey(i))
		if !found || v != int64(i*100) {
			t.Errorf("Restored tree missing or wrong value for key %d", i)
		}
	}
}

func TestCheckpoint_SerializeErrors(t *testing.T) {
	// Root nil
	_, err := SerializeBPlusTree(&btree.BPlusTree{Root: nil}, 0)
	if err == nil {
		t.Error("Expected error for nil root")
	}

	// Invalid magic
	_, _, err = DeserializeBPlusTree([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	if err == nil {
		t.Error("Expected error for invalid magic")
	}
}

func TestCheckpoint_AllTypes(t *testing.T) {
	tmpDir := t.TempDir()
	cm := NewCheckpointManager(tmpDir)

	testCases := []struct {
		name string
		key  types.Comparable
		val  int64
	}{
		{"Int", types.IntKey(1), 100},
		{"Varchar", types.VarcharKey("hello"), 200},
		{"Bool", types.BoolKey(true), 300},
		{"Float", types.FloatKey(3.14), 400},
		{"Date", types.DateKey(time.Now()), 500},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tree := btree.NewTree(3)
			tree.Insert(tc.key, tc.val)

			tableName := "table_" + tc.name
			if err := cm.CreateCheckpoint(tableName, "idx", tree, 1); err != nil {
				t.Fatalf("CreateCheckpoint failed: %v", err)
			}

			restored, _, err := cm.LoadLatestCheckpoint(tableName, "idx")
			if err != nil {
				t.Fatalf("LoadLatestCheckpoint failed: %v", err)
			}

			v, found := restored.Get(tc.key)
			if !found || v != tc.val {
				t.Errorf("Expected found=true, val=%v; got found=%v, val=%v", tc.val, found, v)
			}
		})
	}
}

func TestCheckpoint_DeserializeMalformed(t *testing.T) {
	// Wrong magic
	data := make([]byte, 100)
	binary.LittleEndian.PutUint32(data[0:4], 0xBAADFEED)
	_, _, err := DeserializeBPlusTree(data)
	if err == nil {
		t.Error("Expected error for wrong magic")
	}

	// Truncated data
	header := make([]byte, 24)
	binary.LittleEndian.PutUint32(header[0:4], CheckpointMagic)
	_, _, err = DeserializeBPlusTree(header[0:10])
	if err == nil {
		t.Error("Expected error for truncated header")
	}
}

func TestCheckpoint_KeyTypeErrors(t *testing.T) {
	// Unknown key type tag
	data := []byte{99, 1, 2, 3} // tag 99
	_, err := deserializeKey(data)
	if err == nil {
		t.Error("Expected error for unknown key type tag")
	}
}
