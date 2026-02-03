package storage

import (
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
	time.Sleep(10 * time.Millisecond) // Garante timestamp fs diferente se necessário (não usado aqui, mas bom prático)

	// Cria checkpoint LSN 20
	if err := cm.CreateCheckpoint(tableName, indexName, tree, 20); err != nil {
		t.Fatalf("Create 20 failed: %v", err)
	}

	// Verifica se LSN 10 foi deletado (assumindo que CreateCheckpoint chama cleanOldCheckpoints)
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
