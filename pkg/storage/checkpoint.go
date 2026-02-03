package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/bobboyms/storage-engine/pkg/btree"
)

// CheckpointManager gerencia a criação e leitura de checkpoints
type CheckpointManager struct {
	basePath string
	mu       sync.Mutex
}

func NewCheckpointManager(basePath string) *CheckpointManager {
	return &CheckpointManager{
		basePath: basePath,
	}
}

// CreateCheckpoint cria um snapshot da árvore especificada
func (cm *CheckpointManager) CreateCheckpoint(tableName, indexName string, tree *btree.BPlusTree, lsn uint64) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Nome do arquivo: checkpoint_<tableName>_<indexName>_<LSN>.chk
	filename := fmt.Sprintf("checkpoint_%s_%s_%d.chk", tableName, indexName, lsn)
	path := filepath.Join(cm.basePath, filename)

	// Serializa para memória primeiro (poderia ser stream direto para otimizar RAM)
	data, err := SerializeBPlusTree(tree, lsn)
	if err != nil {
		return fmt.Errorf("serialization failed: %w", err)
	}

	// Grava em arquivo atômico (write temp + rename)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write temp file failed: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename file failed: %w", err)
	}

	// Limpeza de checkpoints antigos (opcional, manter últimos N)
	// Vamos limpar os antigos para não acumular lixo
	return cm.cleanOldCheckpoints(tableName, indexName, lsn)
}

// cleanOldCheckpoints remove checkpoints anteriores para manter apenas o mais recente
func (cm *CheckpointManager) cleanOldCheckpoints(tableName, indexName string, keepLSN uint64) error {
	files, err := os.ReadDir(cm.basePath)
	if err != nil {
		return err
	}

	prefix := fmt.Sprintf("checkpoint_%s_%s_", tableName, indexName)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), prefix) && strings.HasSuffix(f.Name(), ".chk") {
			lsnStr := strings.TrimSuffix(strings.TrimPrefix(f.Name(), prefix), ".chk")
			lsn, err := strconv.ParseUint(lsnStr, 10, 64)
			if err == nil && lsn < keepLSN {
				os.Remove(filepath.Join(cm.basePath, f.Name()))
			}
		}
	}
	return nil
}

// LoadLatestCheckpoint tenta carregar o checkpoint mais recente para a árvore dada
func (cm *CheckpointManager) LoadLatestCheckpoint(tableName, indexName string) (*btree.BPlusTree, uint64, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	files, err := os.ReadDir(cm.basePath)
	if err != nil {
		return nil, 0, err // Se não conseguir ler diretório, assume sem checkpoint
	}

	prefix := fmt.Sprintf("checkpoint_%s_%s_", tableName, indexName)
	var maxLSN uint64
	var latestFile string
	found := false

	for _, f := range files {
		if strings.HasPrefix(f.Name(), prefix) && strings.HasSuffix(f.Name(), ".chk") {
			// Extrai LSN
			lsnStr := strings.TrimSuffix(strings.TrimPrefix(f.Name(), prefix), ".chk")
			lsn, err := strconv.ParseUint(lsnStr, 10, 64)
			if err == nil {
				if lsn >= maxLSN { // >= para garantir pegar o ultimo mesmo se for 0 e unico
					maxLSN = lsn
					latestFile = f.Name()
					found = true
				}
			}
		}
	}

	if !found {
		return nil, 0, os.ErrNotExist
	}

	// Lê e deserializa
	path := filepath.Join(cm.basePath, latestFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}

	return DeserializeBPlusTree(data)
}
