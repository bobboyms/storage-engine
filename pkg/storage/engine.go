package storage

import (
	"github.com/bobboyms/storage-engine/pkg/btree"
	"github.com/bobboyms/storage-engine/pkg/query"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/google/uuid"
)

func GenerateKey() string {
	// NewV7 gera um UUID baseado no tempo atual + aleatoriedade segura
	id, err := uuid.NewV7()
	if err != nil {
		panic(err) // Em caso improvável de erro no gerador de entropia
	}
	return id.String()
}

type StorageEngine struct {
	TableMetaData *TableMetaData
}

func NewStorageEngine(tableMetaData *TableMetaData) *StorageEngine {
	return &StorageEngine{
		TableMetaData: tableMetaData,
	}
}

func (se *StorageEngine) Cursor(tree *btree.BPlusTree) *Cursor {
	return &Cursor{tree: tree}
}

// Put: Insert ou Update
func (se *StorageEngine) Put(tableName string, indexName string, key types.Comparable, val int) error {
	index, err := se.TableMetaData.GetIndexByName(tableName, indexName)
	if err != nil {
		return err
	}
	err = index.Tree.Insert(key, val)
	if err != nil {
		return err
	}
	return nil
}

// Get: Busca exata (SELECT * WHERE id = x)
func (se *StorageEngine) Get(tableName string, indexName string, key types.Comparable) (*btree.Node, bool, error) {
	index, err := se.TableMetaData.GetIndexByName(tableName, indexName)
	if err != nil {
		return nil, false, err
	}
	node, ok := index.Tree.Search(key)
	return node, ok, nil
}

// Scan: Método genérico que aceita qualquer condição de busca
// Suporta: =, !=, >, <, >=, <=, BETWEEN
func (se *StorageEngine) Scan(tableName string, indexName string, condition *query.ScanCondition) ([]int, error) {
	results := []int{}
	index, err := se.TableMetaData.GetIndexByName(tableName, indexName)
	if err != nil {
		return results, err
	}
	c := se.Cursor(index.Tree)

	// Otimiza scan se possível (=, >, >=, BETWEEN)
	if condition.ShouldSeek() {
		startKey := condition.GetStartKey()
		c.Seek(startKey)

		for c.Valid() {
			key := c.Key()

			// Verifica se ainda devemos continuar o scan
			if !condition.ShouldContinue(key) {
				break
			}

			// Verifica se a chave satisfaz a condição
			if condition.Matches(key) {
				results = append(results, c.Value())
			}

			c.Next()
		}
	} else {
		// Full scan para operadores como != e <
		// Inicia do começo da árvore
		leftmost := index.Tree.Root
		for !leftmost.Leaf {
			leftmost = leftmost.Children[0]
		}

		c.currentNode = leftmost
		c.currentIndex = 0

		for c.Valid() {
			key := c.Key()

			// Para < e <=, podemos parar cedo
			if !condition.ShouldContinue(key) {
				break
			}

			if condition.Matches(key) {
				results = append(results, c.Value())
			}
			c.Next()
		}
	}

	return results, nil
}

// RangeScan: Wrapper de conveniência para BETWEEN (mantido para compatibilidade)
func (se *StorageEngine) RangeScan(tableName string, indexName string, start, end types.Comparable) ([]int, error) {
	return se.Scan(tableName, indexName, query.Between(start, end))
}

// Delete: Remove (DELETE FROM WHERE id = x)
func (se *StorageEngine) Del(tableName string, indexName string, key types.Comparable) (bool, error) {
	index, err := se.TableMetaData.GetIndexByName(tableName, indexName)
	if err != nil {
		return false, err
	}

	leaf, idx := index.Tree.FindLeafLowerBound(key)
	if leaf == nil || idx >= leaf.N || leaf.Keys[idx].Compare(key) != 0 {
		return false, nil
	}

	removed := index.Tree.Root.Remove(key)

	// Se a raiz ficou vazia e não é folha, desce um nível
	if index.Tree.Root.N == 0 && !index.Tree.Root.Leaf {
		index.Tree.Root = index.Tree.Root.Children[0]
	}
	return removed, nil
}
