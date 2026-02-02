package storage

import (
	"github.com/bobboyms/storage-engine/pkg/btree"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// Estrutura do Cursor
type Cursor struct {
	tree         *btree.BPlusTree
	currentNode  *btree.Node // Folha atual
	currentIndex int         // Índice dentro da folha
}

// Key/Value: Pega o dado atual
func (c *Cursor) Key() types.Comparable { return c.currentNode.Keys[c.currentIndex] }
func (c *Cursor) Value() int            { return c.currentNode.DataPtrs[c.currentIndex] }
func (c *Cursor) Valid() bool           { return c.currentNode != nil && c.currentIndex < c.currentNode.N }

// Seek: Posiciona o cursor na chave K ou na imediatamente posterior
// Essencial para: WHERE age >= 18
func (c *Cursor) Seek(key types.Comparable) {
	leaf, idx := c.tree.FindLeafLowerBound(key)
	if leaf == nil {
		c.currentNode = nil
		c.currentIndex = 0
		return
	}

	// Se passou do fim, tenta próxima folha
	if idx >= leaf.N {
		if leaf.Next != nil && leaf.Next.N > 0 {
			leaf = leaf.Next
			idx = 0
		} else {
			c.currentNode = nil
			return
		}
	}

	c.currentNode = leaf
	c.currentIndex = idx
}

// Next: Avança para o próximo registro
// Essencial para: Range Scans e Full Table Scans
func (c *Cursor) Next() bool {
	if c.currentNode == nil {
		return false
	}

	// Avança dentro da folha
	if c.currentIndex+1 < c.currentNode.N {
		c.currentIndex++
		return true
	}

	// Senão, tenta ir para a próxima folha
	nextLeaf := c.currentNode.Next
	for nextLeaf != nil && nextLeaf.N == 0 {
		nextLeaf = nextLeaf.Next // pula folhas vazias
	}
	if nextLeaf != nil {
		c.currentNode = nextLeaf
		c.currentIndex = 0
		return true
	}

	// Não há mais dados - INVALIDA o cursor
	c.currentNode = nil
	c.currentIndex = 0
	return false
}
