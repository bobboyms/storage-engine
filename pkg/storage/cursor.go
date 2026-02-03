package storage

import (
	"github.com/bobboyms/storage-engine/pkg/btree"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// Estrutura do Cursor
type Cursor struct {
	tree         *btree.BPlusTree
	currentNode  *btree.Node
	currentIndex int
}

// Close limpa a referência. Não gerencia locks de nó pois confiamos no Table RLock.
func (c *Cursor) Close() {
	c.currentNode = nil
}

// Key/Value: Pega o dado atual
func (c *Cursor) Key() types.Comparable { return c.currentNode.Keys[c.currentIndex] }
func (c *Cursor) Value() int64          { return c.currentNode.DataPtrs[c.currentIndex] }
func (c *Cursor) Valid() bool           { return c.currentNode != nil && c.currentIndex < c.currentNode.N }

// Seek: Posiciona o cursor na chave K ou na imediatamente posterior
func (c *Cursor) Seek(key types.Comparable) {
	c.Close()

	// FindLeafLowerBound retorna o nó com R-LOCK (Latch Crabbing do BTree)
	leaf, idx := c.tree.FindLeafLowerBound(key)

	// Como estamos sob proteção do Table RLock (garantido pelo Engine Scan/Get),
	// não precisamos manter locks individuais nos nós para leitura.
	// Liberamos o lock da folha imediatamente para evitar complexidade e deadlocks.
	if leaf != nil {
		leaf.RUnlock()
	}

	if leaf == nil {
		c.currentNode = nil
		c.currentIndex = 0
		return
	}

	// Lógica de navegação sem locks (Naked traversal protegido por Table Lock)
	if idx >= leaf.N {
		nextLeaf := leaf.Next
		if nextLeaf != nil {
			leaf = nextLeaf
			idx = 0
			// Skip empty
			for leaf != nil && leaf.N == 0 {
				leaf = leaf.Next
				idx = 0
			}
		} else {
			c.currentNode = nil
			return
		}
	}

	c.currentNode = leaf
	c.currentIndex = idx
}

// Next: Avança para o próximo registro
func (c *Cursor) Next() bool {
	if c.currentNode == nil {
		return false
	}

	// Avança dentro da folha
	if c.currentIndex+1 < c.currentNode.N {
		c.currentIndex++
		return true
	}

	// Navegação via ponteiros sem lock adicional
	nextLeaf := c.currentNode.Next

	c.currentNode = nextLeaf
	c.currentIndex = 0

	// Loop para pular vazios
	for c.currentNode != nil && c.currentNode.N == 0 {
		c.currentNode = c.currentNode.Next
		c.currentIndex = 0
	}

	if c.currentNode != nil {
		return true
	}

	return false
}
