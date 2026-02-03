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

// Close limpa a referência e libera o lock do nó atual
func (c *Cursor) Close() {
	if c.currentNode != nil {
		c.currentNode.RUnlock()
		c.currentNode = nil
	}
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

	// Como FindLeafLowerBound retorna o nó com RLock adquirido, NÓS MANTEMOS O LOCK.
	// Isso garante consistência thread-safe para o cursor.

	if leaf == nil {
		c.currentNode = nil
		c.currentIndex = 0
		return
	}

	// Lógica de navegação: Se o índice for inválido, precisamos pular para o próximo nó.
	if idx >= leaf.N {
		// Use nextLeaf com cuidado. leaf.Next não é protegido pelo lock do leaf?
		// O campo Next é modificado em splits protegidos por lock. Como temos RLock, é seguro ler.
		nextLeaf := leaf.Next

		if nextLeaf != nil {
			nextLeaf.RLock() // Lock Coupling
			leaf.RUnlock()   // Libera anterior
			leaf = nextLeaf
			idx = 0
			// Skip empty
			for leaf != nil && leaf.N == 0 {
				next := leaf.Next
				if next != nil {
					next.RLock()
				}
				leaf.RUnlock()
				leaf = next
				idx = 0
			}
		} else {
			// Fim da linha
			leaf.RUnlock()
			c.currentNode = nil
			return
		}
	}

	if leaf == nil {
		c.currentNode = nil
		return
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

	// Navegação via ponteiros COM Latch Coupling
	// Precisamos ler Next enquanto seguramos o lock atual (que já temos)
	nextLeaf := c.currentNode.Next

	if nextLeaf != nil {
		nextLeaf.RLock() // Adquire lock do próximo antes de soltar o atual
	}

	c.currentNode.RUnlock() // Solta lock atual
	c.currentNode = nextLeaf
	c.currentIndex = 0

	// Loop para pular vazios (com locking)
	for c.currentNode != nil && c.currentNode.N == 0 {
		next := c.currentNode.Next
		if next != nil {
			next.RLock()
		}
		c.currentNode.RUnlock()
		c.currentNode = next
		c.currentIndex = 0
	}

	if c.currentNode != nil {

		return true
	}

	return false
}
