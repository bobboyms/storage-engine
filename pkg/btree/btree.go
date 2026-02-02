package btree

import (
	"github.com/bobboyms/storage-engine/pkg/types"
)

// BPlusTree struct
type BPlusTree struct {
	T         int
	Root      *Node
	UniqueKey bool // Se true, não permite chaves duplicadas
}

// NewTree cria uma árvore normal (permite duplicatas)
func NewTree(t int) *BPlusTree {
	return &BPlusTree{
		T:         t,
		Root:      NewNode(t, true),
		UniqueKey: false, // Por padrão permite duplicatas
	}
}

// NewUniqueTree cria um índice único (não permite duplicatas)
func NewUniqueTree(t int) *BPlusTree {
	return &BPlusTree{
		T:         t,
		Root:      NewNode(t, true),
		UniqueKey: true, // Índice único
	}
}

// Insert: Agora retorna um erro em caso de violação de chave única
func (b *BPlusTree) Insert(key types.Comparable, dataPtr int) error {
	root := b.Root

	// Se a raiz estiver cheia, ela divide
	if root.N == 2*b.T-1 {
		newRoot := NewNode(b.T, false)
		newRoot.Children = append(newRoot.Children, root)
		newRoot.SplitChild(0)
		b.Root = newRoot
		return newRoot.InsertNonFull(key, dataPtr, b.UniqueKey)
	}

	return root.InsertNonFull(key, dataPtr, b.UniqueKey)
}

func (b *BPlusTree) Search(key types.Comparable) (*Node, bool) {
	return b.Root.Search(key)
}

// findLeafLowerBound expõe a busca interna para o StorageEngine
func (b *BPlusTree) FindLeafLowerBound(key types.Comparable) (*Node, int) {
	return b.findLeafLowerBound(key)
}

func (b *BPlusTree) findLeafLowerBound(key types.Comparable) (*Node, int) {
	return b.Root.findLeafLowerBound(key)
}
