package btree

import (
	"sort"
	"sync" // Added for Latch Crabbing

	"github.com/bobboyms/storage-engine/pkg/types"
)

// BPlusTree struct
type BPlusTree struct {
	T         int
	Root      *Node
	UniqueKey bool         // Se true, não permite chaves duplicadas
	mu        sync.RWMutex // Protege o ponteiro Root e operações estruturais na árvore
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

// Insert: Implementa inserção concorrente
func (b *BPlusTree) Insert(key types.Comparable, dataPtr int64) error {
	return b.insertHelper(key, dataPtr, b.UniqueKey)
}

// Replace forcily updates the key's value (used for MVCC Updates on Unique Index)
func (b *BPlusTree) Replace(key types.Comparable, dataPtr int64) error {
	// Force uniqueKey=false to allow overwrite
	return b.insertHelper(key, dataPtr, false)
}

func (b *BPlusTree) insertHelper(key types.Comparable, dataPtr int64, uniqueKey bool) error {
	b.mu.Lock()
	root := b.Root
	root.Lock()

	if root.IsFull() {
		newRoot := NewNode(b.T, false)
		newRoot.Children = append(newRoot.Children, root)
		newRoot.SplitChild(0)
		b.Root = newRoot
		b.mu.Unlock()

		newRoot.Lock()
		root.Unlock()

		return b.insertTopDown(newRoot, key, dataPtr, uniqueKey)
	}

	b.mu.Unlock()
	return b.insertTopDown(root, key, dataPtr, uniqueKey)
}

// insertTopDown realiza a inserção descendo a árvore e dividindo nós cheios preventivamente.
// Assume que 'curr' já está trancado (Lock) pelo chamador.
func (b *BPlusTree) insertTopDown(curr *Node, key types.Comparable, dataPtr int64, uniqueKey bool) error {
	// Garante unlock do nó atual no final (ou em caso de erro)
	// Se passarmos o lock para o filho, `curr` mudará, então cuidado com defer.
	// Vamos gerenciar os unlocks manualmente para latch crabbing.

	defer func() {
		if curr != nil {
			curr.Unlock()
		}
	}()

	for !curr.Leaf {
		// Encontra índice do filho
		i := 0
		for i < curr.N && key.Compare(curr.Keys[i]) >= 0 {
			i++
		}

		child := curr.Children[i]
		child.Lock()

		if child.IsFull() {
			// Split preventivo!
			curr.SplitChild(i)

			// Após split, verificamos para qual filho descer
			if key.Compare(curr.Keys[i]) >= 0 {
				// Solta o filho original da esquerda e pega o da direita (novo)
				child.Unlock()
				child = curr.Children[i+1]
				child.Lock()
			} else {
				// Mantém o filho da esquerda, nada a fazer
			}
		}

		// Latch Crabbing: Solta o pai (curr), mantém o filho (child)
		curr.Unlock()
		curr = child
	}

	// Chegamos na folha e ela está lockada.
	// Como usamos split preventivo, é garantido que ela não está cheia.
	// Podemos inserir diretamente.
	return curr.InsertNonFull(key, dataPtr, uniqueKey)
}

// Search busca uma chave na árvore de forma concorrente (RLock coupling)
func (b *BPlusTree) Search(key types.Comparable) (*Node, bool) {
	b.mu.RLock()
	curr := b.Root
	curr.RLock()
	b.mu.RUnlock()

	for !curr.Leaf {
		// Encontra filho
		i := 0
		for i < curr.N && key.Compare(curr.Keys[i]) >= 0 {
			i++
		}
		child := curr.Children[i]
		child.RLock()

		// Latch Crabbing: Solta o pai, mantém o filho
		curr.RUnlock()
		curr = child
	}

	// Busca na folha
	defer curr.RUnlock()

	for j := 0; j < curr.N; j++ {
		if key.Compare(curr.Keys[j]) == 0 {
			return curr, true
		}
	}
	return nil, false
}

// FindLeafLowerBoundSafe busca o nó folha para scan de forma segura.
// Retorna o nó com RLock adquirido. O CHAMADOR DEVE CHAMAR RUnlock() NO NÓ RETORNADO.
func (b *BPlusTree) FindLeafLowerBound(key types.Comparable) (*Node, int) {
	b.mu.RLock()
	curr := b.Root
	curr.RLock()
	b.mu.RUnlock()

	for !curr.Leaf {
		i := sort.Search(curr.N, func(i int) bool {
			return curr.Keys[i].Compare(key) >= 0
		})

		child := curr.Children[i]
		child.RLock()
		curr.RUnlock()
		curr = child
	}

	idx := sort.Search(curr.N, func(i int) bool {
		return curr.Keys[i].Compare(key) >= 0
	})

	return curr, idx
}

// findLeafLowerBound: Wrapper interno para compatibilidade com testes antigos.
// Retorna o nó DESTRAVADO.
func (b *BPlusTree) findLeafLowerBound(key types.Comparable) (*Node, int) {
	node, idx := b.FindLeafLowerBound(key)
	if node != nil {
		node.RUnlock()
	}
	return node, idx
}
