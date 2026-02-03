package btree

import (
	"fmt"
	"sort"
	"sync" // Added for Latch Crabbing

	"github.com/bobboyms/storage-engine/pkg/errors"
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
	return b.Upsert(key, func(oldValue int64, exists bool) (int64, error) {
		return dataPtr, nil
	})
}

// Upsert executes a function on the current value (if exists) and sets the new value.
// The callback is executed while holding the leaf lock, enabling atomic Read-Modify-Write.
func (b *BPlusTree) Upsert(key types.Comparable, fn func(oldValue int64, exists bool) (newValue int64, err error)) error {
	return b.upsertHelper(key, fn)
}

func (b *BPlusTree) insertHelper(key types.Comparable, dataPtr int64, uniqueKey bool) error {
	return b.Upsert(key, func(oldValue int64, exists bool) (int64, error) {
		if exists && uniqueKey {
			return 0, &errors.DuplicateKeyError{Key: fmt.Sprintf("%v", key)}
		}
		return dataPtr, nil
	})
}

func (b *BPlusTree) upsertHelper(key types.Comparable, fn func(oldValue int64, exists bool) (newValue int64, err error)) error {

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

		return b.upsertTopDown(newRoot, key, fn)
	}

	b.mu.Unlock()
	return b.upsertTopDown(root, key, fn)
}

// upsertTopDown realiza a inserção descendo a árvore e dividindo nós cheios preventivamente.
// Assume que 'curr' já está trancado (Lock) pelo chamador.
func (b *BPlusTree) upsertTopDown(curr *Node, key types.Comparable, fn func(oldValue int64, exists bool) (newValue int64, err error)) error {

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
	return curr.UpsertNonFull(key, fn)
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

// Get retorna o valor associado à chave de forma thread-safe (usando latching interno)
func (b *BPlusTree) Get(key types.Comparable) (int64, bool) {
	if b == nil {
		return 0, false
	}
	b.mu.RLock()
	curr := b.Root
	if curr == nil {
		b.mu.RUnlock()
		return 0, false
	}
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
			return curr.DataPtrs[j], true
		}
	}
	return 0, false
}

// FindLeafLowerBoundSafe busca o nó folha para scan de forma segura.
// Retorna o nó com RLock adquirido. O CHAMADOR DEVE CHAMAR RUnlock() NO NÓ RETORNADO.
func (b *BPlusTree) FindLeafLowerBound(key types.Comparable) (*Node, int) {
	b.mu.RLock()
	curr := b.Root
	curr.RLock()
	b.mu.RUnlock()

	for !curr.Leaf {
		var i int
		if key == nil {
			i = 0
		} else {
			i = sort.Search(curr.N, func(i int) bool {
				return curr.Keys[i].Compare(key) >= 0
			})
		}

		child := curr.Children[i]
		child.RLock()
		curr.RUnlock()
		curr = child
	}

	var idx int
	if key == nil {
		idx = 0
	} else {
		idx = sort.Search(curr.N, func(i int) bool {
			return curr.Keys[i].Compare(key) >= 0
		})
	}

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
