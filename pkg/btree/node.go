package btree

import (
	"fmt"
	"sort"
	"sync" // Added for Latch Crabbing

	"github.com/bobboyms/storage-engine/pkg/errors"
	"github.com/bobboyms/storage-engine/pkg/types"
)

type Node struct {
	T        int                // Grau mínimo
	Keys     []types.Comparable // Chaves
	DataPtrs []int64            // Ponteiros para os dados (apenas em folhas)
	Children []*Node            // Filhos (apenas em nós internos)
	Leaf     bool               // Se é folha
	N        int                // Número de chaves atual
	Next     *Node              // Ponteiro para a próxima folha (lista ligada)
	mu       sync.RWMutex       // Latch (Lock) para controle de concorrência granular
}

func NewNode(t int, leaf bool) *Node {
	return &Node{
		T:        t,
		Leaf:     leaf,
		Keys:     make([]types.Comparable, 0, 2*t-1),
		DataPtrs: make([]int64, 0, 2*t-1),
		Children: make([]*Node, 0, 2*t),
	}
}

// Métodos auxiliares de Lock para o Node

func (n *Node) Lock() {
	if n != nil {
		n.mu.Lock()
	}
}

func (n *Node) Unlock() {
	if n != nil {
		n.mu.Unlock()
	}
}

func (n *Node) RLock() {
	if n != nil {
		n.mu.RLock()
	}
}

func (n *Node) RUnlock() {
	if n != nil {
		n.mu.RUnlock()
	}
}

// IsSafeForInsert verifica se o nó pode receber uma inserção sem split
func (n *Node) IsSafeForInsert() bool {
	return n.N < 2*n.T-1
}

// IsSafeForDelete verifica se o nó pode sofrer deleção sem merge/borrow
// Um nó é seguro se tem mais chaves que o mínimo exigido (T-1)
func (n *Node) IsSafeForDelete() bool {
	return n.N > n.T-1
}

func (n *Node) IsFull() bool {
	return n.N == 2*n.T-1
}

func (n *Node) Search(key types.Comparable) (*Node, bool) {
	i := 0
	// Em B+ Tree, se key >= n.Keys[i], vamos para Children[i+1]
	// porque o separador é a menor chave da subárvore direita
	for i < n.N && key.Compare(n.Keys[i]) >= 0 {
		i++
	}

	if n.Leaf {
		// Na folha, precisamos buscar a chave exata
		for j := 0; j < n.N; j++ {
			if key.Compare(n.Keys[j]) == 0 {
				return n, true
			}
		}
		return nil, false
	}

	return n.Children[i].Search(key)
}

func (n *Node) findLeafLowerBound(key types.Comparable) (*Node, int) {
	i := sort.Search(n.N, func(i int) bool {
		return n.Keys[i].Compare(key) >= 0
	})

	if n.Leaf {
		return n, i
	}

	return n.Children[i].findLeafLowerBound(key)
}

func (n *Node) InsertNonFull(key types.Comparable, dataPtr int64, uniqueKey bool) error {
	i := n.N - 1

	if n.Leaf {
		// Encontra posição de inserção
		idx := sort.Search(n.N, func(j int) bool {
			return n.Keys[j].Compare(key) >= 0
		})

		// Se a chave já existe
		if idx < n.N && n.Keys[idx].Compare(key) == 0 {
			if uniqueKey {
				return &errors.DuplicateKeyError{Key: fmt.Sprintf("%v", key)}
			}
			// Se não for único, apenas atualiza o valor
			n.DataPtrs[idx] = dataPtr
			return nil
		}

		// Abre espaço para a nova chave
		n.Keys = append(n.Keys, nil)
		n.DataPtrs = append(n.DataPtrs, 0)
		copy(n.Keys[idx+1:], n.Keys[idx:])
		copy(n.DataPtrs[idx+1:], n.DataPtrs[idx:])

		n.Keys[idx] = key
		n.DataPtrs[idx] = dataPtr
		n.N++
		return nil
	}

	// Nó interno: encontra o filho correto
	for i >= 0 && key.Compare(n.Keys[i]) < 0 {
		i--
	}
	i++

	if n.Children[i].N == 2*n.T-1 {
		n.SplitChild(i)
		if key.Compare(n.Keys[i]) >= 0 {
			i++
		}
	}
	return n.Children[i].InsertNonFull(key, dataPtr, uniqueKey)
}

// UpsertNonFull realiza a inserção ou atualização na folha, executando o callback
func (n *Node) UpsertNonFull(key types.Comparable, fn func(oldValue int64, exists bool) (newValue int64, err error)) error {
	i := n.N - 1

	if n.Leaf {
		// Encontra posição de inserção
		idx := sort.Search(n.N, func(j int) bool {
			return n.Keys[j].Compare(key) >= 0
		})

		// Se a chave já existe
		if idx < n.N && n.Keys[idx].Compare(key) == 0 {
			// Callback com exists=true
			newValue, err := fn(n.DataPtrs[idx], true)
			if err != nil {
				return err
			}
			n.DataPtrs[idx] = newValue
			return nil
		}

		// Callback com exists=false
		newValue, err := fn(0, false)
		if err != nil {
			return err
		}

		// Abre espaço para a nova chave
		n.Keys = append(n.Keys, nil)
		n.DataPtrs = append(n.DataPtrs, 0)
		copy(n.Keys[idx+1:], n.Keys[idx:])
		copy(n.DataPtrs[idx+1:], n.DataPtrs[idx:])

		n.Keys[idx] = key
		n.DataPtrs[idx] = newValue
		n.N++
		return nil
	}

	// Nó interno: encontra o filho correto (código similar ao InsertNonFull, mas chama UpsertNonFull recursivamente)
	// Como upsertTopDown já cuida da descida, UpsertNonFull só deve ser chamado em FOLHA no design atual (Split Preventivo).
	// Mas se houvesse lógica recursiva aqui (sem split preventivo), precisaríamos replicar logic.
	// No btree.go, usamos upsertTopDown que desce até a folha.
	// O UpsertNonFull foi chamado em curr (que é garantido ser folha no upsertTopDown).
	// Portanto, não precisamos implementar a parte de nó interno aqui se apenas usarmos com upsertTopDown.
	// Mas para ser consistente com InsertNonFull:
	for i >= 0 && key.Compare(n.Keys[i]) < 0 {
		i--
	}
	i++

	if n.Children[i].N == 2*n.T-1 {
		n.SplitChild(i)
		if key.Compare(n.Keys[i]) >= 0 {
			i++
		}
	}
	return n.Children[i].UpsertNonFull(key, fn)
}

func (n *Node) SplitChild(i int) {
	t := n.T
	y := n.Children[i]
	z := NewNode(t, y.Leaf)

	// Se for folha, mantém a chave do meio na direita (propriedade B+ Tree)
	if y.Leaf {
		mid := t - 1
		z.N = y.N - mid
		z.Keys = append(z.Keys, y.Keys[mid:]...)
		z.DataPtrs = append(z.DataPtrs, y.DataPtrs[mid:]...)

		y.Keys = y.Keys[:mid]
		y.DataPtrs = y.DataPtrs[:mid]
		y.N = mid

		z.Next = y.Next
		y.Next = z
	} else {
		// Nó interno: chave do meio sobe e sai do filho
		mid := t - 1
		z.N = t - 1
		z.Keys = append(z.Keys, y.Keys[mid+1:]...)
		z.Children = append(z.Children, y.Children[mid+1:]...)

		upKey := y.Keys[mid]

		y.Keys = y.Keys[:mid]
		y.Children = y.Children[:mid+1]
		y.N = mid

		// Abre espaço no pai para a chave que sobe
		n.Keys = append(n.Keys, nil)
		copy(n.Keys[i+1:], n.Keys[i:])
		n.Keys[i] = upKey

		n.Children = append(n.Children, nil)
		copy(n.Children[i+2:], n.Children[i+1:])
		n.Children[i+1] = z
		n.N++
		return
	}

	// No caso de folha, a primeira chave do novo nó z sobe para o pai
	n.Keys = append(n.Keys, nil)
	copy(n.Keys[i+1:], n.Keys[i:])
	n.Keys[i] = z.Keys[0]

	n.Children = append(n.Children, nil)
	copy(n.Children[i+2:], n.Children[i+1:])
	n.Children[i+1] = z
	n.N++
}

func (n *Node) remove(key types.Comparable) bool {
	idx := sort.Search(n.N, func(i int) bool {
		return n.Keys[i].Compare(key) >= 0
	})

	if n.Leaf {
		if idx < n.N && n.Keys[idx].Compare(key) == 0 {
			n.Keys = append(n.Keys[:idx], n.Keys[idx+1:]...)
			n.DataPtrs = append(n.DataPtrs[:idx], n.DataPtrs[idx+1:]...)
			n.N--
			return true
		}
		return false
	}

	// Se a chave estiver no nó interno (como separador), o valor real está na folha à direita
	// Na B+ Tree, apenas descemos.
	childIdx := idx
	if idx < n.N && n.Keys[idx].Compare(key) == 0 {
		childIdx = idx + 1
	}

	child := n.Children[childIdx]
	if child.N < n.T {
		n.fill(childIdx)
	}

	// Após rebalancear, a chave pode ter mudado de filho
	return n.removeRecursive(key)
}

func (n *Node) removeRecursive(key types.Comparable) bool {
	idx := sort.Search(n.N, func(i int) bool {
		return n.Keys[i].Compare(key) >= 0
	})

	childIdx := idx
	if idx < n.N && n.Keys[idx].Compare(key) == 0 {
		childIdx = idx + 1
	}

	// Se o filho foi fundido, childIdx pode estar fora agora
	if childIdx > n.N {
		childIdx = n.N
	}

	ok := n.Children[childIdx].remove(key)

	// Sincroniza separadores se necessário (após deleção na folha)
	if ok {
		n.fixSeparators()
	}

	return ok
}

func (n *Node) fixSeparators() {
	if n.Leaf {
		return
	}
	for i := 0; i < n.N; i++ {
		// No B+ Tree, o separador i é a menor chave da subárvore Children[i+1]
		curr := n.Children[i+1]
		for !curr.Leaf {
			curr = curr.Children[0]
		}
		if curr.N > 0 {
			n.Keys[i] = curr.Keys[0]
		}
	}
}

func (n *Node) fill(i int) {
	if i != 0 && n.Children[i-1].N >= n.T {
		n.borrowFromPrev(i)
	} else if i != n.N && n.Children[i+1].N >= n.T {
		n.borrowFromNext(i)
	} else {
		if i != n.N {
			n.merge(i)
		} else {
			n.merge(i - 1)
		}
	}
}

func (n *Node) borrowFromPrev(i int) {
	child := n.Children[i]
	sibling := n.Children[i-1]

	if child.Leaf {
		child.Keys = append([]types.Comparable{nil}, child.Keys...)
		child.DataPtrs = append([]int64{0}, child.DataPtrs...)
		child.Keys[0] = sibling.Keys[sibling.N-1]
		child.DataPtrs[0] = sibling.DataPtrs[sibling.N-1]
		child.N++

		sibling.Keys = sibling.Keys[:sibling.N-1]
		sibling.DataPtrs = sibling.DataPtrs[:sibling.N-1]
		sibling.N--

		n.Keys[i-1] = child.Keys[0]
	} else {
		child.Keys = append([]types.Comparable{nil}, child.Keys...)
		child.Children = append([]*Node{nil}, child.Children...)
		child.Keys[0] = n.Keys[i-1]
		child.Children[0] = sibling.Children[sibling.N]
		child.N++

		n.Keys[i-1] = sibling.Keys[sibling.N-1]
		sibling.Keys = sibling.Keys[:sibling.N-1]
		sibling.Children = sibling.Children[:sibling.N]
		sibling.N--
	}
}

func (n *Node) borrowFromNext(i int) {
	child := n.Children[i]
	sibling := n.Children[i+1]

	if child.Leaf {
		child.Keys = append(child.Keys, sibling.Keys[0])
		child.DataPtrs = append(child.DataPtrs, sibling.DataPtrs[0])
		child.N++

		sibling.Keys = append([]types.Comparable{}, sibling.Keys[1:]...)
		sibling.DataPtrs = append([]int64{}, sibling.DataPtrs[1:]...)
		sibling.N--

		n.Keys[i] = sibling.Keys[0]
	} else {
		child.Keys = append(child.Keys, n.Keys[i])
		child.Children = append(child.Children, sibling.Children[0])
		child.N++

		n.Keys[i] = sibling.Keys[0]
		sibling.Keys = append([]types.Comparable{}, sibling.Keys[1:]...)
		sibling.Children = append([]*Node{}, sibling.Children[1:]...)
		sibling.N--
	}
}

func (n *Node) merge(i int) {
	child := n.Children[i]
	sibling := n.Children[i+1]

	if child.Leaf {
		child.Keys = append(child.Keys, sibling.Keys...)
		child.DataPtrs = append(child.DataPtrs, sibling.DataPtrs...)
		child.Next = sibling.Next
		child.N = len(child.Keys)
	} else {
		child.Keys = append(child.Keys, n.Keys[i])
		child.Keys = append(child.Keys, sibling.Keys...)
		child.Children = append(child.Children, sibling.Children...)
		child.N = len(child.Keys)
	}

	n.Keys = append(n.Keys[:i], n.Keys[i+1:]...)
	n.Children = append(n.Children[:i+1], n.Children[i+2:]...)
	n.N--
}

// Exported methods for testing/internal project use
func (n *Node) Remove(key types.Comparable) bool {
	return n.remove(key)
}
func (n *Node) FindLeafLowerBound(key types.Comparable) (*Node, int) {
	return n.findLeafLowerBound(key)
}
