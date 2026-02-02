package btree

import (
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/errors"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// Helper modificado para aceitar []int como chaves mas converter para []Comparable
func newNodeWithData(t int, leaf bool, keys []int, data []int, children []*Node) *Node {
	n := NewNode(t, leaf)
	for _, k := range keys {
		n.Keys = append(n.Keys, types.IntKey(k))
	}
	n.DataPtrs = append(n.DataPtrs, data...)
	n.Children = append(n.Children, children...)
	n.N = len(n.Keys)
	return n
}

// Helper para chaves genéricas (usado nos testes de string)
func newNodeWithKeys(t int, leaf bool, keys []types.Comparable, data []int, children []*Node) *Node {
	n := NewNode(t, leaf)
	n.Keys = append(n.Keys, keys...)
	n.DataPtrs = append(n.DataPtrs, data...)
	n.Children = append(n.Children, children...)
	n.N = len(n.Keys)
	return n
}

func TestSplitChild_Leaf(t *testing.T) {
	tVal := 3
	childLeft := newNodeWithData(tVal, true,
		[]int{10, 20, 30, 40, 50},
		[]int{1, 2, 3, 4, 5},
		nil,
	)
	oldNext := NewNode(tVal, true)
	childLeft.Next = oldNext

	parent := NewNode(tVal, false)
	parent.Children = append(parent.Children, childLeft)

	parent.SplitChild(0)

	if len(parent.Keys) != 1 || parent.Keys[0].Compare(types.IntKey(30)) != 0 {
		t.Fatalf("parent keys = %v, want [30]", parent.Keys)
	}
	if len(parent.Children) != 2 {
		t.Fatalf("parent children len = %d, want 2", len(parent.Children))
	}

	left := parent.Children[0]
	right := parent.Children[1]

	if !left.Leaf || !right.Leaf {
		t.Fatalf("expected both children to be leaves")
	}

	// Verifica keys da esquerda
	if got := left.Keys; len(got) != 2 || got[0].Compare(types.IntKey(10)) != 0 || got[1].Compare(types.IntKey(20)) != 0 {
		t.Fatalf("left keys = %v, want [10 20]", got)
	}
	// Verifica keys da direita
	if got := right.Keys; len(got) != 3 || got[0].Compare(types.IntKey(30)) != 0 || got[1].Compare(types.IntKey(40)) != 0 || got[2].Compare(types.IntKey(50)) != 0 {
		t.Fatalf("right keys = %v, want [30 40 50]", got)
	}

	if got := left.DataPtrs; len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("left dataptrs = %v, want [1 2]", got)
	}
	if got := right.DataPtrs; len(got) != 3 || got[0] != 3 || got[1] != 4 || got[2] != 5 {
		t.Fatalf("right dataptrs = %v, want [3 4 5]", got)
	}

	if left.Next != right {
		t.Fatalf("left.Next should point to right child")
	}
	if right.Next != oldNext {
		t.Fatalf("right.Next should preserve previous Next")
	}

	if left.N != 2 || right.N != 3 || parent.N != 1 {
		t.Fatalf("unexpected N values: left=%d right=%d parent=%d", left.N, right.N, parent.N)
	}
}

func TestSplitChild_Internal(t *testing.T) {
	tVal := 3
	// 5 keys, 6 children
	children := []*Node{
		NewNode(tVal, true),
		NewNode(tVal, true),
		NewNode(tVal, true),
		NewNode(tVal, true),
		NewNode(tVal, true),
		NewNode(tVal, true),
	}
	childLeft := newNodeWithData(tVal, false,
		[]int{10, 20, 30, 40, 50},
		nil,
		children,
	)

	parent := NewNode(tVal, false)
	parent.Children = append(parent.Children, childLeft)

	parent.SplitChild(0)

	if len(parent.Keys) != 1 || parent.Keys[0].Compare(types.IntKey(30)) != 0 {
		t.Fatalf("parent keys = %v, want [30]", parent.Keys)
	}
	if len(parent.Children) != 2 {
		t.Fatalf("parent children len = %d, want 2", len(parent.Children))
	}

	left := parent.Children[0]
	right := parent.Children[1]

	if left.Leaf || right.Leaf {
		t.Fatalf("expected both children to be internal nodes")
	}

	if got := left.Keys; len(got) != 2 || got[0].Compare(types.IntKey(10)) != 0 || got[1].Compare(types.IntKey(20)) != 0 {
		t.Fatalf("left keys = %v, want [10 20]", got)
	}
	if got := right.Keys; len(got) != 2 || got[0].Compare(types.IntKey(40)) != 0 || got[1].Compare(types.IntKey(50)) != 0 {
		t.Fatalf("right keys = %v, want [40 50]", got)
	}

	if got := left.Children; len(got) != 3 || got[0] != children[0] || got[1] != children[1] || got[2] != children[2] {
		t.Fatalf("left children unexpected: %v", got)
	}
	if got := right.Children; len(got) != 3 || got[0] != children[3] || got[1] != children[4] || got[2] != children[5] {
		t.Fatalf("right children unexpected: %v", got)
	}

	if left.N != 2 || right.N != 2 || parent.N != 1 {
		t.Fatalf("unexpected N values: left=%d right=%d parent=%d", left.N, right.N, parent.N)
	}

	if left.Next != nil || right.Next != nil {
		t.Errorf("internal nodes should not have Next pointers")
	}
	if len(left.DataPtrs) > 0 || len(right.DataPtrs) > 0 {
		t.Errorf("internal nodes should not have Data pointers")
	}
}

func TestInsertNonFull_LeafOrdering(t *testing.T) {
	cases := []struct {
		name      string
		startKeys []int
		startData []int
		key       int
		dataPtr   int
		wantKeys  []int
		wantData  []int
	}{
		{
			name:      "insert-begin",
			startKeys: []int{20, 30, 40},
			startData: []int{2, 3, 4},
			key:       10,
			dataPtr:   1,
			wantKeys:  []int{10, 20, 30, 40},
			wantData:  []int{1, 2, 3, 4},
		},
		{
			name:      "insert-middle",
			startKeys: []int{10, 30, 40},
			startData: []int{1, 3, 4},
			key:       20,
			dataPtr:   2,
			wantKeys:  []int{10, 20, 30, 40},
			wantData:  []int{1, 2, 3, 4},
		},
		{
			name:      "insert-end",
			startKeys: []int{10, 20, 30},
			startData: []int{1, 2, 3},
			key:       40,
			dataPtr:   4,
			wantKeys:  []int{10, 20, 30, 40},
			wantData:  []int{1, 2, 3, 4},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			leaf := newNodeWithData(3, true, tc.startKeys, tc.startData, nil)
			leaf.InsertNonFull(types.IntKey(tc.key), tc.dataPtr, false)

			if got := leaf.Keys; len(got) != len(tc.wantKeys) {
				t.Fatalf("keys len = %d, want %d", len(got), len(tc.wantKeys))
			}
			for i := range tc.wantKeys {
				if leaf.Keys[i].Compare(types.IntKey(tc.wantKeys[i])) != 0 {
					t.Fatalf("keys = %v, want %v", leaf.Keys, tc.wantKeys)
				}
			}
			for i := range tc.wantData {
				if leaf.DataPtrs[i] != tc.wantData[i] {
					t.Fatalf("dataptrs = %v, want %v", leaf.DataPtrs, tc.wantData)
				}
			}
		})
	}
}

func TestInsertNonFull_InternalRouting(t *testing.T) {
	makeParent := func() *Node {
		tVal := 3
		c0 := newNodeWithData(tVal, true, []int{5}, []int{50}, nil)
		c1 := newNodeWithData(tVal, true, []int{15}, []int{150}, nil)
		c2 := newNodeWithData(tVal, true, []int{25}, []int{250}, nil)
		parent := newNodeWithData(tVal, false, []int{10, 20}, nil, []*Node{c0, c1, c2})
		return parent
	}

	t.Run("route-leftmost", func(t *testing.T) {
		parent := makeParent()
		parent.InsertNonFull(types.IntKey(7), 70, false)
		if got := parent.Children[0].Keys; len(got) != 2 || got[0].Compare(types.IntKey(5)) != 0 || got[1].Compare(types.IntKey(7)) != 0 {
			t.Fatalf("child0 keys = %v, want [5 7]", got)
		}
		if got := parent.Children[1].Keys; len(got) != 1 || got[0].Compare(types.IntKey(15)) != 0 {
			t.Fatalf("child1 keys changed unexpectedly: %v", got)
		}
		if got := parent.Children[2].Keys; len(got) != 1 || got[0].Compare(types.IntKey(25)) != 0 {
			t.Fatalf("child2 keys changed unexpectedly: %v", got)
		}
	})

	t.Run("route-rightmost", func(t *testing.T) {
		parent := makeParent()
		parent.InsertNonFull(types.IntKey(27), 270, false)
		if got := parent.Children[2].Keys; len(got) != 2 || got[0].Compare(types.IntKey(25)) != 0 || got[1].Compare(types.IntKey(27)) != 0 {
			t.Fatalf("child2 keys = %v, want [25 27]", got)
		}
		if got := parent.Children[0].Keys; len(got) != 1 || got[0].Compare(types.IntKey(5)) != 0 {
			t.Fatalf("child0 keys changed unexpectedly: %v", got)
		}
		if got := parent.Children[1].Keys; len(got) != 1 || got[0].Compare(types.IntKey(15)) != 0 {
			t.Fatalf("child1 keys changed unexpectedly: %v", got)
		}
	})

	t.Run("route-middle", func(t *testing.T) {
		parent := makeParent()
		parent.InsertNonFull(types.IntKey(17), 170, false)
		if got := parent.Children[1].Keys; len(got) != 2 || got[0].Compare(types.IntKey(15)) != 0 || got[1].Compare(types.IntKey(17)) != 0 {
			t.Fatalf("child1 keys = %v, want [15 17]", got)
		}
		if got := parent.Children[0].Keys; len(got) != 1 || got[0].Compare(types.IntKey(5)) != 0 {
			t.Fatalf("child0 keys changed unexpectedly: %v", got)
		}
		if got := parent.Children[2].Keys; len(got) != 1 || got[0].Compare(types.IntKey(25)) != 0 {
			t.Fatalf("child2 keys changed unexpectedly: %v", got)
		}
	})
}

func TestInsertNonFull_SplitPreventivo(t *testing.T) {
	tVal := 3
	fullChild := newNodeWithData(tVal, true,
		[]int{10, 20, 30, 40, 50},
		[]int{1, 2, 3, 4, 5},
		nil,
	)
	parent := newNodeWithData(tVal, false, nil, nil, []*Node{fullChild})

	parent.InsertNonFull(types.IntKey(35), 35, false)

	if len(parent.Keys) != 1 || parent.Keys[0].Compare(types.IntKey(30)) != 0 {
		t.Fatalf("parent keys = %v, want [30]", parent.Keys)
	}
	if len(parent.Children) != 2 {
		t.Fatalf("parent children len = %d, want 2", len(parent.Children))
	}

	left := parent.Children[0]
	right := parent.Children[1]

	if got := left.Keys; len(got) != 2 || got[0].Compare(types.IntKey(10)) != 0 || got[1].Compare(types.IntKey(20)) != 0 {
		t.Fatalf("left keys = %v, want [10 20]", got)
	}
	if got := right.Keys; len(got) != 4 || got[0].Compare(types.IntKey(30)) != 0 || got[1].Compare(types.IntKey(35)) != 0 || got[2].Compare(types.IntKey(40)) != 0 || got[3].Compare(types.IntKey(50)) != 0 {
		t.Fatalf("right keys = %v, want [30 35 40 50]", got)
	}
}

func TestInsertNonFull_SplitBoundaryKey(t *testing.T) {
	makeParent := func() *Node {
		tVal := 3
		fullChild := newNodeWithData(tVal, true,
			[]int{10, 20, 30, 40, 50},
			[]int{1, 2, 3, 4, 5},
			nil,
		)
		return newNodeWithData(tVal, false, nil, nil, []*Node{fullChild})
	}

	t.Run("key-29-goes-left", func(t *testing.T) {
		parent := makeParent()
		parent.InsertNonFull(types.IntKey(29), 29, false)

		left := parent.Children[0]
		// right := parent.Children[1]
		if got := left.Keys; len(got) != 3 || got[0].Compare(types.IntKey(10)) != 0 || got[1].Compare(types.IntKey(20)) != 0 || got[2].Compare(types.IntKey(29)) != 0 {
			t.Fatalf("left keys = %v, want [10 20 29]", got)
		}
	})

	t.Run("key-30-goes-right", func(t *testing.T) {
		parent := makeParent()
		parent.InsertNonFull(types.IntKey(30), 30, false)

		left := parent.Children[0]
		right := parent.Children[1]
		if got := left.Keys; len(got) != 2 || got[0].Compare(types.IntKey(10)) != 0 || got[1].Compare(types.IntKey(20)) != 0 {
			t.Fatalf("left keys = %v, want [10 20]", got)
		}
		if got := right.Keys; len(got) != 3 || got[0].Compare(types.IntKey(30)) != 0 || got[1].Compare(types.IntKey(40)) != 0 || got[2].Compare(types.IntKey(50)) != 0 {
			t.Fatalf("right keys = %v, want [30 40 50]", got)
		}
	})

	t.Run("key-31-goes-right", func(t *testing.T) {
		parent := makeParent()
		parent.InsertNonFull(types.IntKey(31), 31, false)

		left := parent.Children[0]
		right := parent.Children[1]
		if got := left.Keys; len(got) != 2 || got[0].Compare(types.IntKey(10)) != 0 || got[1].Compare(types.IntKey(20)) != 0 {
			t.Fatalf("left keys = %v, want [10 20]", got)
		}
		if got := right.Keys; len(got) != 4 || got[0].Compare(types.IntKey(30)) != 0 || got[1].Compare(types.IntKey(31)) != 0 || got[2].Compare(types.IntKey(40)) != 0 || got[3].Compare(types.IntKey(50)) != 0 {
			t.Fatalf("right keys = %v, want [30 31 40 50]", got)
		}
	})
}

func TestDelete_SimpleNoUnderflow(t *testing.T) {
	tVal := 3 // min keys = 2
	leaf := newNodeWithData(tVal, true, []int{10, 20, 30}, []int{1, 2, 3}, nil)
	tree := &BPlusTree{T: tVal, Root: leaf}

	ok := tree.Root.remove(types.IntKey(20))

	if !ok {
		t.Fatalf("expected delete to return true")
	}
	if got := leaf.Keys; len(got) != 2 || got[0].Compare(types.IntKey(10)) != 0 || got[1].Compare(types.IntKey(30)) != 0 {
		t.Fatalf("keys after delete = %v, want [10 30]", got)
	}
	if got := leaf.DataPtrs; len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("dataptrs after delete = %v, want [1 3]", got)
	}
	if leaf.N != 2 {
		t.Fatalf("leaf.N = %d, want 2", leaf.N)
	}
}

func TestDelete_BorrowFromPrev(t *testing.T) {
	tVal := 3
	left := newNodeWithData(tVal, true, []int{5, 6, 7, 8}, []int{50, 60, 70, 80}, nil) // rico
	target := newNodeWithData(tVal, true, []int{20, 30}, []int{200, 300}, nil)         // vai underflow
	right := newNodeWithData(tVal, true, []int{40, 50}, []int{400, 500}, nil)

	parent := newNodeWithData(tVal, false, []int{20, 40}, nil, []*Node{left, target, right})

	ok := parent.remove(types.IntKey(20))
	if !ok {
		t.Fatalf("delete should succeed")
	}
	if got := target.Keys; len(got) != 2 || got[0].Compare(types.IntKey(8)) != 0 || got[1].Compare(types.IntKey(30)) != 0 {
		t.Fatalf("target keys = %v, want [8 30]", got)
	}
	if got := target.DataPtrs; len(got) != 2 || got[0] != 80 || got[1] != 300 {
		t.Fatalf("target dataptrs = %v, want [80 300]", got)
	}
	if parent.Keys[0].Compare(types.IntKey(8)) != 0 {
		t.Fatalf("parent separator updated to %v, want 8", parent.Keys[0])
	}
}

func TestDelete_BorrowFromNext(t *testing.T) {
	tVal := 3
	target := newNodeWithData(tVal, true, []int{10, 20}, []int{100, 200}, nil) // vai underflow
	right := newNodeWithData(tVal, true, []int{40, 50, 60, 70}, []int{400, 500, 600, 700}, nil)

	parent := newNodeWithData(tVal, false, []int{40}, nil, []*Node{target, right})

	ok := parent.remove(types.IntKey(10))
	if !ok {
		t.Fatalf("delete should succeed")
	}
	if got := target.Keys; len(got) != 2 || got[0].Compare(types.IntKey(20)) != 0 || got[1].Compare(types.IntKey(40)) != 0 {
		t.Fatalf("target keys = %v, want [20 40]", got)
	}
	if parent.Keys[0].Compare(types.IntKey(50)) != 0 {
		t.Fatalf("parent separator = %v, want 50", parent.Keys[0])
	}
	if got := right.Keys; len(got) != 3 || got[0].Compare(types.IntKey(50)) != 0 || got[1].Compare(types.IntKey(60)) != 0 || got[2].Compare(types.IntKey(70)) != 0 {
		t.Fatalf("right keys = %v, want [50 60 70]", got)
	}
}

func TestDelete_MergeLeaves(t *testing.T) {
	tVal := 3
	left := newNodeWithData(tVal, true, []int{10, 20}, []int{100, 200}, nil)
	mid := newNodeWithData(tVal, true, []int{31, 32}, []int{310, 320}, nil)
	right := newNodeWithData(tVal, true, []int{50, 60}, []int{500, 600}, nil)
	left.Next = mid
	mid.Next = right

	parent := newNodeWithData(tVal, false, []int{30, 50}, nil, []*Node{left, mid, right})

	ok := parent.remove(types.IntKey(31))
	if !ok {
		t.Fatalf("delete should succeed")
	}
	// mid should have merged with right (idx=1 merge idx)
	merged := parent.Children[1]
	if got := merged.Keys; len(got) != 3 || got[0].Compare(types.IntKey(32)) != 0 || got[1].Compare(types.IntKey(50)) != 0 || got[2].Compare(types.IntKey(60)) != 0 {
		t.Fatalf("merged keys = %v, want [32 50 60]", got)
	}
	if got := merged.DataPtrs; len(got) != 3 || got[0] != 320 || got[1] != 500 || got[2] != 600 {
		t.Fatalf("merged dataptrs = %v, want [320 500 600]", got)
	}
	if parent.N != 1 || len(parent.Keys) != 1 || parent.Keys[0].Compare(types.IntKey(32)) != 0 {
		t.Fatalf("parent keys after merge = %v (N=%d), want [32]", parent.Keys, parent.N)
	}
	if left.Next != merged || merged.Next != nil {
		t.Fatalf("Next pointers incorrect: left.Next=%v merged.Next=%v", left.Next, merged.Next)
	}
}

func TestDelete_RootCollapses(t *testing.T) {
	tVal := 3
	left := newNodeWithData(tVal, true, []int{10, 20}, []int{100, 200}, nil)
	right := newNodeWithData(tVal, true, []int{30, 40}, []int{300, 400}, nil)
	root := newNodeWithData(tVal, false, []int{30}, nil, []*Node{left, right})
	tree := &BPlusTree{T: tVal, Root: root}

	// Note: Este teste testa diretamente a árvore, não o StorageEngine
	// pois testa comportamento interno de merge
	ok := tree.Root.remove(types.IntKey(40))
	if !ok {
		t.Fatalf("delete should succeed")
	}

	// Collapse root quando fica vazio
	if tree.Root.N == 0 && !tree.Root.Leaf {
		tree.Root = tree.Root.Children[0]
	}
	// Merge collapses root
	if tree.Root.Leaf != true {
		t.Fatalf("root should now be leaf")
	}
	if got := tree.Root.Keys; len(got) != 3 || got[0].Compare(types.IntKey(10)) != 0 || got[1].Compare(types.IntKey(20)) != 0 || got[2].Compare(types.IntKey(30)) != 0 {
		t.Fatalf("new root keys = %v, want [10 20 30]", got)
	}
	if tree.Root.N != 3 {
		t.Fatalf("root.N = %d, want 3", tree.Root.N)
	}
}

func TestDelete_MissingKey(t *testing.T) {
	tVal := 3
	leaf := newNodeWithData(tVal, true, []int{10, 20, 30}, []int{1, 2, 3}, nil)
	tree := &BPlusTree{T: tVal, Root: leaf}

	ok := tree.Root.remove(types.IntKey(9999))
	if ok {
		t.Fatalf("expected delete missing key to return false")
	}
	if got := leaf.Keys; len(got) != 3 || got[0].Compare(types.IntKey(10)) != 0 || got[1].Compare(types.IntKey(20)) != 0 || got[2].Compare(types.IntKey(30)) != 0 {
		t.Fatalf("tree modified unexpectedly: %v", got)
	}
	if leaf.N != 3 {
		t.Fatalf("leaf.N changed to %d, want 3", leaf.N)
	}
}

// === NOVOS TESTES PARA OUTROS TIPOS DE DADOS ===

// =============================================
// TESTES PARA VarcharKey
// =============================================

func TestVarcharKey_InsertAndOrdering(t *testing.T) {
	tree := NewTree(3)

	// Inserção desordenada
	tree.Insert(types.VarcharKey("banana"), 1)
	tree.Insert(types.VarcharKey("apple"), 2)
	tree.Insert(types.VarcharKey("cherry"), 3)
	tree.Insert(types.VarcharKey("date"), 4)

	// Busca
	node, found := tree.Search(types.VarcharKey("apple"))
	if !found {
		t.Fatal("should find apple")
	}

	// Verifica ordem alfabética: apple, banana, cherry, date
	expectedOrder := []types.VarcharKey{"apple", "banana", "cherry", "date"}
	for i, expected := range expectedOrder {
		if node.Keys[i].Compare(expected) != 0 {
			t.Fatalf("index %d: expected %v, got %v", i, expected, node.Keys[i])
		}
	}
}

func TestVarcharKey_Split(t *testing.T) {
	tree := NewTree(3) // max keys = 5

	// Insere 6 para forçar split
	tree.Insert(types.VarcharKey("apple"), 1)
	tree.Insert(types.VarcharKey("banana"), 2)
	tree.Insert(types.VarcharKey("cherry"), 3)
	tree.Insert(types.VarcharKey("date"), 4)
	tree.Insert(types.VarcharKey("elderberry"), 5)
	tree.Insert(types.VarcharKey("fig"), 6) // Deve causar split

	root := tree.Root
	if root.Leaf {
		t.Fatal("Root should not be leaf after split")
	}

	// Verifica se a chave que subiu é 'cherry' (midIndex = 2 com 5 keys)
	if root.Keys[0].Compare(types.VarcharKey("cherry")) != 0 {
		t.Fatalf("Expected root key to be 'cherry', got %v", root.Keys[0])
	}

	// Verifica filhos
	if len(root.Children) != 2 {
		t.Fatalf("Expected 2 children, got %d", len(root.Children))
	}

	// Filho esquerdo: apple, banana
	left := root.Children[0]
	if len(left.Keys) != 2 {
		t.Fatalf("Left child should have 2 keys, got %d", len(left.Keys))
	}
	if left.Keys[0].Compare(types.VarcharKey("apple")) != 0 || left.Keys[1].Compare(types.VarcharKey("banana")) != 0 {
		t.Fatalf("Left keys wrong: %v", left.Keys)
	}

	// Filho direito: cherry, date, elderberry, fig
	right := root.Children[1]
	if len(right.Keys) != 4 {
		t.Fatalf("Right child should have 4 keys, got %d", len(right.Keys))
	}
}

func TestVarcharKey_DeleteSimple(t *testing.T) {
	tree := NewTree(3)

	tree.Insert(types.VarcharKey("apple"), 1)
	tree.Insert(types.VarcharKey("banana"), 2)
	tree.Insert(types.VarcharKey("cherry"), 3)

	ok := tree.Root.remove(types.VarcharKey("banana"))
	if !ok {
		t.Fatal("delete should succeed")
	}

	_, found := tree.Search(types.VarcharKey("banana"))
	if found {
		t.Fatal("banana should be deleted")
	}

	// Verifica ordem restante: apple, cherry
	if tree.Root.N != 2 {
		t.Fatalf("Expected 2 keys, got %d", tree.Root.N)
	}
}

func TestVarcharKey_DeleteWithBorrowAndMerge(t *testing.T) {
	tVal := 3

	// Cria estrutura manualmente para testar borrow
	left := newNodeWithKeys(tVal, true,
		[]types.Comparable{types.VarcharKey("a"), types.VarcharKey("b"), types.VarcharKey("c"), types.VarcharKey("d")},
		[]int{1, 2, 3, 4}, nil)
	target := newNodeWithKeys(tVal, true,
		[]types.Comparable{types.VarcharKey("m"), types.VarcharKey("n")},
		[]int{13, 14}, nil)
	right := newNodeWithKeys(tVal, true,
		[]types.Comparable{types.VarcharKey("x"), types.VarcharKey("y")},
		[]int{24, 25}, nil)

	parent := newNodeWithKeys(tVal, false,
		[]types.Comparable{types.VarcharKey("m"), types.VarcharKey("x")},
		nil, []*Node{left, target, right})

	// Delete "m" do target - deve fazer borrow do left
	ok := parent.remove(types.VarcharKey("m"))
	if !ok {
		t.Fatal("delete should succeed")
	}

	// Target deve ter pego "d" do irmão esquerdo
	if target.Keys[0].Compare(types.VarcharKey("d")) != 0 {
		t.Fatalf("Expected 'd' borrowed, got %v", target.Keys[0])
	}
}

// =============================================
// TESTES PARA FloatKey
// =============================================

func TestFloatKey_InsertAndOrdering(t *testing.T) {
	tree := NewTree(3)

	tree.Insert(types.FloatKey(3.14), 1)
	tree.Insert(types.FloatKey(1.41), 2)
	tree.Insert(types.FloatKey(2.71), 3)

	node, found := tree.Search(types.FloatKey(1.41))
	if !found {
		t.Fatal("should find 1.41")
	}

	// Verifica ordem: 1.41, 2.71, 3.14
	if node.Keys[0].Compare(types.FloatKey(1.41)) != 0 {
		t.Fatalf("index 0: expected 1.41, got %v", node.Keys[0])
	}
	if node.Keys[1].Compare(types.FloatKey(2.71)) != 0 {
		t.Fatalf("index 1: expected 2.71, got %v", node.Keys[1])
	}
	if node.Keys[2].Compare(types.FloatKey(3.14)) != 0 {
		t.Fatalf("index 2: expected 3.14, got %v", node.Keys[2])
	}
}

func TestFloatKey_Split(t *testing.T) {
	tree := NewTree(3)

	// Insere 6 floats para forçar split
	tree.Insert(types.FloatKey(1.0), 1)
	tree.Insert(types.FloatKey(2.0), 2)
	tree.Insert(types.FloatKey(3.0), 3)
	tree.Insert(types.FloatKey(4.0), 4)
	tree.Insert(types.FloatKey(5.0), 5)
	tree.Insert(types.FloatKey(6.0), 6)

	if tree.Root.Leaf {
		t.Fatal("Root should not be leaf after split")
	}

	// midIndex = 2 -> chave 3.0 sobe
	if tree.Root.Keys[0].Compare(types.FloatKey(3.0)) != 0 {
		t.Fatalf("Expected root key 3.0, got %v", tree.Root.Keys[0])
	}
}

func TestFloatKey_DeleteWithMerge(t *testing.T) {
	tVal := 3

	// Cria dois filhos com mínimo de chaves
	left := newNodeWithKeys(tVal, true,
		[]types.Comparable{types.FloatKey(1.0), types.FloatKey(2.0)},
		[]int{1, 2}, nil)
	right := newNodeWithKeys(tVal, true,
		[]types.Comparable{types.FloatKey(3.0), types.FloatKey(4.0)},
		[]int{3, 4}, nil)
	left.Next = right

	root := newNodeWithKeys(tVal, false,
		[]types.Comparable{types.FloatKey(3.0)},
		nil, []*Node{left, right})
	tree := &BPlusTree{T: tVal, Root: root}

	// Note: Este teste testa diretamente a árvore
	ok := tree.Root.remove(types.FloatKey(4.0))
	if !ok {
		t.Fatal("delete should succeed")
	}

	// Collapse root
	if tree.Root.N == 0 && !tree.Root.Leaf {
		tree.Root = tree.Root.Children[0]
	}

	// Após merge, root deve ser folha
	if !tree.Root.Leaf {
		t.Fatal("Root should be leaf after merge collapse")
	}

	// Chaves restantes: 1.0, 2.0, 3.0
	if tree.Root.N != 3 {
		t.Fatalf("Expected 3 keys, got %d", tree.Root.N)
	}
}

// =============================================
// TESTES PARA BoolKey
// =============================================

func TestBoolKey_Ordering(t *testing.T) {
	tree := NewTree(3)

	tree.Insert(types.BoolKey(true), 1)
	tree.Insert(types.BoolKey(false), 0)

	node, found := tree.Search(types.BoolKey(false))
	if !found {
		t.Fatal("should find false")
	}

	// false < true
	if node.Keys[0].Compare(types.BoolKey(false)) != 0 {
		t.Fatal("false should be first")
	}
	if node.Keys[1].Compare(types.BoolKey(true)) != 0 {
		t.Fatal("true should be second")
	}
}

func TestBoolKey_Delete(t *testing.T) {
	tree := NewTree(3)

	tree.Insert(types.BoolKey(true), 1)
	tree.Insert(types.BoolKey(false), 0)

	ok := tree.Root.remove(types.BoolKey(true))
	if !ok {
		t.Fatal("delete should succeed")
	}

	_, found := tree.Search(types.BoolKey(true))
	if found {
		t.Fatal("true should be deleted")
	}

	if tree.Root.N != 1 {
		t.Fatalf("Expected 1 key, got %d", tree.Root.N)
	}
}

// =============================================
// TESTES PARA DateKey
// =============================================

func TestDateKey_InsertAndOrdering(t *testing.T) {
	tree := NewTree(3)

	now := time.Now()
	yesterday := now.Add(-24 * time.Hour)
	tomorrow := now.Add(24 * time.Hour)

	tree.Insert(types.DateKey(now), 1)
	tree.Insert(types.DateKey(tomorrow), 2)
	tree.Insert(types.DateKey(yesterday), 3)

	// Verifica ordem: yesterday, now, tomorrow
	leaf, _ := tree.findLeafLowerBound(types.DateKey(yesterday))
	if leaf == nil {
		t.Fatal("leaf not found")
	}

	if leaf.Keys[0].Compare(types.DateKey(yesterday)) != 0 {
		t.Fatal("First key should be yesterday")
	}
	if leaf.Keys[1].Compare(types.DateKey(now)) != 0 {
		t.Fatal("Second key should be now")
	}
	if leaf.Keys[2].Compare(types.DateKey(tomorrow)) != 0 {
		t.Fatal("Third key should be tomorrow")
	}
}

func TestDateKey_Split(t *testing.T) {
	tree := NewTree(3)

	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Insere 6 datas para forçar split
	for i := 0; i < 6; i++ {
		d := baseTime.Add(time.Duration(i) * 24 * time.Hour)
		tree.Insert(types.DateKey(d), i)
	}

	if tree.Root.Leaf {
		t.Fatal("Root should not be leaf after split")
	}

	// Verifica que a chave que subiu é a do dia 3 (índice 2 no slice ordenado de 5)
	expectedUp := baseTime.Add(2 * 24 * time.Hour)
	if tree.Root.Keys[0].Compare(types.DateKey(expectedUp)) != 0 {
		t.Fatalf("Expected root key to be %v, got %v", expectedUp, tree.Root.Keys[0])
	}
}

func TestDateKey_DeleteWithBorrow(t *testing.T) {
	tVal := 3

	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	d1 := types.DateKey(baseTime)
	d2 := types.DateKey(baseTime.Add(1 * 24 * time.Hour))
	d3 := types.DateKey(baseTime.Add(2 * 24 * time.Hour))
	d4 := types.DateKey(baseTime.Add(3 * 24 * time.Hour))
	d5 := types.DateKey(baseTime.Add(4 * 24 * time.Hour))
	d6 := types.DateKey(baseTime.Add(5 * 24 * time.Hour))

	// Left tem 4 chaves (rico), target tem 2 (mínimo)
	left := newNodeWithKeys(tVal, true,
		[]types.Comparable{d1, d2, d3, d4},
		[]int{1, 2, 3, 4}, nil)
	target := newNodeWithKeys(tVal, true,
		[]types.Comparable{d5, d6},
		[]int{5, 6}, nil)

	parent := newNodeWithKeys(tVal, false,
		[]types.Comparable{d5},
		nil, []*Node{left, target})

	// Deletar d5 - deve pegar d4 emprestado
	ok := parent.remove(d5)
	if !ok {
		t.Fatal("delete should succeed")
	}

	// Target deve ter d4 e d6 agora
	if target.Keys[0].Compare(d4) != 0 {
		t.Fatalf("Expected d4 borrowed, got %v", target.Keys[0])
	}
	if target.Keys[1].Compare(d6) != 0 {
		t.Fatalf("Expected d6 to remain, got %v", target.Keys[1])
	}
}

// =============================================
// TESTES PARA CHAVE ÚNICA
// =============================================

func TestUniqueKey_PreventsDuplicates(t *testing.T) {
	tree := NewUniqueTree(3)

	// Primeira inserção deve funcionar
	err := tree.Insert(types.IntKey(10), 100)
	if err != nil {
		t.Fatalf("first insert should succeed, got error: %v", err)
	}

	// Segunda inserção da mesma chave deve falhar
	err = tree.Insert(types.IntKey(10), 200)
	if err == nil {
		t.Fatal("expected error for duplicate key in unique index")
	}

	// Verifica que é o erro correto
	if _, ok := err.(*errors.DuplicateKeyError); !ok {
		t.Fatalf("expected DuplicateKeyError, got %T: %v", err, err)
	}

	// Verifica que o valor original não foi alterado
	node, found := tree.Search(types.IntKey(10))
	if !found {
		t.Fatal("key should still exist")
	}
	if node.DataPtrs[0] != 100 {
		t.Fatalf("expected original value 100, got %d", node.DataPtrs[0])
	}
}

func TestUniqueKey_AllowsDifferentKeys(t *testing.T) {
	tree := NewUniqueTree(3)

	// Inserir várias chaves diferentes deve funcionar
	err := tree.Insert(types.IntKey(10), 100)
	if err != nil {
		t.Fatalf("insert 10 failed: %v", err)
	}

	err = tree.Insert(types.IntKey(20), 200)
	if err != nil {
		t.Fatalf("insert 20 failed: %v", err)
	}

	err = tree.Insert(types.IntKey(30), 300)
	if err != nil {
		t.Fatalf("insert 30 failed: %v", err)
	}

	// Verifica que todas as chaves existem
	if _, found := tree.Search(types.IntKey(10)); !found {
		t.Fatal("key 10 should exist")
	}
	if _, found := tree.Search(types.IntKey(20)); !found {
		t.Fatal("key 20 should exist")
	}
	if _, found := tree.Search(types.IntKey(30)); !found {
		t.Fatal("key 30 should exist")
	}
}

func TestNonUniqueKey_AllowsDuplicates(t *testing.T) {
	tree := NewTree(3) // Árvore normal (não única)

	// Primeira inserção
	err := tree.Insert(types.IntKey(10), 100)
	if err != nil {
		t.Fatalf("first insert failed: %v", err)
	}

	// Segunda inserção da mesma chave deve funcionar (atualiza o valor)
	err = tree.Insert(types.IntKey(10), 200)
	if err != nil {
		t.Fatalf("second insert should succeed in non-unique index, got error: %v", err)
	}

	// Verifica que o valor foi atualizado
	node, found := tree.Search(types.IntKey(10))
	if !found {
		t.Fatal("key should exist")
	}
	if node.DataPtrs[0] != 200 {
		t.Fatalf("expected updated value 200, got %d", node.DataPtrs[0])
	}
}

func TestUniqueKey_WithVarchar(t *testing.T) {
	tree := NewUniqueTree(3)

	err := tree.Insert(types.VarcharKey("alice"), 1)
	if err != nil {
		t.Fatalf("insert alice failed: %v", err)
	}

	// Tentar inserir novamente deve falhar
	err = tree.Insert(types.VarcharKey("alice"), 2)
	if err == nil {
		t.Fatal("expected error for duplicate varchar key")
	}

	if _, ok := err.(*errors.DuplicateKeyError); !ok {
		t.Fatalf("expected DuplicateKeyError, got %T", err)
	}
}
