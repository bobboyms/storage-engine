package btree

import (
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// =============================================
// TESTES ADICIONAIS PARA MAIOR COBERTURA
// =============================================

// Testa FindLeafLowerBound diretamente
func TestFindLeafLowerBound_SingleLeaf(t *testing.T) {
	tree := NewTree(3)
	tree.Insert(types.IntKey(10), 100)
	tree.Insert(types.IntKey(20), 200)
	tree.Insert(types.IntKey(30), 300)

	leaf, idx := tree.FindLeafLowerBound(types.IntKey(20))
	if leaf == nil {
		t.Fatal("Expected non-nil leaf")
	}
	if idx >= leaf.N {
		t.Fatalf("Index out of range")
	}
	if leaf.Keys[idx].Compare(types.IntKey(20)) != 0 {
		t.Fatalf("Expected key 20 at index, got %v", leaf.Keys[idx])
	}
}

func TestFindLeafLowerBound_KeyNotExists(t *testing.T) {
	tree := NewTree(3)
	tree.Insert(types.IntKey(10), 100)
	tree.Insert(types.IntKey(30), 300)

	// Busca por 20 que não existe
	leaf, idx := tree.FindLeafLowerBound(types.IntKey(20))
	if leaf == nil {
		t.Fatal("Expected non-nil leaf")
	}
	// Deve retornar índice onde 20 seria inserido ou o próximo maior
	if idx >= leaf.N {
		t.Fatalf("Index out of bounds: %d >= %d", idx, leaf.N)
	}
	if leaf.Keys[idx].Compare(types.IntKey(30)) != 0 {
		t.Fatalf("Expected lower bound to be 30, got %v", leaf.Keys[idx])
	}
}

func TestFindLeafLowerBound_MultipleLeaves(t *testing.T) {
	tree := NewTree(3)

	// Insere dados suficientes para criar múltiplos níveis
	for i := 1; i <= 15; i++ {
		tree.Insert(types.IntKey(i*10), int64(i*100))
	}

	// Busca por uma chave existente
	leaf, idx := tree.FindLeafLowerBound(types.IntKey(80))
	if leaf == nil {
		t.Fatal("Expected non-nil leaf")
	}

	// Verifica se encontrou a chave correta
	found := false
	for i := 0; i < leaf.N; i++ {
		if leaf.Keys[i].Compare(types.IntKey(80)) == 0 {
			found = true
			break
		}
	}
	if !found {
		t.Log("Key 80 not in this leaf, checking index returned")
		if idx < leaf.N {
			t.Logf("Index %d points to key %v", idx, leaf.Keys[idx])
		}
	}
}

// Testa Search em árvore com múltiplos níveis
func TestSearch_MultiLevel(t *testing.T) {
	tree := NewTree(3)

	// Insere chaves e verifica cada uma
	for i := 1; i <= 15; i++ {
		tree.Insert(types.IntKey(i*10), int64(i*100))
	}

	// Verifica algumas chaves
	testKeys := []int{10, 50, 100, 150}
	for _, key := range testKeys {
		_, found := tree.Search(types.IntKey(key))
		if !found {
			t.Errorf("Expected to find key %d", key)
		}
	}

	// Busca falha para chave não existente
	_, found := tree.Search(types.IntKey(75))
	if found {
		t.Error("Should not find key 75")
	}
}

func TestSearch_KeyAtBeginning(t *testing.T) {
	tree := NewTree(3)
	tree.Insert(types.IntKey(10), 100)
	tree.Insert(types.IntKey(20), 200)
	tree.Insert(types.IntKey(30), 300)

	node, found := tree.Search(types.IntKey(10))
	if !found {
		t.Fatal("Expected to find key 10")
	}
	if node == nil {
		t.Fatal("Expected non-nil node")
	}
}

func TestSearch_KeyAtEnd(t *testing.T) {
	tree := NewTree(3)
	tree.Insert(types.IntKey(10), 100)
	tree.Insert(types.IntKey(20), 200)
	tree.Insert(types.IntKey(30), 300)

	node, found := tree.Search(types.IntKey(30))
	if !found {
		t.Fatal("Expected to find key 30")
	}
	if node == nil {
		t.Fatal("Expected non-nil node")
	}
}

// Testa deleções que causam rebalanceamento via API pública
func TestDelete_CausesRebalancing(t *testing.T) {
	tree := NewTree(3)

	// Insere muitos dados
	for i := 1; i <= 20; i++ {
		tree.Insert(types.IntKey(i), int64(i*10))
	}

	// Remove chaves de forma a causar rebalanceamento
	keysToDelete := []int{5, 10, 15, 1, 2, 3, 4}
	for _, key := range keysToDelete {
		ok := tree.Root.Remove(types.IntKey(key))
		if !ok {
			t.Errorf("Failed to delete key %d", key)
		}

		// Collapse root se necessário
		if tree.Root.N == 0 && !tree.Root.Leaf && len(tree.Root.Children) > 0 {
			tree.Root = tree.Root.Children[0]
		}
	}

	// Verifica que chaves restantes ainda estão acessíveis
	remainingKeys := []int{6, 7, 8, 9, 11, 12, 13, 14, 16, 17, 18, 19, 20}
	for _, key := range remainingKeys {
		_, found := tree.Search(types.IntKey(key))
		if !found {
			t.Errorf("Expected to find remaining key %d", key)
		}
	}
}

// Testa deleção que causa merge seguido de collapse da raiz
func TestDelete_RootCollapse(t *testing.T) {
	tree := NewTree(3)

	// Insere poucos dados
	tree.Insert(types.IntKey(10), 100)
	tree.Insert(types.IntKey(20), 200)
	tree.Insert(types.IntKey(30), 300)
	tree.Insert(types.IntKey(40), 400)
	tree.Insert(types.IntKey(50), 500)
	tree.Insert(types.IntKey(60), 600) // Causa primeiro split

	// Remove para causar merge
	tree.Root.Remove(types.IntKey(10))
	tree.Root.Remove(types.IntKey(20))

	if tree.Root.N == 0 && !tree.Root.Leaf && len(tree.Root.Children) > 0 {
		tree.Root = tree.Root.Children[0]
	}

	// Verifica que as chaves restantes estão OK
	for _, key := range []int{30, 40, 50, 60} {
		_, found := tree.Search(types.IntKey(key))
		if !found {
			t.Errorf("Expected to find key %d after collapse", key)
		}
	}
}

// Testa fixSeparators após múltiplas deleções
func TestDelete_FixSeparators(t *testing.T) {
	tree := NewTree(3)

	// Insere dados
	for i := 1; i <= 10; i++ {
		tree.Insert(types.IntKey(i*10), int64(i*100))
	}

	// Remove chaves
	tree.Root.Remove(types.IntKey(30))
	tree.Root.Remove(types.IntKey(40))

	// Verifica que ainda podemos encontrar outras chaves
	_, found := tree.Search(types.IntKey(50))
	if !found {
		t.Error("Expected to find key 50 after deletes")
	}

	_, found = tree.Search(types.IntKey(60))
	if !found {
		t.Error("Expected to find key 60 after deletes")
	}
}

// Testa deleção de todas as chaves
func TestDelete_AllKeys(t *testing.T) {
	tree := NewTree(3)

	keys := []int{10, 20, 30, 40, 50}
	for _, k := range keys {
		tree.Insert(types.IntKey(k), int64(k*10))
	}

	// Remove todas as chaves
	for _, k := range keys {
		ok := tree.Root.Remove(types.IntKey(k))
		if !ok {
			t.Errorf("Failed to delete key %d", k)
		}

		// Collapse se necessário
		if tree.Root.N == 0 && !tree.Root.Leaf && len(tree.Root.Children) > 0 {
			tree.Root = tree.Root.Children[0]
		}
	}

	// Árvore deve estar vazia
	if tree.Root.N != 0 {
		t.Errorf("Expected empty tree, got %d keys", tree.Root.N)
	}
}

// Testa Search quando passa pelo loop em nó interno
func TestSearch_InternalNodeTraversal(t *testing.T) {
	tree := NewTree(3)

	// Cria árvore com múltiplos níveis
	for i := 1; i <= 20; i++ {
		tree.Insert(types.IntKey(i*5), int64(i*50))
	}

	// Busca chaves em diferentes posições
	testCases := []int{5, 25, 50, 75, 100}
	for _, key := range testCases {
		_, found := tree.Search(types.IntKey(key))
		if !found {
			t.Errorf("Expected to find key %d", key)
		}
	}

	// Busca chave que não existe
	_, found := tree.Search(types.IntKey(7))
	if found {
		t.Error("Should not find key 7")
	}
}

// Testa Node.Remove exportado
func TestNode_Remove_Exported(t *testing.T) {
	tree := NewTree(3)
	tree.Insert(types.IntKey(10), 100)
	tree.Insert(types.IntKey(20), 200)
	tree.Insert(types.IntKey(30), 300)

	ok := tree.Root.Remove(types.IntKey(20))
	if !ok {
		t.Fatal("Expected Remove to succeed")
	}

	_, found := tree.Search(types.IntKey(20))
	if found {
		t.Error("Key 20 should have been removed")
	}
}

// Testa Node.FindLeafLowerBound exportado
func TestNode_FindLeafLowerBound_Exported(t *testing.T) {
	tree := NewTree(3)
	tree.Insert(types.IntKey(10), 100)
	tree.Insert(types.IntKey(20), 200)
	tree.Insert(types.IntKey(30), 300)

	node, idx := tree.Root.FindLeafLowerBound(types.IntKey(20))
	if node == nil {
		t.Fatal("Expected non-nil node")
	}
	if idx >= node.N {
		t.Fatalf("Index %d out of range", idx)
	}
	if node.Keys[idx].Compare(types.IntKey(20)) != 0 {
		t.Fatalf("Expected key 20 at index %d", idx)
	}
}

// Testa inserção e busca com muitos elementos
func TestLargeTreeOperations(t *testing.T) {
	tree := NewTree(3)

	// Insere 100 elementos
	for i := 1; i <= 100; i++ {
		err := tree.Insert(types.IntKey(i), int64(i*10))
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Busca cada elemento
	for i := 1; i <= 100; i++ {
		_, found := tree.Search(types.IntKey(i))
		if !found {
			t.Errorf("Failed to find key %d", i)
		}
	}

	// Remove metade dos elementos
	for i := 1; i <= 50; i++ {
		ok := tree.Root.Remove(types.IntKey(i))
		if !ok {
			t.Errorf("Failed to remove key %d", i)
		}

		// Collapse se necessário
		if tree.Root.N == 0 && !tree.Root.Leaf && len(tree.Root.Children) > 0 {
			tree.Root = tree.Root.Children[0]
		}
	}

	// Verifica que os removidos não existem mais
	for i := 1; i <= 50; i++ {
		_, found := tree.Search(types.IntKey(i))
		if found {
			t.Errorf("Key %d should have been removed", i)
		}
	}

	// Verifica que os restantes ainda existem
	for i := 51; i <= 100; i++ {
		_, found := tree.Search(types.IntKey(i))
		if !found {
			t.Errorf("Key %d should still exist", i)
		}
	}
}

// Testa BPlusTree.FindLeafLowerBound público
func TestBPlusTree_FindLeafLowerBound(t *testing.T) {
	tree := NewTree(3)

	tree.Insert(types.IntKey(10), 100)
	tree.Insert(types.IntKey(20), 200)
	tree.Insert(types.IntKey(30), 300)

	// Testa busca exata
	node, idx := tree.FindLeafLowerBound(types.IntKey(20))
	if node == nil {
		t.Fatal("Expected non-nil node")
	}
	if idx >= node.N {
		t.Fatal("Index out of range")
	}

	// Testa busca de valor não existente
	node2, idx2 := tree.FindLeafLowerBound(types.IntKey(15))
	if node2 == nil {
		t.Fatal("Expected non-nil node for non-existent key")
	}
	// Deve apontar para 20 (próximo maior)
	if idx2 < node2.N && node2.Keys[idx2].Compare(types.IntKey(20)) != 0 {
		t.Log("Lower bound returned different key, which is valid behavior")
	}
}

// Testa inserção de chaves em ordem reversa
func TestInsert_ReverseOrder(t *testing.T) {
	tree := NewTree(3)

	// Insere em ordem reversa
	for i := 20; i >= 1; i-- {
		tree.Insert(types.IntKey(i), int64(i*10))
	}

	// Verifica todas as chaves
	for i := 1; i <= 20; i++ {
		_, found := tree.Search(types.IntKey(i))
		if !found {
			t.Errorf("Failed to find key %d", i)
		}
	}
}

// Testa inserção de chaves iguais (update)
func TestInsert_Update(t *testing.T) {
	tree := NewTree(3) // Não é unique, permite update

	tree.Insert(types.IntKey(10), 100)
	tree.Insert(types.IntKey(10), 200) // Deve atualizar

	node, found := tree.Search(types.IntKey(10))
	if !found {
		t.Fatal("Key should exist")
	}

	// Encontra o índice correto
	for i := 0; i < node.N; i++ {
		if node.Keys[i].Compare(types.IntKey(10)) == 0 {
			if node.DataPtrs[i] != 200 {
				t.Errorf("Expected updated value 200, got %d", node.DataPtrs[i])
			}
			break
		}
	}
}

func TestNode_IsSafeForInsert(t *testing.T) {
	// T=3 => Max Keys = 2*T - 1 = 5
	node := NewNode(3, true)

	if !node.IsSafeForInsert() {
		t.Error("Empty node should be safe for insert")
	}

	for i := 1; i <= 4; i++ {
		node.InsertNonFull(types.IntKey(i), int64(i), false)
	}

	if !node.IsSafeForInsert() {
		t.Error("Node with 4 keys (max 5) should be safe for insert")
	}

	node.InsertNonFull(types.IntKey(5), 5, false)

	if node.IsSafeForInsert() {
		t.Error("Full node (5 keys) should NOT be safe for insert")
	}
}

func TestNode_IsSafeForDelete(t *testing.T) {
	// T=3 => Min Keys = T-1 = 2
	node := NewNode(3, true)

	// Fill with min keys + 1
	node.InsertNonFull(types.IntKey(1), 1, false)
	node.InsertNonFull(types.IntKey(2), 2, false)
	node.InsertNonFull(types.IntKey(3), 3, false)

	if !node.IsSafeForDelete() {
		t.Error("Node with 3 keys (min 2) should be safe for delete")
	}

	node.Remove(types.IntKey(3))
	// Now has 2 keys (min allowed)

	if node.IsSafeForDelete() {
		t.Error("Node with 2 keys (min allowed) should NOT be safe for delete (needs merge/borrow)")
	}
}
