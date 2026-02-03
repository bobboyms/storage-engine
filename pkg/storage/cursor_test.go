package storage_test

import (
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/btree"
	"github.com/bobboyms/storage-engine/pkg/heap"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// =============================================
// TESTES PARA CURSOR
// =============================================

func TestCursor_SeekEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm)

	se, _ := storage.NewStorageEngine(tableMgr, nil)
	index, _ := se.TableMetaData.GetIndexByName("test", "id")
	cursor := se.Cursor(index.Tree)

	// Seek em árvore vazia
	cursor.Seek(types.IntKey(10))
	if cursor.Valid() {
		t.Error("Expected cursor to be invalid on empty tree")
	}
}

func TestCursor_SeekExactMatch(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm)

	se, _ := storage.NewStorageEngine(tableMgr, nil)
	se.Put("test", "id", types.IntKey(10), "val_100")
	se.Put("test", "id", types.IntKey(20), "val_200")
	se.Put("test", "id", types.IntKey(30), "val_300")

	index, _ := se.TableMetaData.GetIndexByName("test", "id")
	cursor := se.Cursor(index.Tree)

	cursor.Seek(types.IntKey(20))
	if !cursor.Valid() {
		t.Fatal("Expected cursor to be valid")
	}
	if cursor.Key().Compare(types.IntKey(20)) != 0 {
		t.Fatalf("Expected key 20, got %v", cursor.Key())
	}

	// Value now returns offset. We verify by reading from heap.
	// Access heap via table, not se.Heap
	table, _ := se.TableMetaData.GetTableByName("test")
	offset := cursor.Value()
	docBytes, _, err := table.Heap.Read(offset)
	if err != nil {
		t.Fatalf("Failed to read from heap at offset %d: %v", offset, err)
	}
	if string(docBytes) != "val_200" {
		t.Fatalf("Expected value 'val_200', got %q", string(docBytes))
	}
}

func TestCursor_SeekLowerBound(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm)

	se, _ := storage.NewStorageEngine(tableMgr, nil)
	se.Put("test", "id", types.IntKey(10), "val_100")
	se.Put("test", "id", types.IntKey(20), "val_200")
	se.Put("test", "id", types.IntKey(30), "val_300")

	index, _ := se.TableMetaData.GetIndexByName("test", "id")
	cursor := se.Cursor(index.Tree)

	// Seek para valor que não existe (15) - deve posicionar em 20
	cursor.Seek(types.IntKey(15))
	if !cursor.Valid() {
		t.Fatal("Expected cursor to be valid")
	}
	if cursor.Key().Compare(types.IntKey(20)) != 0 {
		t.Fatalf("Expected key 20 (lower bound), got %v", cursor.Key())
	}
}

func TestCursor_SeekBeyondEnd(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm)

	se, _ := storage.NewStorageEngine(tableMgr, nil)
	se.Put("test", "id", types.IntKey(10), "val_100")
	se.Put("test", "id", types.IntKey(20), "val_200")

	index, _ := se.TableMetaData.GetIndexByName("test", "id")
	cursor := se.Cursor(index.Tree)

	// Seek para valor além do último
	cursor.Seek(types.IntKey(100))
	if cursor.Valid() {
		t.Error("Expected cursor to be invalid when seeking beyond end")
	}
}

func TestCursor_NextIteration(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm)

	se, _ := storage.NewStorageEngine(tableMgr, nil)
	for i := 1; i <= 5; i++ {
		se.Put("test", "id", types.IntKey(i*10), "val")
	}

	index, _ := se.TableMetaData.GetIndexByName("test", "id")
	cursor := se.Cursor(index.Tree)

	cursor.Seek(types.IntKey(10))

	count := 0
	expectedKeys := []int{10, 20, 30, 40, 50}
	for cursor.Valid() {
		if count >= len(expectedKeys) {
			t.Fatalf("Too many iterations: expected %d", len(expectedKeys))
		}
		if cursor.Key().Compare(types.IntKey(expectedKeys[count])) != 0 {
			t.Fatalf("Expected key %d at position %d, got %v", expectedKeys[count], count, cursor.Key())
		}
		count++
		cursor.Next()
	}

	if count != len(expectedKeys) {
		t.Fatalf("Expected %d iterations, got %d", len(expectedKeys), count)
	}
}

func TestCursor_NextOnInvalid(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm)

	se, _ := storage.NewStorageEngine(tableMgr, nil)
	index, _ := se.TableMetaData.GetIndexByName("test", "id")
	cursor := se.Cursor(index.Tree)

	// Next em cursor não inicializado/inválido
	result := cursor.Next()
	if result {
		t.Error("Expected Next() to return false on invalid cursor")
	}
}

func TestCursor_NextAcrossLeaves(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm) // T=3 significa max 5 keys por folha

	se, _ := storage.NewStorageEngine(tableMgr, nil)

	// Insere dados suficientes para criar múltiplas folhas
	for i := 1; i <= 20; i++ {
		se.Put("test", "id", types.IntKey(i), "val")
	}

	index, _ := se.TableMetaData.GetIndexByName("test", "id")
	cursor := se.Cursor(index.Tree)

	cursor.Seek(types.IntKey(1))

	count := 0
	for cursor.Valid() {
		count++
		cursor.Next()
	}

	if count != 20 {
		t.Fatalf("Expected 20 items, got %d", count)
	}
}

func TestCursor_SeekToFirstKey(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm)

	se, _ := storage.NewStorageEngine(tableMgr, nil)
	se.Put("test", "id", types.IntKey(10), "val_100")
	se.Put("test", "id", types.IntKey(20), "val_200")
	se.Put("test", "id", types.IntKey(30), "val_300")

	index, _ := se.TableMetaData.GetIndexByName("test", "id")
	cursor := se.Cursor(index.Tree)

	// Seek para valor menor que o primeiro
	cursor.Seek(types.IntKey(5))
	if !cursor.Valid() {
		t.Fatal("Expected cursor to be valid")
	}
	if cursor.Key().Compare(types.IntKey(10)) != 0 {
		t.Fatalf("Expected key 10 (first key), got %v", cursor.Key())
	}
}

func TestCursor_ValidAfterExhaustion(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm)

	se, _ := storage.NewStorageEngine(tableMgr, nil)
	se.Put("test", "id", types.IntKey(10), "val_100")

	index, _ := se.TableMetaData.GetIndexByName("test", "id")
	cursor := se.Cursor(index.Tree)

	cursor.Seek(types.IntKey(10))
	if !cursor.Valid() {
		t.Fatal("Expected cursor to be valid initially")
	}

	// Avança além do último elemento
	cursor.Next()
	if cursor.Valid() {
		t.Error("Expected cursor to be invalid after exhaustion")
	}

	// Next em cursor exaurido
	result := cursor.Next()
	if result {
		t.Error("Expected Next() to return false on exhausted cursor")
	}
}

// =============================================
// TESTES PARA CENÁRIOS DE ERRO DO ENGINE
// =============================================

func TestEngine_PutTableNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	filepath.Join(tmpDir, "heap.data") // unused path?
	// hm unused

	tableMgr := storage.NewTableMenager()
	se, _ := storage.NewStorageEngine(tableMgr, nil)

	err := se.Put("nonexistent", "id", types.IntKey(1), "val")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

func TestEngine_PutIndexNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm)

	se, _ := storage.NewStorageEngine(tableMgr, nil)

	err := se.Put("users", "nonexistent_index", types.IntKey(1), "val")
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

func TestEngine_GetTableNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	filepath.Join(tmpDir, "heap.data") // unused

	tableMgr := storage.NewTableMenager()
	se, _ := storage.NewStorageEngine(tableMgr, nil)

	_, _, err := se.Get("nonexistent", "id", types.IntKey(1))
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

func TestEngine_GetIndexNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm)

	se, _ := storage.NewStorageEngine(tableMgr, nil)

	_, _, err := se.Get("users", "nonexistent_index", types.IntKey(1))
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

func TestEngine_DelTableNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	filepath.Join(tmpDir, "heap.data")

	tableMgr := storage.NewTableMenager()
	se, _ := storage.NewStorageEngine(tableMgr, nil)

	_, err := se.Del("nonexistent", "id", types.IntKey(1))
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

func TestEngine_DelIndexNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm)

	se, _ := storage.NewStorageEngine(tableMgr, nil)

	_, err := se.Del("users", "nonexistent_index", types.IntKey(1))
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

func TestEngine_ScanTableNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	filepath.Join(tmpDir, "heap.data")

	tableMgr := storage.NewTableMenager()
	se, _ := storage.NewStorageEngine(tableMgr, nil)

	_, err := se.RangeScan("nonexistent", "id", types.IntKey(1), types.IntKey(10))
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

func TestEngine_ScanIndexNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm)

	se, _ := storage.NewStorageEngine(tableMgr, nil)

	_, err := se.RangeScan("users", "nonexistent_index", types.IntKey(1), types.IntKey(10))
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

func TestCursor_SeekNil(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm)

	se, _ := storage.NewStorageEngine(tableMgr, nil)
	se.Put("test", "id", types.IntKey(10), "val_100")
	se.Put("test", "id", types.IntKey(20), "val_200")

	index, _ := se.TableMetaData.GetIndexByName("test", "id")
	cursor := se.Cursor(index.Tree)

	// Seek nil (start from beginning)
	cursor.Seek(nil)
	if !cursor.Valid() {
		t.Fatal("Expected cursor to be valid")
	}
	if cursor.Key().Compare(types.IntKey(10)) != 0 {
		t.Fatalf("Expected first key 10, got %v", cursor.Key())
	}
}

func TestCursor_SeekEndOfLeaf(t *testing.T) {
	// Força Seek para o exato final de uma folha (idx >= leaf.N)
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 2, hm) // T=2, max=3 keys por folha

	se, _ := storage.NewStorageEngine(tableMgr, nil)

	// Folha 1: 10, 20, 30
	// Split ocorre ao inserir 40
	se.Put("test", "id", types.IntKey(10), "")
	se.Put("test", "id", types.IntKey(20), "")
	se.Put("test", "id", types.IntKey(30), "")
	se.Put("test", "id", types.IntKey(40), "")
	se.Put("test", "id", types.IntKey(50), "")

	index, _ := se.TableMetaData.GetIndexByName("test", "id")
	cursor := se.Cursor(index.Tree)

	// Chave maior que todas na primeira folha mas inexistente
	// FindLeafLowerBound pode retornar a folha 1 com idx=3 (N=3)
	// Isso deve forçar a navegação para a folha 2
	cursor.Seek(types.IntKey(35))
	if !cursor.Valid() {
		t.Fatal("Expected cursor to be valid")
	}
	if cursor.Key().Compare(types.IntKey(40)) != 0 {
		t.Fatalf("Expected key 40, got %v", cursor.Key())
	}
}

func TestCursor_NextEmptyNodes(t *testing.T) {
	// Difícil de criar com Put/Del normal, mas vamos garantir o branch Coverage
	// de navegar através de múltiplas folhas.
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 2, hm)

	se, _ := storage.NewStorageEngine(tableMgr, nil)

	for i := 1; i <= 10; i++ {
		se.Put("test", "id", types.IntKey(i), "")
	}

	index, _ := se.TableMetaData.GetIndexByName("test", "id")
	cursor := se.Cursor(index.Tree)
	cursor.Seek(types.IntKey(1))

	for i := 1; i <= 10; i++ {
		if !cursor.Valid() {
			t.Fatalf("Expected valid cursor at index %d", i)
		}
		cursor.Next()
	}

	if cursor.Valid() {
		t.Error("Expected cursor to be invalid after end")
	}
}

func TestCursor_ComplexNavigation(t *testing.T) {
	// Este teste usa manipulação manual dos nós para atingir branches difíceis de Seek e Next
	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 2, hm)

	se, _ := storage.NewStorageEngine(tableMgr, nil)

	se.Put("test", "id", types.IntKey(10), "")
	se.Put("test", "id", types.IntKey(20), "")

	index, _ := se.TableMetaData.GetIndexByName("test", "id")
	tree := index.Tree

	// Agora temos um Root que é Leaf. Vamos forçar um estado onde ele aponta para outro mas está "vazio"
	// Na verdade, vamos criar duas folhas e linká-las.
	leaf1 := tree.Root
	leaf2 := btree.NewNode(2, true)
	leaf2.Keys = append(leaf2.Keys, types.IntKey(30))
	leaf2.DataPtrs = append(leaf2.DataPtrs, 0)
	leaf2.N = 1

	leaf1.Next = leaf2

	// Cenário 1: Seek em nó vazio que tem Next
	// Backup original N
	oldN := leaf1.N
	leaf1.N = 0

	cursor := se.Cursor(tree)
	cursor.Seek(types.IntKey(5))

	if !cursor.Valid() {
		t.Error("Expected cursor to be valid by navigating to nextLeaf")
	} else if cursor.Key().Compare(types.IntKey(30)) != 0 {
		t.Errorf("Expected to find 30 in next leaf, got %v", cursor.Key())
	}

	// Cenário 2: Next através de nó vazio
	leaf1.N = oldN
	cursor.Seek(types.IntKey(10))

	// Insere DOIS nós vazios no meio
	midEmpty1 := btree.NewNode(2, true)
	midEmpty1.N = 0
	midEmpty2 := btree.NewNode(2, true)
	midEmpty2.N = 0

	midEmpty1.Next = midEmpty2
	midEmpty2.Next = leaf2
	leaf1.Next = midEmpty1

	cursor.Next() // de 10 para 20
	cursor.Next() // de 20 para 30 (pulando DOIS vazios)

	if !cursor.Valid() || cursor.Key().Compare(types.IntKey(30)) != 0 {
		t.Errorf("Expected 30 after skipping two empty nodes, got valid=%v", cursor.Valid())
	}

	// Cenário 3: Seek com múltiplos vazios
	leaf1.N = 0
	midEmpty1.N = 0
	midEmpty2.N = 0
	cursor.Seek(types.IntKey(5))
	if !cursor.Valid() || cursor.Key().Compare(types.IntKey(30)) != 0 {
		t.Errorf("Seek failed through multiple empty nodes")
	}
}
