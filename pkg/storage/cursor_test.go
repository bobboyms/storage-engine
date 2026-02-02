package storage_test

import (
	"testing"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// =============================================
// TESTES PARA CURSOR
// =============================================

func TestCursor_SeekEmpty(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)

	se := storage.NewStorageEngine(tableMgr)
	index, _ := se.TableMetaData.GetIndexByName("test", "id")
	cursor := se.Cursor(index.Tree)

	// Seek em árvore vazia
	cursor.Seek(types.IntKey(10))
	if cursor.Valid() {
		t.Error("Expected cursor to be invalid on empty tree")
	}
}

func TestCursor_SeekExactMatch(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)

	se := storage.NewStorageEngine(tableMgr)
	se.Put("test", "id", types.IntKey(10), 100)
	se.Put("test", "id", types.IntKey(20), 200)
	se.Put("test", "id", types.IntKey(30), 300)

	index, _ := se.TableMetaData.GetIndexByName("test", "id")
	cursor := se.Cursor(index.Tree)

	cursor.Seek(types.IntKey(20))
	if !cursor.Valid() {
		t.Fatal("Expected cursor to be valid")
	}
	if cursor.Key().Compare(types.IntKey(20)) != 0 {
		t.Fatalf("Expected key 20, got %v", cursor.Key())
	}
	if cursor.Value() != 200 {
		t.Fatalf("Expected value 200, got %d", cursor.Value())
	}
}

func TestCursor_SeekLowerBound(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)

	se := storage.NewStorageEngine(tableMgr)
	se.Put("test", "id", types.IntKey(10), 100)
	se.Put("test", "id", types.IntKey(20), 200)
	se.Put("test", "id", types.IntKey(30), 300)

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
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)

	se := storage.NewStorageEngine(tableMgr)
	se.Put("test", "id", types.IntKey(10), 100)
	se.Put("test", "id", types.IntKey(20), 200)

	index, _ := se.TableMetaData.GetIndexByName("test", "id")
	cursor := se.Cursor(index.Tree)

	// Seek para valor além do último
	cursor.Seek(types.IntKey(100))
	if cursor.Valid() {
		t.Error("Expected cursor to be invalid when seeking beyond end")
	}
}

func TestCursor_NextIteration(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)

	se := storage.NewStorageEngine(tableMgr)
	for i := 1; i <= 5; i++ {
		se.Put("test", "id", types.IntKey(i*10), i*100)
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
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)

	se := storage.NewStorageEngine(tableMgr)
	index, _ := se.TableMetaData.GetIndexByName("test", "id")
	cursor := se.Cursor(index.Tree)

	// Next em cursor não inicializado/inválido
	result := cursor.Next()
	if result {
		t.Error("Expected Next() to return false on invalid cursor")
	}
}

func TestCursor_NextAcrossLeaves(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3) // T=3 significa max 5 keys por folha

	se := storage.NewStorageEngine(tableMgr)

	// Insere dados suficientes para criar múltiplas folhas
	for i := 1; i <= 20; i++ {
		se.Put("test", "id", types.IntKey(i), i*10)
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
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)

	se := storage.NewStorageEngine(tableMgr)
	se.Put("test", "id", types.IntKey(10), 100)
	se.Put("test", "id", types.IntKey(20), 200)
	se.Put("test", "id", types.IntKey(30), 300)

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
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("test", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)

	se := storage.NewStorageEngine(tableMgr)
	se.Put("test", "id", types.IntKey(10), 100)

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
	tableMgr := storage.NewTableMenager()
	se := storage.NewStorageEngine(tableMgr)

	err := se.Put("nonexistent", "id", types.IntKey(1), 100)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

func TestEngine_PutIndexNotFound(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)
	se := storage.NewStorageEngine(tableMgr)

	err := se.Put("users", "nonexistent_index", types.IntKey(1), 100)
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

func TestEngine_GetTableNotFound(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	se := storage.NewStorageEngine(tableMgr)

	_, _, err := se.Get("nonexistent", "id", types.IntKey(1))
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

func TestEngine_GetIndexNotFound(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)
	se := storage.NewStorageEngine(tableMgr)

	_, _, err := se.Get("users", "nonexistent_index", types.IntKey(1))
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

func TestEngine_DelTableNotFound(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	se := storage.NewStorageEngine(tableMgr)

	_, err := se.Del("nonexistent", "id", types.IntKey(1))
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

func TestEngine_DelIndexNotFound(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)
	se := storage.NewStorageEngine(tableMgr)

	_, err := se.Del("users", "nonexistent_index", types.IntKey(1))
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

func TestEngine_ScanTableNotFound(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	se := storage.NewStorageEngine(tableMgr)

	_, err := se.RangeScan("nonexistent", "id", types.IntKey(1), types.IntKey(10))
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

func TestEngine_ScanIndexNotFound(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)
	se := storage.NewStorageEngine(tableMgr)

	_, err := se.RangeScan("users", "nonexistent_index", types.IntKey(1), types.IntKey(10))
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}
