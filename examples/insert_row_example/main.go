package main

import (
	"fmt"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
	"os"
)

/*
EXEMPLO: InsertRow - Inserção Otimizada com Múltiplos Índices

Este example demonstra a diferença entre:
1. Put() - Insere documento duplicado para cada index
2. InsertRow() - Insere documento UMA VEZ e atualiza múltiplos indexs

Problema com Put():
- Se você tem 2 indexs (id, email), fazer Put() em cada um
  resulta em 2 cópias do documento no heap.
- Para N indexs com Put(), você terá N cópias do mesmo documento.

Solução com InsertRow():
- InsertRow() escreve o documento UMA VEZ no heap
- Atualiza TODOS os indexs para apontar para o mesmo offset
- Economiza espaço em disco e mantém consistência
- Para atualizar uma linha existente, use UpsertRow().

IMPORTANTE: InsertRow() é atômico - ou todos os indexs são atualizados
ou nenhum é (em caso de error).
*/

func main() {
	// Configuração
	walPath := "data.wal"
	heapPath := "data.heap"

	cleanup(walPath, heapPath)
	defer cleanup(walPath, heapPath)

	// ========================================
	// CENÁRIO 1: Usando Put() (forma problemática)
	// ========================================
	fmt.Println("=== Cenário 1: Usando Put() ===")

	engine1 := setupEngine("heap1.heap", "wal1.wal")

	// Inserir usuário com 2 indexs usando Put()
	doc := `{"id": 1, "email": "alice@example.com", "name": "Alice"}`

	// Put no index primário
	engine1.Put("users", "id", types.IntKey(1), doc)
	// Put no index secundário (DUPLICA o documento no heap!)
	engine1.Put("users", "email", types.VarcharKey("alice@example.com"), doc)

	size1 := getFileSize("heap1.heap")
	fmt.Printf("Tamanho do heap after 2x Put(): %d bytes\n", size1)
	fmt.Println("(Documento duplicado no heap - 2 cópias)")

	engine1.Close()

	// ========================================
	// CENÁRIO 2: Usando InsertRow() (forma otimizada)
	// ========================================
	fmt.Println("\n=== Cenário 2: Usando InsertRow() ===")

	engine2 := setupEngine("heap2.heap", "wal2.wal")

	// Inserir usuário com InsertRow() - atualiza múltiplos indexs atomicamente
	doc2 := `{"id": 2, "email": "bob@example.com", "name": "Bob"}`

	err := engine2.InsertRow("users", doc2, map[string]types.Comparable{
		"id":    types.IntKey(2),
		"email": types.VarcharKey("bob@example.com"),
	})

	if err != nil {
		fmt.Printf("Erro: %v\n", err)
	}

	size2 := getFileSize("heap2.heap")
	fmt.Printf("Tamanho do heap after InsertRow(): %d bytes\n", size2)
	fmt.Println("(Documento único no heap - 1 cópia)")

	engine2.Close()

	// ========================================
	// CENÁRIO 3: Comparação com múltiplos records
	// ========================================
	fmt.Println("\n=== Cenário 3: Comparação em escala ===")

	engine3 := setupEngine("heap3.heap", "wal3.wal")
	engine4 := setupEngine("heap4.heap", "wal4.wal")

	users := []struct {
		id    int64
		email string
		name  string
	}{
		{1, "alice@test.com", "Alice"},
		{2, "bob@test.com", "Bob"},
		{3, "charlie@test.com", "Charlie"},
		{4, "diana@test.com", "Diana"},
		{5, "eve@test.com", "Eve"},
	}

	// Engine 3: Usando Put() para cada index
	for _, u := range users {
		doc := fmt.Sprintf(`{"id": %d, "email": "%s", "name": "%s"}`, u.id, u.email, u.name)
		engine3.Put("users", "id", types.IntKey(u.id), doc)
		engine3.Put("users", "email", types.VarcharKey(u.email), doc)
	}

	// Engine 4: Usando InsertRow()
	for _, u := range users {
		doc := fmt.Sprintf(`{"id": %d, "email": "%s", "name": "%s"}`, u.id, u.email, u.name)
		engine4.InsertRow("users", doc, map[string]types.Comparable{
			"id":    types.IntKey(u.id),
			"email": types.VarcharKey(u.email),
		})
	}

	engine3.Close()
	engine4.Close()

	size3 := getFileSize("heap3.heap")
	size4 := getFileSize("heap4.heap")

	fmt.Printf("\nResultados para %d usuários com 2 indexs:\n", len(users))
	fmt.Printf("  Put() (2 chamadas por usuário): %d bytes\n", size3)
	fmt.Printf("  InsertRow() (1 chamada por usuário): %d bytes\n", size4)

	if size3 > size4 {
		economia := float64(size3-size4) / float64(size3) * 100
		fmt.Printf("  Economia: %.1f%%\n", economia)
	}

	// ========================================
	// CENÁRIO 4: Verificar que ambos os indexs funcionam
	// ========================================
	fmt.Println("\n=== Cenário 4: Verificação dos Índices ===")

	engine5 := setupEngine("heap5.heap", "wal5.wal")

	doc5 := `{"id": 100, "email": "test@example.com", "name": "Test User"}`
	engine5.InsertRow("users", doc5, map[string]types.Comparable{
		"id":    types.IntKey(100),
		"email": types.VarcharKey("test@example.com"),
	})

	// Buscar pelo index primário
	result, found, _ := engine5.Get("users", "id", types.IntKey(100))
	if found {
		fmt.Printf("Busca por ID=100: %s\n", result)
	}

	// Buscar pelo index secundário
	result, found, _ = engine5.Get("users", "email", types.VarcharKey("test@example.com"))
	if found {
		fmt.Printf("Busca por email: %s\n", result)
	}

	engine5.Close()

	// Cleanup
	cleanupAll()

	fmt.Println("\n✓ Example concluído!")
	fmt.Println("\nResumo:")
	fmt.Println("- Use Put() quando você tem apenas 1 index")
	fmt.Println("- Use InsertRow() quando você tem múltiplos indexs para o mesmo documento")
}

func setupEngine(heapPath, walPath string) *storage.StorageEngine {
	hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
		{Name: "email", Primary: false, Type: storage.TypeVarchar},
	}, 3, hm)

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	engine, _ := storage.NewStorageEngine(tableMgr, walWriter)

	return engine
}

func getFileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func cleanup(walPath, heapPath string) {
	os.Remove(walPath)
	os.Remove(heapPath)
	os.RemoveAll("checkpoints")
}

func cleanupAll() {
	for i := 1; i <= 5; i++ {
		os.Remove(fmt.Sprintf("heap%d.heap", i))
		os.Remove(fmt.Sprintf("wal%d.wal", i))
	}
	os.RemoveAll("checkpoints")
}
