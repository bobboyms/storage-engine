package main

import (
	"fmt"
	"os"

	"github.com/bobboyms/storage-engine/pkg/heap"
	"github.com/bobboyms/storage-engine/pkg/query"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

/*
EXEMPLO: Paginação

Este exemplo demonstra 3 estratégias de paginação:

1. OFFSET-BASED (tradicional):
   - SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 20
   - Simples, mas ineficiente para páginas grandes (O(n))

2. CURSOR-BASED (keyset pagination):
   - SELECT * FROM users WHERE id > last_id ORDER BY id LIMIT 10
   - Eficiente (O(log n)), mas não permite pular páginas

3. SCAN + SKIP (híbrido):
   - Usa Cursor.Seek() + Next() para pular registros
   - Bom compromisso entre flexibilidade e performance
*/

func main() {
	walPath := "data.wal"
	heapPath := "data.heap"

	cleanup(walPath, heapPath)
	defer cleanup(walPath, heapPath)

	engine, hm := setupEngine(heapPath, walPath)
	defer engine.Close()

	// ========================================
	// PREPARAÇÃO: Inserir 100 produtos
	// ========================================
	fmt.Println("=== Inserindo 100 produtos ===")
	for i := 1; i <= 100; i++ {
		doc := fmt.Sprintf(`{"id": %d, "name": "Product %d", "price": %.2f}`, i, i, float64(i)*10.5)
		engine.Put("products", "id", types.IntKey(int64(i)), doc)
	}
	fmt.Println("✓ 100 produtos inseridos\n")

	// ========================================
	// ESTRATÉGIA 1: OFFSET-BASED PAGINATION
	// ========================================
	fmt.Println("=== Estratégia 1: Offset-Based (tradicional) ===")
	fmt.Println("SQL equivalente: SELECT * FROM products LIMIT 10 OFFSET 20")

	pageSize := 10
	offset := 20 // Página 3 (0-9, 10-19, 20-29)

	page1 := offsetBasedPagination(engine, hm, "products", "id", pageSize, offset)
	fmt.Printf("\nPágina 3 (offset=%d, limit=%d):\n", offset, pageSize)
	for _, doc := range page1 {
		fmt.Printf("  %s\n", doc)
	}

	fmt.Println("\n⚠️  Problema: Para offset=90, precisa iterar 90 registros!")
	fmt.Println("   Complexidade: O(offset + limit)")

	// ========================================
	// ESTRATÉGIA 2: CURSOR-BASED (Keyset)
	// ========================================
	fmt.Println("\n=== Estratégia 2: Cursor-Based (Keyset Pagination) ===")
	fmt.Println("SQL equivalente: SELECT * FROM products WHERE id > 20 LIMIT 10")

	lastSeenID := int64(20) // Último ID da página anterior
	page2 := cursorBasedPagination(engine, "products", "id", lastSeenID, pageSize)

	fmt.Printf("\nPróxima página após ID=%d (limit=%d):\n", lastSeenID, pageSize)
	for _, doc := range page2 {
		fmt.Printf("  %s\n", doc)
	}

	fmt.Println("\n✓ Vantagem: Sempre O(log n) - eficiente para qualquer página")
	fmt.Println("✗ Desvantagem: Não pode pular para página específica (ex: página 5)")

	// ========================================
	// ESTRATÉGIA 3: SCAN + SKIP (Híbrido)
	// ========================================
	fmt.Println("\n=== Estratégia 3: Scan + Skip (Híbrido) ===")

	pageNumber := 5 // Ir direto para página 5
	page3 := scanAndSkipPagination(engine, hm, "products", "id", pageNumber, pageSize)

	fmt.Printf("\nPágina %d (limit=%d):\n", pageNumber, pageSize)
	for _, doc := range page3 {
		fmt.Printf("  %s\n", doc)
	}

	// ========================================
	// COMPARAÇÃO DE PERFORMANCE
	// ========================================
	fmt.Println("\n=== Comparação de Performance ===")
	fmt.Println(`
┌─────────────────┬──────────────┬─────────────────┬──────────────────┐
│ Estratégia      │ Complexidade │ Pular páginas?  │ Uso recomendado  │
├─────────────────┼──────────────┼─────────────────┼──────────────────┤
│ Offset-Based    │ O(offset+n)  │ ✓ Sim           │ Datasets pequenos│
│                 │              │                 │ (< 10k registros)│
├─────────────────┼──────────────┼─────────────────┼──────────────────┤
│ Cursor-Based    │ O(log n)     │ ✗ Não           │ Feeds infinitos, │
│ (Keyset)        │              │                 │ APIs de streaming│
├─────────────────┼──────────────┼─────────────────┼──────────────────┤
│ Scan + Skip     │ O(skip + n)  │ ✓ Sim (limitado)│ Datasets médios  │
│                 │              │                 │ (10k-100k)       │
└─────────────────┴──────────────┴─────────────────┴──────────────────┘
`)

	// ========================================
	// EXEMPLO PRÁTICO: API REST
	// ========================================
	fmt.Println("=== Exemplo: API REST ===")
	fmt.Println(`
// Endpoint 1: Paginação tradicional (offset-based)
GET /api/products?page=3&limit=10
→ Usa offsetBasedPagination(engine, hm, "products", "id", 10, 20)

// Endpoint 2: Infinite scroll (cursor-based)
GET /api/products?after=20&limit=10
→ Usa cursorBasedPagination(engine, "products", "id", 20, 10)

// Endpoint 3: Híbrido
GET /api/products?page=5&limit=10
→ Usa scanAndSkipPagination(engine, hm, "products", "id", 5, 10)
`)

	// ========================================
	// PAGINAÇÃO COM FILTROS
	// ========================================
	fmt.Println("=== Paginação com Filtros ===")

	// Exemplo: produtos com preço > 500
	fmt.Println("\nProdutos com preço > 500 (primeira página):")
	filteredPage := paginationWithFilter(engine, "products", "price",
		query.GreaterThan(types.FloatKey(500.0)), 5)

	for _, doc := range filteredPage {
		fmt.Printf("  %s\n", doc)
	}

	fmt.Println("\n✓ Exemplo concluído!")
}

// ========================================
// IMPLEMENTAÇÕES
// ========================================

// offsetBasedPagination - Tradicional, simples mas ineficiente
func offsetBasedPagination(engine *storage.StorageEngine, hm *heap.HeapManager,
	tableName, indexName string, limit, offset int) []string {

	index, _ := engine.TableMetaData.GetIndexByName(tableName, indexName)
	cursor := engine.Cursor(index.Tree)
	defer cursor.Close()

	// Seek para o início
	cursor.Seek(types.IntKey(0))

	// Pular 'offset' registros
	for i := 0; i < offset && cursor.Valid(); i++ {
		cursor.Next()
	}

	// Coletar 'limit' registros
	results := make([]string, 0, limit)
	for i := 0; i < limit && cursor.Valid(); i++ {
		offset := cursor.Value()
		doc, _, _ := hm.Read(offset)
		results = append(results, string(doc))
		cursor.Next()
	}

	return results
}

// cursorBasedPagination - Keyset, eficiente mas sem pular páginas
func cursorBasedPagination(engine *storage.StorageEngine, tableName, indexName string,
	lastSeenKey int64, limit int) []string {

	// Usar Scan com condição > lastSeenKey
	condition := query.GreaterThan(types.IntKey(lastSeenKey))
	results, _ := engine.Scan(tableName, indexName, condition)

	// Limitar resultados
	if len(results) > limit {
		results = results[:limit]
	}

	return results
}

// scanAndSkipPagination - Híbrido, usa Seek + Skip
func scanAndSkipPagination(engine *storage.StorageEngine, hm *heap.HeapManager,
	tableName, indexName string, pageNumber, pageSize int) []string {

	index, _ := engine.TableMetaData.GetIndexByName(tableName, indexName)
	cursor := engine.Cursor(index.Tree)
	defer cursor.Close()

	// Calcular offset
	offset := (pageNumber - 1) * pageSize

	// Seek para o início
	cursor.Seek(types.IntKey(0))

	// Pular registros
	for i := 0; i < offset && cursor.Valid(); i++ {
		cursor.Next()
	}

	// Coletar página
	results := make([]string, 0, pageSize)
	for i := 0; i < pageSize && cursor.Valid(); i++ {
		offset := cursor.Value()
		doc, _, _ := hm.Read(offset)
		results = append(results, string(doc))
		cursor.Next()
	}

	return results
}

// paginationWithFilter - Paginação com condição de filtro
func paginationWithFilter(engine *storage.StorageEngine, tableName, indexName string,
	condition *query.ScanCondition, limit int) []string {

	results, _ := engine.Scan(tableName, indexName, condition)

	// Limitar resultados
	if len(results) > limit {
		results = results[:limit]
	}

	return results
}

// ========================================
// SETUP
// ========================================

func setupEngine(heapPath, walPath string) (*storage.StorageEngine, *heap.HeapManager) {
	hm, _ := heap.NewHeapManager(heapPath)

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("products", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
		{Name: "price", Primary: false, Type: storage.TypeFloat},
	}, 3, hm)

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	engine, _ := storage.NewStorageEngine(tableMgr, walWriter)

	return engine, hm
}

func cleanup(walPath, heapPath string) {
	os.Remove(walPath)
	os.Remove(heapPath)
	os.RemoveAll("checkpoints")
}
