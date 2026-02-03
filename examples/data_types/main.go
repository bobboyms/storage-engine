package main

import (
	"fmt"
	"os"
	"time"

	"github.com/bobboyms/storage-engine/pkg/heap"
	"github.com/bobboyms/storage-engine/pkg/query"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

/*
EXEMPLO: Tipos de Dados Suportados

Este exemplo demonstra todos os tipos de dados suportados pelo Storage Engine:
1. TypeInt     - Inteiros (int64)
2. TypeVarchar - Strings
3. TypeFloat   - Números de ponto flutuante (float64)
4. TypeBoolean - Valores booleanos
5. TypeDate    - Datas/Timestamps

Cada tipo tem sua própria chave comparável:
- types.IntKey
- types.VarcharKey
- types.FloatKey
- types.BoolKey
- types.DateKey
*/

func main() {
	// Configuração
	walPath := "data.wal"
	heapPath := "data.heap"

	cleanup(walPath, heapPath)
	defer cleanup(walPath, heapPath)

	engine := setupEngine(heapPath, walPath)
	defer engine.Close()

	// ========================================
	// 1. TypeInt - INTEIROS
	// ========================================
	fmt.Println("=== TypeInt (Inteiros) ===")

	for i := int64(1); i <= 5; i++ {
		doc := fmt.Sprintf(`{"id": %d, "type": "integer", "value": %d}`, i, i*100)
		engine.Put("int_table", "id", types.IntKey(i), doc)
	}

	// Buscar por inteiro
	doc, found, _ := engine.Get("int_table", "id", types.IntKey(3))
	if found {
		fmt.Printf("  IntKey(3): %s\n", doc)
	}

	// Range scan com inteiros
	results, _ := engine.Scan("int_table", "id", query.Between(types.IntKey(2), types.IntKey(4)))
	fmt.Printf("  Range [2-4]: %v\n", results)

	// ========================================
	// 2. TypeVarchar - STRINGS
	// ========================================
	fmt.Println("\n=== TypeVarchar (Strings) ===")

	names := []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}
	for _, name := range names {
		doc := fmt.Sprintf(`{"name": "%s", "type": "string"}`, name)
		engine.Put("string_table", "name", types.VarcharKey(name), doc)
	}

	// Buscar por string
	doc, found, _ = engine.Get("string_table", "name", types.VarcharKey("Charlie"))
	if found {
		fmt.Printf("  VarcharKey(\"Charlie\"): %s\n", doc)
	}

	// Range scan com strings (ordenação lexicográfica)
	results, _ = engine.Scan("string_table", "name", query.Between(
		types.VarcharKey("Bob"),
		types.VarcharKey("Diana"),
	))
	fmt.Printf("  Range [Bob-Diana]: %v\n", results)

	// ========================================
	// 3. TypeFloat - FLOATS
	// ========================================
	fmt.Println("\n=== TypeFloat (Ponto Flutuante) ===")

	prices := []float64{1.99, 5.50, 10.00, 25.75, 99.99}
	for _, price := range prices {
		doc := fmt.Sprintf(`{"price": %.2f, "type": "float"}`, price)
		engine.Put("float_table", "price", types.FloatKey(price), doc)
	}

	// Buscar por float
	doc, found, _ = engine.Get("float_table", "price", types.FloatKey(10.00))
	if found {
		fmt.Printf("  FloatKey(10.00): %s\n", doc)
	}

	// Produtos com preço > 5.00
	results, _ = engine.Scan("float_table", "price", query.GreaterThan(types.FloatKey(5.00)))
	fmt.Printf("  Preços > 5.00: %v\n", results)

	// ========================================
	// 4. TypeBoolean - BOOLEANOS
	// ========================================
	fmt.Println("\n=== TypeBoolean (Booleanos) ===")

	// Tabela de flags (exemplo: usuários ativos/inativos)
	engine.Put("bool_table", "active", types.BoolKey(false), `{"user": "inactive1", "active": false}`)
	engine.Put("bool_table", "active", types.BoolKey(true), `{"user": "active1", "active": true}`)

	// Buscar por boolean
	doc, found, _ = engine.Get("bool_table", "active", types.BoolKey(true))
	if found {
		fmt.Printf("  BoolKey(true): %s\n", doc)
	}

	doc, found, _ = engine.Get("bool_table", "active", types.BoolKey(false))
	if found {
		fmt.Printf("  BoolKey(false): %s\n", doc)
	}

	// ========================================
	// 5. TypeDate - DATAS
	// ========================================
	fmt.Println("\n=== TypeDate (Datas/Timestamps) ===")

	now := time.Now()
	dates := []time.Time{
		now.AddDate(0, 0, -7), // 7 dias atrás
		now.AddDate(0, 0, -3), // 3 dias atrás
		now,                   // Hoje
		now.AddDate(0, 0, 3),  // 3 dias no futuro
		now.AddDate(0, 0, 7),  // 7 dias no futuro
	}

	for _, date := range dates {
		dateStr := date.Format("2006-01-02")
		doc := fmt.Sprintf(`{"date": "%s", "type": "date"}`, dateStr)
		engine.Put("date_table", "date", types.DateKey(date), doc)
	}

	// Buscar por data específica
	doc, found, _ = engine.Get("date_table", "date", types.DateKey(now))
	if found {
		fmt.Printf("  DateKey(hoje): %s\n", doc)
	}

	// Datas maiores que hoje
	results, _ = engine.Scan("date_table", "date", query.GreaterThan(types.DateKey(now)))
	fmt.Printf("  Datas futuras: %v\n", results)

	// ========================================
	// 6. COMPARAÇÃO E ORDENAÇÃO
	// ========================================
	fmt.Println("\n=== Comparação de Tipos ===")

	// IntKey
	fmt.Printf("IntKey(5) vs IntKey(10): %d\n", types.IntKey(5).Compare(types.IntKey(10)))
	fmt.Printf("IntKey(10) vs IntKey(5): %d\n", types.IntKey(10).Compare(types.IntKey(5)))
	fmt.Printf("IntKey(5) vs IntKey(5): %d\n", types.IntKey(5).Compare(types.IntKey(5)))

	// VarcharKey (lexicográfica)
	fmt.Printf("\nVarcharKey(\"abc\") vs VarcharKey(\"xyz\"): %d\n",
		types.VarcharKey("abc").Compare(types.VarcharKey("xyz")))

	// BoolKey (false < true)
	fmt.Printf("\nBoolKey(false) vs BoolKey(true): %d\n",
		types.BoolKey(false).Compare(types.BoolKey(true)))

	// DateKey
	yesterday := now.AddDate(0, 0, -1)
	tomorrow := now.AddDate(0, 0, 1)
	fmt.Printf("\nDateKey(ontem) vs DateKey(amanhã): %d\n",
		types.DateKey(yesterday).Compare(types.DateKey(tomorrow)))

	fmt.Println("\n✓ Exemplo concluído!")
}

func setupEngine(heapPath, walPath string) *storage.StorageEngine {
	hm, err := heap.NewHeapManager(heapPath)
	if err != nil {
		fmt.Printf("Erro: %v\n", err)
		os.Exit(1)
	}

	tableMgr := storage.NewTableMenager()

	// Tabelas para cada tipo de dado
	tableMgr.NewTable("int_table", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm)

	tableMgr.NewTable("string_table", []storage.Index{
		{Name: "name", Primary: true, Type: storage.TypeVarchar},
	}, 3, hm)

	tableMgr.NewTable("float_table", []storage.Index{
		{Name: "price", Primary: true, Type: storage.TypeFloat},
	}, 3, hm)

	tableMgr.NewTable("bool_table", []storage.Index{
		{Name: "active", Primary: true, Type: storage.TypeBoolean},
	}, 3, hm)

	tableMgr.NewTable("date_table", []storage.Index{
		{Name: "date", Primary: true, Type: storage.TypeDate},
	}, 3, hm)

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	engine, _ := storage.NewStorageEngine(tableMgr, walWriter)

	return engine
}

func cleanup(walPath, heapPath string) {
	os.Remove(walPath)
	os.Remove(heapPath)
	os.RemoveAll("checkpoints")
}
