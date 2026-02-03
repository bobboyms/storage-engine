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
EXEMPLO: Múltiplos Índices

Este exemplo demonstra:
1. Criação de tabela com índice primário e secundários
2. Busca por diferentes índices
3. Trade-offs: espaço vs performance
4. Quando usar índices secundários

Conceitos:
- Índice Primário: Chave única, usado para identificação
- Índice Secundário: Permite buscas rápidas por outros campos

IMPORTANTE: Cada índice adicional aumenta:
- Tempo de escrita (mais atualizações de B+Tree)
- Espaço de armazenamento (cópias dos ponteiros)
- Mas ACELERA leituras por aquele campo
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
	// 1. INSERIR DADOS COM MÚLTIPLOS ÍNDICES
	// ========================================
	fmt.Println("=== Inserção de Dados ===")

	// Usando InsertRow para atualizar todos os índices atomicamente
	employees := []struct {
		id         int64
		email      string
		department string
		salary     float64
	}{
		{1, "alice@company.com", "Engineering", 95000.00},
		{2, "bob@company.com", "Engineering", 85000.00},
		{3, "carol@company.com", "Sales", 75000.00},
		{4, "david@company.com", "Engineering", 90000.00},
		{5, "eva@company.com", "HR", 70000.00},
		{6, "frank@company.com", "Sales", 80000.00},
		{7, "grace@company.com", "Engineering", 100000.00},
		{8, "henry@company.com", "HR", 65000.00},
	}

	for _, emp := range employees {
		doc := fmt.Sprintf(`{"id": %d, "email": "%s", "department": "%s", "salary": %.2f}`,
			emp.id, emp.email, emp.department, emp.salary)

		err := engine.InsertRow("employees", doc, map[string]types.Comparable{
			"id":         types.IntKey(emp.id),
			"email":      types.VarcharKey(emp.email),
			"department": types.VarcharKey(emp.department),
			"salary":     types.FloatKey(emp.salary),
		})

		if err != nil {
			fmt.Printf("Erro ao inserir %s: %v\n", emp.email, err)
		}
	}
	fmt.Printf("✓ %d funcionários inseridos\n", len(employees))

	// ========================================
	// 2. BUSCA POR ÍNDICE PRIMÁRIO (ID)
	// ========================================
	fmt.Println("\n=== Busca por Índice Primário (id) ===")

	// O(log n) - Busca direta na B+Tree
	doc, found, _ := engine.Get("employees", "id", types.IntKey(3))
	if found {
		fmt.Printf("ID=3: %s\n", doc)
	}

	// ========================================
	// 3. BUSCA POR ÍNDICE SECUNDÁRIO (EMAIL)
	// ========================================
	fmt.Println("\n=== Busca por Índice Secundário (email) ===")

	// Também O(log n) graças ao índice
	doc, found, _ = engine.Get("employees", "email", types.VarcharKey("grace@company.com"))
	if found {
		fmt.Printf("Email='grace@company.com': %s\n", doc)
	}

	// ========================================
	// 4. BUSCA POR DEPARTAMENTO
	// ========================================
	fmt.Println("\n=== Busca por Departamento (índice secundário) ===")

	// Buscar todos do Engineering usando scan
	fmt.Println("Funcionários do Engineering:")
	results, _ := engine.Scan("employees", "department", query.Equal(types.VarcharKey("Engineering")))
	for _, r := range results {
		fmt.Printf("  %s\n", r)
	}

	// Buscar todos de Sales
	fmt.Println("\nFuncionários de Sales:")
	results, _ = engine.Scan("employees", "department", query.Equal(types.VarcharKey("Sales")))
	for _, r := range results {
		fmt.Printf("  %s\n", r)
	}

	// ========================================
	// 5. BUSCA POR RANGE DE SALÁRIO
	// ========================================
	fmt.Println("\n=== Busca por Range de Salário ===")

	// Funcionários com salário >= 80000
	fmt.Println("Salário >= $80,000:")
	results, _ = engine.Scan("employees", "salary", query.GreaterOrEqual(types.FloatKey(80000.00)))
	for _, r := range results {
		fmt.Printf("  %s\n", r)
	}

	// Funcionários com salário entre 70000 e 90000
	fmt.Println("\nSalário entre $70,000 e $90,000:")
	results, _ = engine.Scan("employees", "salary", query.Between(
		types.FloatKey(70000.00),
		types.FloatKey(90000.00),
	))
	for _, r := range results {
		fmt.Printf("  %s\n", r)
	}

	// ========================================
	// 6. COMPARAÇÃO DE PERFORMANCE
	// ========================================
	fmt.Println("\n=== Comparação de Performance ===")

	fmt.Println(`
┌─────────────────────────────────────────────────────────────────┐
│ Cenário: Buscar funcionário por email                          │
├─────────────────────────────────────────────────────────────────┤
│ SEM índice secundário:                                          │
│   • Precisa fazer SCAN em toda a tabela                         │
│   • Complexidade: O(n) onde n = número de registros             │
│   • Com 1 milhão de registros: ~1 milhão de comparações         │
├─────────────────────────────────────────────────────────────────┤
│ COM índice secundário (email):                                  │
│   • Busca direta na B+Tree do índice                            │
│   • Complexidade: O(log n)                                      │
│   • Com 1 milhão de registros: ~20 comparações                  │
└─────────────────────────────────────────────────────────────────┘
`)

	// ========================================
	// 7. TRADE-OFFS
	// ========================================
	fmt.Println("=== Trade-offs de Múltiplos Índices ===")

	fmt.Println(`
┌──────────────────┬─────────────────────────────────────────────┐
│ Vantagens        │ Desvantagens                                │
├──────────────────┼─────────────────────────────────────────────┤
│ ✓ Buscas rápidas │ ✗ Mais espaço em disco (B+Tree extra)       │
│   por múltiplos  │                                             │
│   campos         │ ✗ Escritas mais lentas (atualiza N árvores) │
│                  │                                             │
│ ✓ Range scans    │ ✗ Mais complexidade no recovery             │
│   eficientes     │                                             │
│                  │ ✗ Checkpoints maiores                       │
└──────────────────┴─────────────────────────────────────────────┘

Quando criar índice secundário?
  ✓ Campo frequentemente usado em WHERE
  ✓ Campo usado em ORDER BY
  ✓ Campo usado em JOINs (quando suportado)

Quando NÃO criar índice secundário?
  ✗ Tabela pequena (< 1000 registros)
  ✗ Campo raramente consultado
  ✗ Alto volume de escritas e poucas leituras
`)

	// ========================================
	// 8. EXEMPLO: ATUALIZAÇÃO
	// ========================================
	fmt.Println("=== Atualização de Registro ===")

	// Atualizar salário do funcionário ID=2
	newDoc := `{"id": 2, "email": "bob@company.com", "department": "Engineering", "salary": 95000.00}`

	// IMPORTANTE: Ao atualizar, use InsertRow para manter todos os índices consistentes
	err := engine.InsertRow("employees", newDoc, map[string]types.Comparable{
		"id":         types.IntKey(2),
		"email":      types.VarcharKey("bob@company.com"),
		"department": types.VarcharKey("Engineering"),
		"salary":     types.FloatKey(95000.00),
	})

	if err != nil {
		fmt.Printf("Erro: %v\n", err)
	} else {
		fmt.Println("✓ Bob promovido: salário atualizado para $95,000")

		// Verificar
		doc, _, _ := engine.Get("employees", "id", types.IntKey(2))
		fmt.Printf("  Verificação: %s\n", doc)
	}

	fmt.Println("\n✓ Exemplo concluído!")
}

func setupEngine(heapPath, walPath string) *storage.StorageEngine {
	hm, _ := heap.NewHeapManager(heapPath)

	tableMgr := storage.NewTableMenager()

	// Tabela com 4 índices:
	// - id: Primário (identificação única)
	// - email: Secundário (busca por email)
	// - department: Secundário (busca por departamento)
	// - salary: Secundário (range queries de salário)
	tableMgr.NewTable("employees", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
		{Name: "email", Primary: false, Type: storage.TypeVarchar},
		{Name: "department", Primary: false, Type: storage.TypeVarchar},
		{Name: "salary", Primary: false, Type: storage.TypeFloat},
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
