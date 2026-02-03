package main

import (
	"fmt"
	"os"

	"github.com/bobboyms/storage-engine/pkg/heap"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

/*
EXEMPLO: Operações CRUD Básicas

Este exemplo demonstra as operações fundamentais do Storage Engine:
- Put: Inserir ou atualizar documentos
- Get: Buscar documentos por chave
- Del: Remover documentos

O Storage Engine usa B+Tree para indexação e Heap para armazenamento.
*/

func main() {
	// Cleanup de arquivos anteriores
	cleanup()
	defer cleanup()

	// ========================================
	// 1. CONFIGURAÇÃO DO STORAGE ENGINE
	// ========================================

	// Inicializar Heap
	hm, err := heap.NewHeapManager("data.heap")
	if err != nil {
		fmt.Printf("Erro ao criar heap: %v\n", err)
		return
	}

	// Criar o gerenciador de tabelas
	tableMgr := storage.NewTableMenager()

	// Definir a estrutura da tabela "products"
	// - "id": chave primária do tipo inteiro
	// - "name": índice secundário do tipo varchar
	err = tableMgr.NewTable("products", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
		{Name: "name", Primary: false, Type: storage.TypeVarchar},
	}, 3, hm) // Parâmetro t=3 define o grau mínimo da B+Tree

	if err != nil {
		fmt.Printf("Erro ao criar tabela: %v\n", err)
		return
	}

	// Inicializar o Storage Engine com WAL
	walWriter, err := wal.NewWALWriter("data.wal", wal.DefaultOptions())
	if err != nil {
		fmt.Printf("Erro ao criar WAL: %v\n", err)
		return
	}
	engine, err := storage.NewStorageEngine(tableMgr, walWriter)
	if err != nil {
		walWriter.Close()
		fmt.Printf("Erro ao criar engine: %v\n", err)
		return
	}
	defer engine.Close()

	// ========================================
	// 2. OPERAÇÃO PUT (INSERT/UPDATE)
	// ========================================
	fmt.Println("=== PUT (Insert) ===")

	// Inserir documentos JSON
	products := []struct {
		id   int64
		name string
		json string
	}{
		{1, "Laptop", `{"id": 1, "name": "Laptop", "price": 2500.00, "stock": 10}`},
		{2, "Mouse", `{"id": 2, "name": "Mouse", "price": 50.00, "stock": 100}`},
		{3, "Keyboard", `{"id": 3, "name": "Keyboard", "price": 150.00, "stock": 50}`},
		{4, "Monitor", `{"id": 4, "name": "Monitor", "price": 800.00, "stock": 25}`},
	}

	for _, p := range products {
		// Put no índice primário (id)
		err := engine.Put("products", "id", types.IntKey(p.id), p.json)
		if err != nil {
			fmt.Printf("Erro ao inserir produto %d: %v\n", p.id, err)
			continue
		}

		// ⚠️  IMPORTANTE: Put no índice secundário DUPLICA o documento no heap!
		// Cada Put() escreve uma cópia completa no heap.
		// Para 4 produtos com 2 índices = 8 registros no heap (não 4).
		// Isso é uma limitação do design atual - índices são independentes.
		err = engine.Put("products", "name", types.VarcharKey(p.name), p.json)
		if err != nil {
			fmt.Printf("Erro ao indexar nome %s: %v\n", p.name, err)
		}
	}
	fmt.Println("✓ 4 produtos inseridos com sucesso!")
	fmt.Println("  (Nota: 8 registros no heap devido aos 2 índices)")

	// ========================================
	// 3. OPERAÇÃO GET (READ)
	// ========================================
	fmt.Println("\n=== GET (Read) ===")

	// Buscar pelo índice primário (id)
	doc, found, err := engine.Get("products", "id", types.IntKey(2))
	if err != nil {
		fmt.Printf("Erro ao buscar: %v\n", err)
	} else if found {
		fmt.Printf("Produto ID=2: %s\n", doc)
	} else {
		fmt.Println("Produto ID=2 não encontrado")
	}

	// Buscar pelo índice secundário (name)
	doc, found, err = engine.Get("products", "name", types.VarcharKey("Laptop"))
	if err != nil {
		fmt.Printf("Erro ao buscar: %v\n", err)
	} else if found {
		fmt.Printf("Produto Name='Laptop': %s\n", doc)
	} else {
		fmt.Println("Produto 'Laptop' não encontrado")
	}

	// Buscar chave inexistente
	_, found, _ = engine.Get("products", "id", types.IntKey(999))
	fmt.Printf("Produto ID=999 existe? %v\n", found)

	// ========================================
	// 4. OPERAÇÃO PUT (UPDATE)
	// ========================================
	fmt.Println("\n=== PUT (Update) ===")

	// Atualizar um documento existente (mesmo id sobrescreve)
	updatedDoc := `{"id": 1, "name": "Laptop Pro", "price": 3500.00, "stock": 5}`
	err = engine.Put("products", "id", types.IntKey(1), updatedDoc)
	if err != nil {
		fmt.Printf("Erro ao atualizar: %v\n", err)
	}

	// Verificar atualização
	doc, _, _ = engine.Get("products", "id", types.IntKey(1))
	fmt.Printf("Produto ID=1 atualizado: %s\n", doc)

	// ========================================
	// 5. OPERAÇÃO DEL (DELETE)
	// ========================================
	fmt.Println("\n=== DEL (Delete) ===")

	// Remover produto
	deleted, err := engine.Del("products", "id", types.IntKey(4))
	if err != nil {
		fmt.Printf("Erro ao deletar: %v\n", err)
	} else {
		fmt.Printf("Produto ID=4 deletado? %v\n", deleted)
	}

	// Tentar buscar item deletado
	_, found, _ = engine.Get("products", "id", types.IntKey(4))
	fmt.Printf("Produto ID=4 existe após delete? %v\n", found)

	// Tentar deletar item inexistente
	deleted, _ = engine.Del("products", "id", types.IntKey(999))
	fmt.Printf("Produto ID=999 deletado (não existia)? %v\n", deleted)

	// ========================================
	// RESUMO FINAL
	// ========================================
	fmt.Println("\n=== Estado Final ===")
	for i := int64(1); i <= 4; i++ {
		doc, found, _ := engine.Get("products", "id", types.IntKey(i))
		if found {
			fmt.Printf("ID=%d: %s\n", i, doc)
		} else {
			fmt.Printf("ID=%d: (deletado)\n", i)
		}
	}
}

func cleanup() {
	os.Remove("data.wal")
	os.Remove("data.heap")
	os.RemoveAll("checkpoints")
}
