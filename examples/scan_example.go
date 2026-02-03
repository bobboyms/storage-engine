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
EXEMPLO DE USO: Sistema de Scan com Operadores Genéricos

Este arquivo demonstra como usar o novo sistema de operadores de scan
que suporta: =, !=, >, <, >=, <=, BETWEEN
*/

func main() {
	// Cleanup previous run
	os.Remove("example.wal")
	os.Remove("example.heap")

	// 1. Criar Heap Manager
	hm, err := heap.NewHeapManager("example.heap")
	if err != nil {
		fmt.Printf("Erro ao criar heap: %v\n", err)
		return
	}

	// 2. Criar Table Manager e Tabela
	tableMgr := storage.NewTableMenager()
	err = tableMgr.NewTable("users", []storage.Index{
		{Name: "age", Primary: true, Type: storage.TypeInt},
		{Name: "name", Primary: false, Type: storage.TypeVarchar},
	}, 3, hm)
	if err != nil {
		fmt.Printf("Erro: %v\n", err)
		return
	}

	// 3. Criar Storage Engine
	walWriter, _ := wal.NewWALWriter("example.wal", wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(tableMgr, walWriter)
	defer paramsCleanup()

	// 4. Inserir dados
	se.Put("users", "age", types.IntKey(65), "user_65")
	se.Put("users", "age", types.IntKey(18), "user_18")
	se.Put("users", "age", types.IntKey(30), "user_30")

	// ========================================
	// OPERADOR EQUAL (=)
	// ========================================
	// SQL: SELECT * FROM users WHERE age = 18
	results, _ := se.Scan("users", "age", query.Equal(types.IntKey(18)))
	fmt.Printf("Age = 18: %v\n", results)

	// ========================================
	// OPERADOR GREATER THAN (>)
	// ========================================
	// SQL: SELECT * FROM users WHERE age > 18
	results, _ = se.Scan("users", "age", query.GreaterThan(types.IntKey(18)))
	fmt.Printf("Age > 18: %v\n", results)

	// ========================================
	// OPERADOR BETWEEN
	// ========================================
	// SQL: SELECT * FROM users WHERE age BETWEEN 18 AND 30
	results, _ = se.Scan("users", "age", query.Between(types.IntKey(18), types.IntKey(30)))
	fmt.Printf("Age BETWEEN 18 AND 30: %v\n", results)
}

func paramsCleanup() {
	os.Remove("example.wal")
	os.Remove("example.heap")
}
