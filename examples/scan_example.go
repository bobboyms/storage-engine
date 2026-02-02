package main

import (
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/query"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

/*
EXEMPLO DE USO: Sistema de Scan com Operadores Genéricos

Este arquivo demonstra como usar o novo sistema de operadores de scan
que suporta: =, !=, >, <, >=, <=, BETWEEN
*/

func main() {
	// 1. Criar Table Manager e Tabela
	tableMgr := storage.NewTableMenager()
	err := tableMgr.NewTable("users", []storage.Index{
		{Name: "age", Primary: true, Type: storage.TypeInt},
		{Name: "name", Primary: false, Type: storage.TypeVarchar},
	}, 3)
	if err != nil {
		fmt.Printf("Erro: %v\n", err)
		return
	}

	// 2. Criar Storage Engine
	se := storage.NewStorageEngine(tableMgr)

	// 3. Inserir dados
	se.Put("users", "age", types.IntKey(15), 1)
	se.Put("users", "age", types.IntKey(18), 2)
	se.Put("users", "age", types.IntKey(25), 3)
	se.Put("users", "age", types.IntKey(30), 4)
	se.Put("users", "age", types.IntKey(65), 5)

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
