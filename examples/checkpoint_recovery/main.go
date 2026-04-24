package main

import (
	"fmt"
	"os"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

/*
EXEMPLO: Flush Durável e Recovery

Este exemplo demonstra:
1. Flush durável explícito do estado page-based
2. Simulação de "crash" (fechamento sem flush explícito)
3. Recovery automático a partir do WAL
4. Verificação da integridade dos dados após recovery

O sistema usa Write-Ahead Logging (WAL) para garantir durabilidade:
- Todas as operações são primeiro escritas no WAL
- CreateCheckpoint agora força flush durável do estado page-based
- Recovery reconstrói o estado relendo o WAL
*/

func main() {
	// Configurações de arquivos
	walPath := "data.wal"
	heapPath := "data.heap"
	cleanup(walPath, heapPath)
	defer cleanup(walPath, heapPath)

	// ========================================
	// FASE 1: INSERÇÃO DE DADOS + FLUSH DURÁVEL
	// ========================================
	fmt.Println("=== FASE 1: Inserção e Flush Durável ===")

	engine := setupEngine(heapPath, walPath)

	// Inserir alguns dados
	users := []struct {
		id   int64
		name string
	}{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
		{4, "Diana"},
		{5, "Eve"},
	}

	for _, u := range users {
		doc := fmt.Sprintf(`{"id": %d, "name": "%s", "status": "active"}`, u.id, u.name)
		err := engine.Put("users", "id", types.IntKey(u.id), doc)
		if err != nil {
			fmt.Printf("Erro ao inserir user %d: %v\n", u.id, err)
		}
	}
	fmt.Printf("✓ %d usuários inseridos\n", len(users))

	// Força flush durável das páginas/árvores
	err := engine.CreateCheckpoint()
	if err != nil {
		fmt.Printf("Erro ao flushar estado: %v\n", err)
	} else {
		fmt.Println("✓ Estado flushado com sucesso")
	}

	// Verificar dados antes do "crash"
	fmt.Println("\nDados antes do crash:")
	for i := int64(1); i <= 5; i++ {
		doc, found, _ := engine.Get("users", "id", types.IntKey(i))
		if found {
			fmt.Printf("  User %d: %s\n", i, doc)
		}
	}

	// Fechar engine (simula shutdown normal)
	engine.Close()
	fmt.Println("\n✓ Engine fechado (estado flushado)")

	// ========================================
	// FASE 2: NOVAS OPERAÇÕES SEM FLUSH EXPLÍCITO
	// ========================================
	fmt.Println("\n=== FASE 2: Operações sem Flush Explícito (simulando crash) ===")

	engine = setupEngine(heapPath, walPath)

	err = engine.Recover(walPath)
	if err != nil {
		fmt.Printf("Erro no recover inicial: %v\n", err)
	}
	fmt.Println("✓ Estado recuperado do WAL")

	// Inserir mais dados (estes só estão duráveis no WAL)
	newUsers := []struct {
		id   int64
		name string
	}{
		{6, "Frank"},
		{7, "Grace"},
		{8, "Henry"},
	}

	for _, u := range newUsers {
		doc := fmt.Sprintf(`{"id": %d, "name": "%s", "status": "new"}`, u.id, u.name)
		engine.Put("users", "id", types.IntKey(u.id), doc)
	}
	fmt.Printf("✓ %d novos usuários inseridos (apenas no WAL)\n", len(newUsers))

	// Atualizar um usuário existente
	engine.Put("users", "id", types.IntKey(1), `{"id": 1, "name": "Alice Updated", "status": "modified"}`)
	fmt.Println("✓ User 1 atualizado (apenas no WAL)")

	// Deletar um usuário
	engine.Del("users", "id", types.IntKey(3))
	fmt.Println("✓ User 3 deletado (apenas no WAL)")

	// **SIMULAR CRASH: Fechar sem flush explícito**
	// As operações já estão persistidas no WAL.
	engine.Close()
	fmt.Println("\n⚠️  Engine fechado SEM flush explícito (simulando crash)")

	// ========================================
	// FASE 3: RECOVERY APÓS "CRASH"
	// ========================================
	fmt.Println("\n=== FASE 3: Recovery ===")

	// Reabrir o engine
	engine = setupEngine(heapPath, walPath)

	// Executar recovery - deve reconstituir:
	// 1. Estado flushado (users 1-5)
	// 2. Operações do WAL após o flush (users 6-8, update user 1, delete user 3)
	err = engine.Recover(walPath)
	if err != nil {
		fmt.Printf("❌ Erro no recovery: %v\n", err)
		return
	}
	fmt.Println("✓ Recovery executado com sucesso!")

	// ========================================
	// FASE 4: VERIFICAÇÃO DOS DADOS
	// ========================================
	fmt.Println("\n=== FASE 4: Verificação dos Dados ===")

	fmt.Println("\nEstado esperado após recovery:")
	fmt.Println("- User 1: Alice Updated (atualizado via WAL)")
	fmt.Println("- User 2: Bob (do flush)")
	fmt.Println("- User 3: (deletado via WAL)")
	fmt.Println("- User 4: Diana (do flush)")
	fmt.Println("- User 5: Eve (do flush)")
	fmt.Println("- User 6, 7, 8: novos (do WAL)")

	fmt.Println("\nDados reais recuperados:")
	for i := int64(1); i <= 8; i++ {
		doc, found, _ := engine.Get("users", "id", types.IntKey(i))
		if found {
			fmt.Printf("  ✓ User %d: %s\n", i, doc)
		} else {
			fmt.Printf("  ✗ User %d: (não encontrado/deletado)\n", i)
		}
	}

	engine.Close()
	fmt.Println("\n✓ Exemplo concluído com sucesso!")
}

func setupEngine(heapPath, walPath string) *storage.StorageEngine {
	// Criar ou abrir Heap
	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, heapPath, nil)
	if err != nil {
		fmt.Printf("Erro ao criar heap: %v\n", err)
		os.Exit(1)
	}

	// Criar tabela
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
		{Name: "name", Primary: false, Type: storage.TypeVarchar},
	}, 3, hm)

	// Criar WAL
	walWriter, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		fmt.Printf("Erro ao criar WAL: %v\n", err)
		os.Exit(1)
	}

	// Criar engine
	engine, err := storage.NewStorageEngine(tableMgr, walWriter)
	if err != nil {
		fmt.Printf("Erro ao criar engine: %v\n", err)
		walWriter.Close()
		os.Exit(1)
	}

	return engine
}

func cleanup(walPath, heapPath string) {
	os.Remove(walPath)
	os.Remove(heapPath)
}
