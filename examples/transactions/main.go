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
EXEMPLO: Write Transactions

Este exemplo demonstra:
1. BeginWriteTransaction() - Inicia uma transação de escrita
2. Put() e Del() dentro da transação
3. Commit() - Persiste todas as operações atomicamente
4. Rollback() - Descarta todas as operações pendentes

Transações de escrita garantem:
- Atomicidade: Todas as operações ou nenhuma
- Durabilidade: WAL com marcadores BEGIN/COMMIT/ABORT
- Consistência: Estado válido após commit ou rollback

Fluxo de WAL:
  BEGIN -> OP1 -> OP2 -> ... -> COMMIT (sucesso)
  BEGIN -> OP1 -> OP2 -> ABORT (rollback)
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
	// CENÁRIO 1: TRANSAÇÃO COM COMMIT
	// ========================================
	fmt.Println("=== Cenário 1: Transação com Commit ===")

	// Iniciar transação
	tx1 := engine.BeginWriteTransaction()
	fmt.Println("TX1 iniciada")

	// Adicionar operações à transação (ainda não persistidas)
	tx1.Put("accounts", "id", types.IntKey(1), `{"id": 1, "name": "Alice", "balance": 1000}`)
	tx1.Put("accounts", "id", types.IntKey(2), `{"id": 2, "name": "Bob", "balance": 2000}`)
	tx1.Put("accounts", "id", types.IntKey(3), `{"id": 3, "name": "Carol", "balance": 3000}`)
	fmt.Println("  3 operações adicionadas ao buffer da transação")

	// Verificar que os dados AINDA NÃO estão visíveis
	_, found, _ := engine.Get("accounts", "id", types.IntKey(1))
	fmt.Printf("  Antes do commit - Alice existe? %v\n", found)

	// Commit - persiste atomicamente todas as operações
	err := tx1.Commit()
	if err != nil {
		fmt.Printf("  Erro no commit: %v\n", err)
	} else {
		fmt.Println("  ✓ Commit realizado com sucesso")
	}

	// Agora os dados estão visíveis
	doc, found, _ := engine.Get("accounts", "id", types.IntKey(1))
	if found {
		fmt.Printf("  Após commit - Alice: %s\n", doc)
	}

	// ========================================
	// CENÁRIO 2: TRANSAÇÃO COM ROLLBACK
	// ========================================
	fmt.Println("\n=== Cenário 2: Transação com Rollback ===")

	// Estado inicial
	docBefore, _, _ := engine.Get("accounts", "id", types.IntKey(2))
	fmt.Printf("Estado inicial de Bob: %s\n", docBefore)

	// Iniciar transação de atualização
	tx2 := engine.BeginWriteTransaction()
	fmt.Println("TX2 iniciada")

	// Tentar atualizar Bob e deletar Carol
	tx2.Put("accounts", "id", types.IntKey(2), `{"id": 2, "name": "Bob", "balance": 5000}`)
	tx2.Del("accounts", "id", types.IntKey(3))
	fmt.Println("  Operações adicionadas: atualizar Bob, deletar Carol")

	// Simular erro ou decisão de cancelar
	fmt.Println("  [Decidimos cancelar a operação]")
	tx2.Rollback()
	fmt.Println("  ✓ Rollback realizado")

	// Verificar que nada mudou
	docAfter, _, _ := engine.Get("accounts", "id", types.IntKey(2))
	fmt.Printf("Estado de Bob após rollback: %s\n", docAfter)

	_, found3, _ := engine.Get("accounts", "id", types.IntKey(3))
	fmt.Printf("Carol ainda existe? %v\n", found3)

	// ========================================
	// CENÁRIO 3: TRANSFERÊNCIA ATÔMICA
	// ========================================
	fmt.Println("\n=== Cenário 3: Transferência Atômica entre Contas ===")

	// Ler saldos atuais
	fmt.Println("Saldos antes da transferência:")
	for i := int64(1); i <= 3; i++ {
		doc, _, _ := engine.Get("accounts", "id", types.IntKey(i))
		fmt.Printf("  Conta %d: %s\n", i, doc)
	}

	// Transferir $500 de Carol para Alice
	transfer := engine.BeginWriteTransaction()
	fmt.Println("\nIniciando transferência: Carol -> Alice ($500)")

	// Debitar Carol (de 3000 para 2500)
	transfer.Put("accounts", "id", types.IntKey(3), `{"id": 3, "name": "Carol", "balance": 2500}`)

	// Creditar Alice (de 1000 para 1500)
	transfer.Put("accounts", "id", types.IntKey(1), `{"id": 1, "name": "Alice", "balance": 1500}`)

	// Commit atômico
	err = transfer.Commit()
	if err == nil {
		fmt.Println("✓ Transferência concluída com sucesso")
	}

	// Verificar resultado
	fmt.Println("\nSaldos após a transferência:")
	for i := int64(1); i <= 3; i++ {
		doc, _, _ := engine.Get("accounts", "id", types.IntKey(i))
		fmt.Printf("  Conta %d: %s\n", i, doc)
	}

	// ========================================
	// CENÁRIO 4: TRANSAÇÃO EM MÚLTIPLAS TABELAS
	// ========================================
	fmt.Println("\n=== Cenário 4: Transação em Múltiplas Tabelas ===")

	// Criar tabela de transações para log
	engine.Close()
	engine = setupEngineWithLog(heapPath, walPath)

	// Inserir dados iniciais novamente
	init := engine.BeginWriteTransaction()
	init.Put("accounts", "id", types.IntKey(100), `{"id": 100, "name": "Empresa", "balance": 100000}`)
	init.Put("accounts", "id", types.IntKey(101), `{"id": 101, "name": "Fornecedor", "balance": 0}`)
	init.Commit()

	// Transação que atualiza múltiplas tabelas
	payment := engine.BeginWriteTransaction()
	fmt.Println("Iniciando pagamento para fornecedor...")

	// Atualizar contas
	payment.Put("accounts", "id", types.IntKey(100), `{"id": 100, "name": "Empresa", "balance": 90000}`)
	payment.Put("accounts", "id", types.IntKey(101), `{"id": 101, "name": "Fornecedor", "balance": 10000}`)

	// Registrar transação no log
	payment.Put("transactions", "id", types.IntKey(1),
		`{"id": 1, "from": 100, "to": 101, "amount": 10000, "status": "completed"}`)

	// Commit atômico - ou todas as mudanças acontecem ou nenhuma
	err = payment.Commit()
	if err == nil {
		fmt.Println("✓ Pagamento processado (conta + log)")
	}

	// Verificar
	doc, _, _ = engine.Get("accounts", "id", types.IntKey(100))
	fmt.Printf("  Empresa: %s\n", doc)
	doc, _, _ = engine.Get("accounts", "id", types.IntKey(101))
	fmt.Printf("  Fornecedor: %s\n", doc)
	doc, _, _ = engine.Get("transactions", "id", types.IntKey(1))
	fmt.Printf("  Log: %s\n", doc)

	// ========================================
	// RESUMO: GARANTIAS ACID
	// ========================================
	fmt.Println("\n=== Garantias ACID ===")
	fmt.Println(`
┌─────────────────────────────────────────────────────────────────┐
│ Propriedade   │ Implementação                                   │
├───────────────┼─────────────────────────────────────────────────┤
│ Atomicidade   │ Todas as operações são aplicadas juntas no      │
│               │ Commit ou nenhuma no Rollback                   │
├───────────────┼─────────────────────────────────────────────────┤
│ Consistência  │ Estado válido após cada transação               │
│               │ (ex: saldos sempre somam o mesmo total)         │
├───────────────┼─────────────────────────────────────────────────┤
│ Isolamento    │ Snapshot Isolation via BeginRead/BeginTx        │
│               │ (outras transações não veem mudanças pendentes) │
├───────────────┼─────────────────────────────────────────────────┤
│ Durabilidade  │ WAL com marcadores BEGIN/COMMIT/ABORT           │
│               │ Recovery reconstrói estado do WAL               │
└───────────────┴─────────────────────────────────────────────────┘
`)

	fmt.Println("✓ Exemplo concluído!")
}

func setupEngine(heapPath, walPath string) *storage.StorageEngine {
	hm, _ := heap.NewHeapManager(heapPath)

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("accounts", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm)

	walWriter, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	engine, _ := storage.NewStorageEngine(tableMgr, walWriter)

	return engine
}

func setupEngineWithLog(heapPath, walPath string) *storage.StorageEngine {
	hm, _ := heap.NewHeapManager(heapPath)

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("accounts", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm)
	tableMgr.NewTable("transactions", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
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
