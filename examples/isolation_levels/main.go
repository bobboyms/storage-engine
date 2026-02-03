package main

import (
	"fmt"
	"os"
	"time"

	"github.com/bobboyms/storage-engine/pkg/heap"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

/*
EXEMPLO: Níveis de Isolamento de Transações

Este exemplo demonstra os dois níveis de isolamento suportados:

1. RepeatableRead (Padrão - Snapshot Isolation):
   - Transação vê um snapshot consistente do momento em que iniciou
   - Leituras repetidas retornam os mesmos dados
   - Não vê alterações feitas por outras transações após seu início

2. ReadCommitted:
   - Cada leitura vê os dados mais recentes commitados
   - Pode ver alterações feitas por outras transações durante sua execução
   - Menos isolada, mas mais "fresca"

Fenômenos de concorrência:
- RepeatableRead previne "non-repeatable reads"
- ReadCommitted permite "non-repeatable reads"
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
	// PREPARAÇÃO
	// ========================================
	fmt.Println("=== Preparação ===")

	// Inserir conta bancária inicial
	engine.Put("accounts", "id", types.IntKey(1), `{"id": 1, "balance": 1000, "owner": "Alice"}`)
	engine.Put("accounts", "id", types.IntKey(2), `{"id": 2, "balance": 2000, "owner": "Bob"}`)

	fmt.Println("✓ Contas criadas: Alice=$1000, Bob=$2000")

	// ========================================
	// CENÁRIO 1: REPEATABLE READ (Padrão)
	// ========================================
	fmt.Println("\n=== Cenário 1: Repeatable Read (Snapshot Isolation) ===")

	// Iniciar transação com Repeatable Read (padrão)
	tx1 := engine.BeginTransaction(storage.RepeatableRead)
	fmt.Println("TX1 iniciada (RepeatableRead)")

	// Ler saldo de Alice
	doc, _, _ := tx1.Get("accounts", "id", types.IntKey(1))
	fmt.Printf("TX1 - Primeira leitura de Alice: %s\n", doc)

	// Simular outra transação que modifica o saldo
	fmt.Println("\n[Outra transação atualiza saldo de Alice para $1500]")
	engine.Put("accounts", "id", types.IntKey(1), `{"id": 1, "balance": 1500, "owner": "Alice"}`)

	// Aguardar um pouco para garantir que a escrita foi feita
	time.Sleep(time.Millisecond * 10)

	// TX1 ainda deve ver o valor antigo (snapshot isolation)
	doc, _, _ = tx1.Get("accounts", "id", types.IntKey(1))
	fmt.Printf("TX1 - Segunda leitura de Alice (após update externo): %s\n", doc)

	// Nova transação deve ver o valor novo
	tx2 := engine.BeginTransaction(storage.RepeatableRead)
	doc, _, _ = tx2.Get("accounts", "id", types.IntKey(1))
	fmt.Printf("TX2 (nova) - Lê Alice: %s\n", doc)

	fmt.Println("\n→ RepeatableRead: TX1 vê o mesmo valor nas duas leituras")
	fmt.Println("  Isso PREVINE 'non-repeatable reads'")

	// ========================================
	// CENÁRIO 2: READ COMMITTED
	// ========================================
	fmt.Println("\n=== Cenário 2: Read Committed ===")

	// Reset
	engine.Put("accounts", "id", types.IntKey(1), `{"id": 1, "balance": 1000, "owner": "Alice"}`)

	// Iniciar transação com Read Committed
	tx3 := engine.BeginTransaction(storage.ReadCommitted)
	fmt.Println("TX3 iniciada (ReadCommitted)")

	// Primeira leitura
	doc, _, _ = tx3.Get("accounts", "id", types.IntKey(1))
	fmt.Printf("TX3 - Primeira leitura de Alice: %s\n", doc)

	// Outra transação modifica
	fmt.Println("\n[Outra transação atualiza saldo de Alice para $1800]")
	engine.Put("accounts", "id", types.IntKey(1), `{"id": 1, "balance": 1800, "owner": "Alice"}`)
	time.Sleep(time.Millisecond * 10)

	// TX3 deve ver o valor NOVO (read committed refresha o snapshot)
	doc, _, _ = tx3.Get("accounts", "id", types.IntKey(1))
	fmt.Printf("TX3 - Segunda leitura de Alice (após update externo): %s\n", doc)

	fmt.Println("\n→ ReadCommitted: TX3 vê o valor atualizado na segunda leitura")
	fmt.Println("  Isso PERMITE 'non-repeatable reads'")

	// ========================================
	// CENÁRIO 3: TRANSFERÊNCIA COM REPEATABLE READ
	// ========================================
	fmt.Println("\n=== Cenário 3: Operação de Transferência ===")

	// Reset
	engine.Put("accounts", "id", types.IntKey(1), `{"id": 1, "balance": 1000, "owner": "Alice"}`)
	engine.Put("accounts", "id", types.IntKey(2), `{"id": 2, "balance": 2000, "owner": "Bob"}`)

	// Transação de leitura que calcula saldo total
	fmt.Println("\nTX4: Calculando saldo total (RepeatableRead)")
	tx4 := engine.BeginTransaction(storage.RepeatableRead)

	docAlice, _, _ := tx4.Get("accounts", "id", types.IntKey(1))
	fmt.Printf("  Leu Alice: %s\n", docAlice)

	// Simular transferência durante a leitura
	fmt.Println("\n  [Transferência em andamento: Alice -$500, Bob +$500]")
	engine.Put("accounts", "id", types.IntKey(1), `{"id": 1, "balance": 500, "owner": "Alice"}`)
	engine.Put("accounts", "id", types.IntKey(2), `{"id": 2, "balance": 2500, "owner": "Bob"}`)
	time.Sleep(time.Millisecond * 10)

	docBob, _, _ := tx4.Get("accounts", "id", types.IntKey(2))
	fmt.Printf("  Leu Bob: %s\n", docBob)

	fmt.Println("\n→ Com RepeatableRead, TX4 vê estado CONSISTENTE:")
	fmt.Println("  Alice=$1000 + Bob=$2000 = $3000 (correto!)")
	fmt.Println("  Mesmo que a transferência tenha ocorrido durante a leitura")

	// Mostrar diferença com ReadCommitted
	fmt.Println("\n--- Se fosse ReadCommitted (hipotético) ---")
	fmt.Println("  Poderia ler Alice=$1000 e Bob=$2500")
	fmt.Println("  Saldo total seria $3500 (INCORRETO!)")

	// ========================================
	// RESUMO
	// ========================================
	fmt.Println("\n=== Resumo ===")
	fmt.Println("╔═══════════════════╦════════════════════════════════════════╗")
	fmt.Println("║ Nível             ║ Comportamento                          ║")
	fmt.Println("╠═══════════════════╬════════════════════════════════════════╣")
	fmt.Println("║ RepeatableRead    ║ Snapshot no início da transação        ║")
	fmt.Println("║ (Padrão)          ║ Leituras consistentes, sem mudanças    ║")
	fmt.Println("╠═══════════════════╬════════════════════════════════════════╣")
	fmt.Println("║ ReadCommitted     ║ Snapshot refreshado a cada leitura     ║")
	fmt.Println("║                   ║ Sempre vê dados mais recentes          ║")
	fmt.Println("╚═══════════════════╩════════════════════════════════════════╝")

	fmt.Println("\nQuando usar cada um?")
	fmt.Println("- RepeatableRead: Relatórios, cálculos agregados, consistência")
	fmt.Println("- ReadCommitted: Monitoramento em tempo real, dashboards")

	fmt.Println("\n✓ Exemplo concluído!")
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

func cleanup(walPath, heapPath string) {
	os.Remove(walPath)
	os.Remove(heapPath)
	os.RemoveAll("checkpoints")
}
