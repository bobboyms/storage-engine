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
EXEMPLO: Navegação com Cursor

Este exemplo demonstra:
1. Uso do Cursor para navegar pela B+Tree
2. Operações: Seek(), Next(), Key(), Value(), Valid()
3. Iteração ordenada sobre documentos
4. Leitura do Heap a partir dos ponteiros da B+Tree

O Cursor permite navegação eficiente sobre dados ordenados,
similar a um iterator em outras linguagens.
*/

func main() {
	// Configuração
	walPath := "data.wal"
	heapPath := "data.heap"

	cleanup(walPath, heapPath)
	defer cleanup(walPath, heapPath)

	engine, hm := setupEngine(heapPath, walPath)
	defer engine.Close()

	// ========================================
	// 1. INSERIR DADOS ORDENADOS
	// ========================================
	fmt.Println("=== Inserindo Dados ===")

	products := []struct {
		id    int64
		name  string
		price float64
	}{
		{10, "Apple", 1.50},
		{20, "Banana", 0.75},
		{30, "Cherry", 3.00},
		{40, "Date", 2.50},
		{50, "Elderberry", 4.00},
		{60, "Fig", 2.00},
		{70, "Grape", 1.25},
		{80, "Honeydew", 3.50},
	}

	for _, p := range products {
		doc := fmt.Sprintf(`{"id": %d, "name": "%s", "price": %.2f}`, p.id, p.name, p.price)
		engine.Put("products", "id", types.IntKey(p.id), doc)
	}
	fmt.Printf("✓ %d produtos inseridos\n\n", len(products))

	// ========================================
	// 2. OBTER ÍNDICE E CRIAR CURSOR
	// ========================================
	fmt.Println("=== Navegação com Cursor ===")

	// Obter a B+Tree do índice
	index, err := engine.TableMetaData.GetIndexByName("products", "id")
	if err != nil {
		fmt.Printf("Erro: %v\n", err)
		return
	}

	// Criar cursor sobre a árvore
	cursor := engine.Cursor(index.Tree)
	defer cursor.Close() // Importante: libera locks

	// ========================================
	// 3. SEEK E ITERAÇÃO
	// ========================================
	fmt.Println("--- Seek para ID >= 30, iterar até o fim ---")

	// Seek posiciona no primeiro registro >= chave
	cursor.Seek(types.IntKey(30))

	for cursor.Valid() {
		key := cursor.Key()
		offset := cursor.Value()

		// Ler documento do Heap usando o offset
		doc, _, err := hm.Read(offset)
		if err != nil {
			fmt.Printf("Erro ao ler heap: %v\n", err)
			cursor.Next()
			continue
		}

		fmt.Printf("  Key: %v, Offset: %d, Doc: %s\n", key, offset, string(doc))

		// Avançar para o próximo registro
		if !cursor.Next() {
			break
		}
	}

	// ========================================
	// 4. NOVO CURSOR - SEEK PARA VALOR INEXISTENTE
	// ========================================
	fmt.Println("\n--- Seek para ID >= 25 (não existe, vai para 30) ---")

	cursor2 := engine.Cursor(index.Tree)
	defer cursor2.Close()

	cursor2.Seek(types.IntKey(25)) // 25 não existe, cursor vai para 30

	if cursor2.Valid() {
		fmt.Printf("  Primeiro registro encontrado: Key=%v\n", cursor2.Key())
	}

	// ========================================
	// 5. ITERAR TODOS OS REGISTROS
	// ========================================
	fmt.Println("\n--- Iteração completa (do início) ---")

	cursor3 := engine.Cursor(index.Tree)
	defer cursor3.Close()

	// Seek para o menor valor possível
	cursor3.Seek(types.IntKey(0))

	count := 0
	for cursor3.Valid() {
		key := cursor3.Key()
		offset := cursor3.Value()
		doc, _, _ := hm.Read(offset)

		fmt.Printf("  [%d] Key: %v -> %s\n", count, key, string(doc))
		count++

		if !cursor3.Next() {
			break
		}
	}
	fmt.Printf("\n✓ Total de registros iterados: %d\n", count)

	// ========================================
	// 6. RANGE SCAN COM CURSOR
	// ========================================
	fmt.Println("\n--- Range Scan: 20 <= ID <= 50 ---")

	cursor4 := engine.Cursor(index.Tree)
	defer cursor4.Close()

	startKey := types.IntKey(20)
	endKey := types.IntKey(50)

	cursor4.Seek(startKey)

	for cursor4.Valid() {
		key := cursor4.Key()

		// Verificar se ultrapassou o limite superior
		if key.Compare(endKey) > 0 {
			break
		}

		offset := cursor4.Value()
		doc, _, _ := hm.Read(offset)

		fmt.Printf("  Key: %v -> %s\n", key, string(doc))

		if !cursor4.Next() {
			break
		}
	}

	fmt.Println("\n✓ Exemplo concluído!")
}

func setupEngine(heapPath, walPath string) (*storage.StorageEngine, *heap.HeapManager) {
	hm, err := heap.NewHeapManager(heapPath)
	if err != nil {
		fmt.Printf("Erro: %v\n", err)
		os.Exit(1)
	}

	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("products", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
		{Name: "name", Primary: false, Type: storage.TypeVarchar},
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
