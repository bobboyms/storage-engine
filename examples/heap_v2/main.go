// EXEMPLO: Heap page-based (v2) via opt-in
//
// Este example mostra como criar uma tabela que usa o HEAP V2 — a
// implementação page-based com BufferPool e Vacuum in-place.
//
// Diferença vs o heap v1:
//   - v1 (padrão): pkg/heap, append-only segmentado. Sem cache.
//   - v2 (opt-in): pkg/heap/v2, pages de 8KB no pagestore, BufferPool
//     de 64 pages em RAM, Vacuum compactante sem reescrever o B+ tree.
//
// A API do engine é idêntica — Put/Get/Delete/Vacuum funcionam igual.
// A única mudança é no momento de criar o heap.
package main

import (
	"fmt"
	"os"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

const (
	dbPath  = "users_v2.heap"
	walPath = "users_v2.wal"
)

func main() {
	cleanup()
	defer cleanup()

	// ─────────────────────────────────────────────────────────────
	// 1. Cria o heap v2 via NewHeapForTable
	// ─────────────────────────────────────────────────────────────
	// Cifra nil = sem TDE; passe um crypto.Cipher para ativar.
	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, dbPath, nil)
	if err != nil {
		fmt.Printf("error criando heap v2: %v\n", err)
		return
	}

	// Registra a tabela com o heap v2 — o resto do engine not enxerga
	// diferença: Put/Get/Delete vão todos pela mesma interface heap.Heap.
	tableMgr := storage.NewTableMenager()
	err = tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3, hm)
	if err != nil {
		fmt.Printf("error criando tabela: %v\n", err)
		return
	}

	walWriter, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		fmt.Printf("error criando WAL: %v\n", err)
		return
	}

	se, err := storage.NewStorageEngine(tableMgr, walWriter)
	if err != nil {
		walWriter.Close()
		fmt.Printf("error criando engine: %v\n", err)
		return
	}
	defer se.Close()

	// ─────────────────────────────────────────────────────────────
	// 2. INSERT: escreve três documentos
	// ─────────────────────────────────────────────────────────────
	docs := []struct {
		id   int64
		json string
	}{
		{1, `{"id":1,"nome":"Alice","email":"alice@example.com"}`},
		{2, `{"id":2,"nome":"Bob","email":"bob@example.com"}`},
		{3, `{"id":3,"nome":"Carol","email":"carol@example.com"}`},
	}

	for _, d := range docs {
		if err := se.Put("users", "id", types.IntKey(d.id), d.json); err != nil {
			fmt.Printf("error put id=%d: %v\n", d.id, err)
			return
		}
	}
	fmt.Printf("✓ Inseridos %d documentos\n", len(docs))

	// ─────────────────────────────────────────────────────────────
	// 3. GET: lê cada um pelo id
	// ─────────────────────────────────────────────────────────────
	for _, d := range docs {
		doc, found, err := se.Get("users", "id", types.IntKey(d.id))
		if err != nil {
			fmt.Printf("error get id=%d: %v\n", d.id, err)
			return
		}
		if !found {
			fmt.Printf("id=%d: NÃO ENCONTRADO\n", d.id)
			continue
		}
		fmt.Printf("✓ id=%d: %s\n", d.id, doc)
	}

	// ─────────────────────────────────────────────────────────────
	// 4. UPDATE: cria nova versão encadeada via MVCC
	// ─────────────────────────────────────────────────────────────
	err = se.Put("users", "id", types.IntKey(1), `{"id":1,"nome":"Alice Atualizada","email":"alice+new@example.com"}`)
	if err != nil {
		fmt.Printf("error update: %v\n", err)
		return
	}
	doc, _, _ := se.Get("users", "id", types.IntKey(1))
	fmt.Printf("✓ Após update, id=1: %s\n", doc)

	// ─────────────────────────────────────────────────────────────
	// 5. VACUUM: dispatch polimórfico — vai pro caminho v2 (compact
	//    in-place). Note a mensagem "Vacuum v2 completed" que o
	//    storage engine emite.
	// ─────────────────────────────────────────────────────────────
	if err := se.Vacuum("users"); err != nil {
		fmt.Printf("error vacuum: %v\n", err)
		return
	}

	// Vacuum not deveria afetar read de linhas vivas
	doc, found, _ := se.Get("users", "id", types.IntKey(2))
	if !found || doc == "" {
		fmt.Printf("inesperado: id=2 sumiu after vacuum\n")
		return
	}
	fmt.Printf("✓ Após vacuum, id=2 ainda legível: %s\n", doc)

	fmt.Println("\nOK — heap v2 funciona drop-in no engine.")
}

func cleanup() {
	// v2 cria files com o path exato (sem sufixo _001.data)
	os.Remove(dbPath)
	os.Remove(walPath)
}
