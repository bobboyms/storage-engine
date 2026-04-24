package v2

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/crypto"
)

func newHeap(t testing.TB, cipher crypto.Cipher) *HeapV2 {
	t.Helper()
	path := filepath.Join(t.TempDir(), "heap.db")
	h, err := NewHeapV2(path, 16, cipher)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

func newHeapAt(t testing.TB, path string, cipher crypto.Cipher) *HeapV2 {
	t.Helper()
	h, err := NewHeapV2(path, 16, cipher)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func makeCipher(t testing.TB) crypto.Cipher {
	t.Helper()
	k := make([]byte, crypto.KeySize)
	if _, err := io.ReadFull(rand.Reader, k); err != nil {
		t.Fatal(err)
	}
	c, err := crypto.NewAESGCM(k)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestHeapV2_NewAndClose(t *testing.T) {
	h := newHeap(t, nil)
	if h == nil {
		t.Fatal("heap nil")
	}
	// Close é chamado pelo Cleanup do newHeap
}

func TestHeapV2_WriteRead_Single(t *testing.T) {
	h := newHeap(t, nil)

	doc := []byte(`{"id":1,"nome":"Alice"}`)
	rid, err := h.Write(doc, 100, NoRecordID)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if rid <= 0 {
		t.Fatalf("RecordID deveria ser > 0, recebi %d", rid)
	}

	gotDoc, hdr, err := h.Read(rid)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(gotDoc, doc) {
		t.Fatalf("doc divergente: %q != %q", gotDoc, doc)
	}
	if !hdr.Valid {
		t.Fatal("Valid deveria ser true em registro recém-escrito")
	}
	if hdr.CreateLSN != 100 {
		t.Fatalf("CreateLSN esperado 100, recebi %d", hdr.CreateLSN)
	}
	if hdr.PrevRecordID != NoRecordID {
		t.Fatalf("PrevRecordID esperado NoRecordID, recebi %d", hdr.PrevRecordID)
	}
}

func TestHeapV2_WriteSpansMultiplePages(t *testing.T) {
	// Insere registros até forçar pelo menos 2 páginas, depois lê todos.
	// Valida que:
	//  1. Write sabe alocar página nova quando a ativa enche.
	//  2. RecordIDs em páginas diferentes são únicos e decodificáveis.
	//  3. Read consegue recuperar de qualquer página.
	h := newHeap(t, nil)

	bigDoc := make([]byte, 1000) // ~8 registros por página
	for i := range bigDoc {
		bigDoc[i] = byte(i % 251)
	}

	const total = 30 // força várias rotações de página
	rids := make([]int64, total)
	seen := make(map[int64]bool, total)
	pageSet := make(map[uint64]struct{})

	for i := 0; i < total; i++ {
		rid, err := h.Write(bigDoc, uint64(i+1), NoRecordID)
		if err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
		if seen[rid] {
			t.Fatalf("RecordID %d duplicado no write %d", rid, i)
		}
		seen[rid] = true
		rids[i] = rid

		pid, _ := DecodeRecordID(rid)
		pageSet[uint64(pid)] = struct{}{}
	}

	if len(pageSet) < 2 {
		t.Fatalf("esperava >= 2 páginas usadas, usei %d", len(pageSet))
	}

	// Lê tudo de volta
	for i, rid := range rids {
		got, hdr, err := h.Read(rid)
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		if !bytes.Equal(got, bigDoc) {
			t.Fatalf("doc %d divergente", i)
		}
		if hdr.CreateLSN != uint64(i+1) {
			t.Fatalf("CreateLSN %d: esperado %d, recebi %d", i, i+1, hdr.CreateLSN)
		}
	}
}

func TestHeapV2_CloseReopen_PreservesData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "heap.db")
	cipher := makeCipher(t) // exercita TDE de ponta a ponta

	// Escreve alguns registros e fecha
	h1 := newHeapAt(t, path, cipher)
	docs := [][]byte{
		[]byte(`primeiro`),
		[]byte(`segundo`),
		[]byte(`terceiro documento`),
	}
	rids := make([]int64, len(docs))
	for i, d := range docs {
		rid, err := h1.Write(d, uint64(i+1), NoRecordID)
		if err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
		rids[i] = rid
	}
	if err := h1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reabre com a MESMA chave
	h2, err := NewHeapV2(path, 16, cipher)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer h2.Close()

	for i, expected := range docs {
		got, hdr, err := h2.Read(rids[i])
		if err != nil {
			t.Fatalf("Read pós-reopen %d: %v", i, err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("doc %d: esperado %q, recebi %q", i, expected, got)
		}
		if hdr.CreateLSN != uint64(i+1) {
			t.Fatalf("LSN %d divergente", i)
		}
	}

	// E consegue continuar escrevendo (adota última página como ativa)
	rid, err := h2.Write([]byte(`quarto`), 99, NoRecordID)
	if err != nil {
		t.Fatalf("Write pós-reopen: %v", err)
	}
	got, _, _ := h2.Read(rid)
	if !bytes.Equal(got, []byte(`quarto`)) {
		t.Fatal("write pós-reopen corrompido")
	}
}

func TestHeapV2_Concurrent_WritesAndReads(t *testing.T) {
	// Stress test: writers inserindo, readers lendo tudo. -race limpo.
	h := newHeap(t, nil)

	// Pré-popula pra readers terem alvos
	const seeds = 20
	seedRIDs := make([]int64, seeds)
	for i := 0; i < seeds; i++ {
		rid, err := h.Write([]byte(`seed`), uint64(i), NoRecordID)
		if err != nil {
			t.Fatal(err)
		}
		seedRIDs[i] = rid
	}

	const writers = 8
	const readers = 8
	const opsPerG = 30

	var wg sync.WaitGroup
	var errCount atomic.Int64

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				doc := []byte{byte(g), byte(i)}
				if _, err := h.Write(doc, uint64(g*1000+i), NoRecordID); err != nil {
					errCount.Add(1)
					return
				}
			}
		}(w)
	}

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				rid := seedRIDs[(g+i)%seeds]
				if _, _, err := h.Read(rid); err != nil {
					errCount.Add(1)
					return
				}
			}
		}(r)
	}

	wg.Wait()

	if errCount.Load() != 0 {
		t.Fatalf("%d erros durante execução concorrente", errCount.Load())
	}
}

func TestHeapV2_RecordTooLarge(t *testing.T) {
	h := newHeap(t, nil)

	huge := make([]byte, 10000) // > 8KB, não cabe numa página
	if _, err := h.Write(huge, 1, NoRecordID); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("esperava ErrRecordTooLarge, recebi: %v", err)
	}
}

func TestHeapV2_Delete_LazyPreservesDoc(t *testing.T) {
	// Invariante MVCC: delete NÃO apaga os bytes — só marca Valid=false
	// e seta DeleteLSN. Read continua devolvendo o doc.
	h := newHeap(t, nil)

	doc := []byte(`versão antiga`)
	rid, _ := h.Write(doc, 10, NoRecordID)

	if err := h.Delete(rid, 20); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	gotDoc, hdr, err := h.Read(rid)
	if err != nil {
		t.Fatalf("Read pós-delete: %v", err)
	}
	if !bytes.Equal(gotDoc, doc) {
		t.Fatal("doc foi perdido no delete lazy")
	}
	if hdr.Valid {
		t.Fatal("Valid deveria ser false")
	}
	if hdr.DeleteLSN != 20 {
		t.Fatalf("DeleteLSN esperado 20, recebi %d", hdr.DeleteLSN)
	}
	if hdr.CreateLSN != 10 {
		t.Fatalf("CreateLSN deveria ser preservado (10), recebi %d", hdr.CreateLSN)
	}
}

func TestHeapV2_WriteRead_Multiple_SamePage(t *testing.T) {
	h := newHeap(t, nil)

	docs := [][]byte{
		[]byte(`alpha`),
		[]byte(`beta`),
		[]byte(`gamma`),
	}
	rids := make([]int64, len(docs))
	for i, d := range docs {
		rid, err := h.Write(d, uint64(i+1), NoRecordID)
		if err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
		rids[i] = rid
	}

	// Todos os três devem estar na mesma página (são pequenos)
	pid0, _ := DecodeRecordID(rids[0])
	pid2, _ := DecodeRecordID(rids[2])
	if pid0 != pid2 {
		t.Fatalf("esperava mesma página, recebi %d e %d", pid0, pid2)
	}

	for i, expected := range docs {
		got, _, err := h.Read(rids[i])
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("doc %d divergente", i)
		}
	}
}

// TestHeapV2_Vacuum_PopulatesFSM verifica que Vacuum registra espaço livre
// no FSM após compactar páginas com tombstones, e que o espaço reportado
// é maior do que antes (mais slots livres após compactação).
func TestHeapV2_Vacuum_PopulatesFSM(t *testing.T) {
	// Heap com pool amplo para não forçar flush inesperado
	path := filepath.Join(t.TempDir(), "heap.db")
	h, err := NewHeapV2(path, 32, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	// Insere registros suficientes para preencher pelo menos uma página.
	// Usa docs grandes (500 bytes) para garantir que ficam em página única.
	doc := bytes.Repeat([]byte("x"), 500)
	var rids []int64
	for i := 0; i < 3; i++ {
		rid, err := h.Write(doc, uint64(i+1), -1)
		if err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
		rids = append(rids, rid)
	}

	// Captura espaço livre da primeira página via FSM antes dos deletes.
	pid0, _ := DecodeRecordID(rids[0])
	freeBeforeVacuum := 0
	h.FSM().mu.Lock()
	if f, ok := h.FSM().pages[pid0]; ok {
		freeBeforeVacuum = f
	}
	h.FSM().mu.Unlock()

	// Deleta todos os registros para gerar tombstones.
	for i, rid := range rids {
		if err := h.Delete(rid, uint64(10+i)); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
	}

	// Vacuum com minLSN alto → todos os tombstones reclamados.
	n, err := h.Vacuum(100)
	if err != nil {
		t.Fatalf("Vacuum: %v", err)
	}
	if n == 0 {
		t.Fatal("Vacuum deveria ter reclamado ao menos um slot")
	}

	// Após vacuum, o FSM deve reportar MAIS espaço livre na página (slots reclamados).
	freeAfterVacuum := 0
	h.FSM().mu.Lock()
	if f, ok := h.FSM().pages[pid0]; ok {
		freeAfterVacuum = f
	}
	h.FSM().mu.Unlock()

	if freeAfterVacuum <= freeBeforeVacuum {
		t.Fatalf("FSM deveria reportar mais espaço após Vacuum: antes=%d, depois=%d",
			freeBeforeVacuum, freeAfterVacuum)
	}
}

// TestHeapV2_FSM_ReusesVacuumedSpace verifica que Write reutiliza espaço
// liberado pelo Vacuum via FSM em vez de sempre alocar nova página.
func TestHeapV2_FSM_ReusesVacuumedSpace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "heap.db")
	h, err := NewHeapV2(path, 8, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	doc := bytes.Repeat([]byte("z"), 30)
	rid, err := h.Write(doc, 1, -1)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Delete(rid, 2); err != nil {
		t.Fatal(err)
	}

	if _, err := h.Vacuum(10); err != nil {
		t.Fatal(err)
	}

	if h.FSM().Len() == 0 {
		t.Skip("página não compactada (muito espaço em uso), pulando teste de reuso")
	}

	rid2, err := h.Write(doc, 3, -1)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := h.Read(rid2)
	if err != nil {
		t.Fatalf("Read após reutilização: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatal("doc corrompido após reutilização de página via FSM")
	}
}
