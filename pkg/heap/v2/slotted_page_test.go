package v2

import (
	"errors"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// Helper: cria uma Page zerada e inicializa como slotted.
func newSlottedPage(t *testing.T) (*pagestore.Page, *SlottedPage) {
	t.Helper()
	var p pagestore.Page
	sp := InitSlottedPage(&p, pagestore.BodySize)
	return &p, sp
}

func TestSlottedPage_InitEmpty(t *testing.T) {
	_, sp := newSlottedPage(t)

	if got := sp.NumSlots(); got != 0 {
		t.Fatalf("página recém-init deveria ter 0 slots, tem %d", got)
	}
	if got := sp.NumValid(); got != 0 {
		t.Fatalf("página recém-init deveria ter 0 slots válidos, tem %d", got)
	}

	// Free space = BodySize - SlottedHeaderSize (nenhum slot, nenhum registro)
	expected := pagestore.BodySize - SlottedHeaderSize
	if got := sp.FreeSpace(); got != expected {
		t.Fatalf("free space: esperado %d, recebi %d", expected, got)
	}
}

func TestSlottedPage_MarkDeleted_PreservesEverythingElse(t *testing.T) {
	_, sp := newSlottedPage(t)

	doc := []byte(`registro original`)
	slotID, _ := sp.Insert(RecordHeader{
		Valid:        true,
		CreateLSN:    10,
		DeleteLSN:    0,
		PrevRecordID: 42, // simula chain MVCC
	}, doc)

	// Delete LAZY: marca inválido com DeleteLSN, bytes ficam no lugar
	if err := sp.MarkDeleted(slotID, 20); err != nil {
		t.Fatalf("MarkDeleted: %v", err)
	}

	if sp.NumValid() != 0 {
		t.Fatalf("NumValid deveria ser 0 após delete, é %d", sp.NumValid())
	}
	if sp.NumSlots() != 1 {
		t.Fatal("slot não pode desaparecer (SlotIDs são estáveis)")
	}

	// Read ainda devolve o registro — invariante crítico pro MVCC:
	// uma transação antiga (LSN < DeleteLSN) precisa conseguir ver a versão.
	gotDoc, gotHdr, err := sp.Read(slotID)
	if err != nil {
		t.Fatalf("Read pós-delete: %v", err)
	}
	if string(gotDoc) != string(doc) {
		t.Fatal("doc foi perdido no delete lazy")
	}
	if gotHdr.Valid {
		t.Fatal("Valid deveria ser false após MarkDeleted")
	}
	if gotHdr.DeleteLSN != 20 {
		t.Fatalf("DeleteLSN esperado 20, recebi %d", gotHdr.DeleteLSN)
	}
	if gotHdr.CreateLSN != 10 {
		t.Fatalf("CreateLSN deveria ser preservado (10), recebi %d", gotHdr.CreateLSN)
	}
	if gotHdr.PrevRecordID != 42 {
		t.Fatalf("PrevRecordID deveria ser preservado (42), recebi %d", gotHdr.PrevRecordID)
	}
}

func TestSlottedPage_MarkDeleted_InvalidSlot(t *testing.T) {
	_, sp := newSlottedPage(t)
	if err := sp.MarkDeleted(999, 1); err == nil {
		t.Fatal("esperava erro em slot inexistente")
	}
}

func TestSlottedPage_Iterate_IncludesInvalidSlots(t *testing.T) {
	// Invariante crítica pro vacuum: iterar TODOS os slots (válidos ou
	// não) na ordem do SlotID. Vacuum precisa ver tombstones pra poder
	// decidir se reclama.
	_, sp := newSlottedPage(t)

	_, _ = sp.Insert(RecordHeader{Valid: true, CreateLSN: 1, PrevRecordID: NoRecordID}, []byte("um"))
	_, _ = sp.Insert(RecordHeader{Valid: true, CreateLSN: 2, PrevRecordID: NoRecordID}, []byte("dois"))
	_, _ = sp.Insert(RecordHeader{Valid: true, CreateLSN: 3, PrevRecordID: NoRecordID}, []byte("tres"))

	// Mata o slot 1 — mantém 0 e 2
	_ = sp.MarkDeleted(1, 99)

	var visited []struct {
		slotID uint16
		valid  bool
		doc    string
	}
	err := sp.Iterate(func(slotID uint16, rh RecordHeader, doc []byte) error {
		visited = append(visited, struct {
			slotID uint16
			valid  bool
			doc    string
		}{slotID, rh.Valid, string(doc)})
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate: %v", err)
	}

	if len(visited) != 3 {
		t.Fatalf("esperava 3 slots visitados, recebi %d", len(visited))
	}
	// Ordem e conteúdo
	expected := []struct {
		slotID uint16
		valid  bool
		doc    string
	}{
		{0, true, "um"},
		{1, false, "dois"}, // invalid mas doc preservado (MVCC)
		{2, true, "tres"},
	}
	for i, e := range expected {
		if visited[i] != e {
			t.Fatalf("slot %d: esperado %+v, recebi %+v", i, e, visited[i])
		}
	}
}

func TestSlottedPage_Iterate_EarlyStop(t *testing.T) {
	_, sp := newSlottedPage(t)
	_, _ = sp.Insert(RecordHeader{Valid: true, CreateLSN: 1, PrevRecordID: NoRecordID}, []byte("a"))
	_, _ = sp.Insert(RecordHeader{Valid: true, CreateLSN: 2, PrevRecordID: NoRecordID}, []byte("b"))
	_, _ = sp.Insert(RecordHeader{Valid: true, CreateLSN: 3, PrevRecordID: NoRecordID}, []byte("c"))

	stop := errors.New("stop")
	count := 0
	err := sp.Iterate(func(slotID uint16, rh RecordHeader, doc []byte) error {
		count++
		if slotID == 1 {
			return stop
		}
		return nil
	})
	if err != stop {
		t.Fatalf("esperava erro 'stop', recebi %v", err)
	}
	if count != 2 {
		t.Fatalf("esperava parar na segunda iteração (count=2), recebi %d", count)
	}
}

func TestSlottedPage_ErrPageFull(t *testing.T) {
	_, sp := newSlottedPage(t)

	// Insere registros até esgotar o espaço.
	big := make([]byte, 1000) // record total: 25 + 1000 + 4 slot = 1029 por insert
	var inserted int
	var lastErr error
	for i := 0; i < 100; i++ {
		if _, err := sp.Insert(RecordHeader{Valid: true, CreateLSN: uint64(i), PrevRecordID: NoRecordID}, big); err != nil {
			lastErr = err
			break
		}
		inserted++
	}

	if !errors.Is(lastErr, ErrPageFull) {
		t.Fatalf("esperava ErrPageFull, recebi: %v", lastErr)
	}
	if inserted < 5 {
		t.Fatalf("inseriu muito pouco (%d) — algo está errado no cálculo de espaço", inserted)
	}

	// Depois de cheio, ainda é possível ler todos os registros inseridos
	for i := 0; i < inserted; i++ {
		if _, _, err := sp.Read(uint16(i)); err != nil {
			t.Fatalf("Read %d pós-full: %v", i, err)
		}
	}
}

func TestSlottedPage_InsertAndRead_Multiple(t *testing.T) {
	_, sp := newSlottedPage(t)

	docs := [][]byte{
		[]byte(`{"id":1}`),
		[]byte(`{"id":2,"extra":"longer payload"}`),
		[]byte(`tiny`),
	}
	slotIDs := make([]uint16, len(docs))

	for i, doc := range docs {
		id, err := sp.Insert(RecordHeader{Valid: true, CreateLSN: uint64(i + 1), PrevRecordID: NoRecordID}, doc)
		if err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
		// SlotIDs devem ser monotônicos e únicos (invariante MVCC)
		if id != uint16(i) {
			t.Fatalf("esperava slotID %d, recebi %d", i, id)
		}
		slotIDs[i] = id
	}

	// Lê em ordem reversa e fora de ordem — SlotIDs ficam estáveis
	for _, i := range []int{2, 0, 1} {
		gotDoc, gotHdr, err := sp.Read(slotIDs[i])
		if err != nil {
			t.Fatalf("Read slot %d: %v", slotIDs[i], err)
		}
		if string(gotDoc) != string(docs[i]) {
			t.Fatalf("slot %d: doc divergente", i)
		}
		if gotHdr.CreateLSN != uint64(i+1) {
			t.Fatalf("slot %d: CreateLSN esperado %d, recebi %d", i, i+1, gotHdr.CreateLSN)
		}
	}

	if sp.NumSlots() != 3 || sp.NumValid() != 3 {
		t.Fatalf("esperado NumSlots=3 NumValid=3, recebi %d/%d", sp.NumSlots(), sp.NumValid())
	}
}

func TestSlottedPage_InsertAndRead_Single(t *testing.T) {
	_, sp := newSlottedPage(t)

	doc := []byte(`{"id":1,"name":"alice"}`)
	hdr := RecordHeader{
		Valid:        true,
		CreateLSN:    100,
		DeleteLSN:    0,
		PrevRecordID: NoRecordID,
	}

	slotID, err := sp.Insert(hdr, doc)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if slotID != 0 {
		t.Fatalf("primeiro slotID deveria ser 0, recebi %d", slotID)
	}
	if sp.NumSlots() != 1 || sp.NumValid() != 1 {
		t.Fatalf("esperado NumSlots=1 NumValid=1, recebi %d/%d", sp.NumSlots(), sp.NumValid())
	}

	gotDoc, gotHdr, err := sp.Read(slotID)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(gotDoc) != string(doc) {
		t.Fatalf("doc divergente: esperado %q, recebi %q", doc, gotDoc)
	}
	if gotHdr != hdr {
		t.Fatalf("header divergente: esperado %+v, recebi %+v", hdr, gotHdr)
	}
}
