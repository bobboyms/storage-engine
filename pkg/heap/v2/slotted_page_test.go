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
		t.Fatalf("page freshly initialized should have 0 slots, tem %d", got)
	}
	if got := sp.NumValid(); got != 0 {
		t.Fatalf("page freshly initialized should have 0 slots válidos, tem %d", got)
	}

	// Free space = BodySize - SlottedHeaderSize (nenhum slot, nenhum record)
	expected := pagestore.BodySize - SlottedHeaderSize
	if got := sp.FreeSpace(); got != expected {
		t.Fatalf("free space: expected %d, got %d", expected, got)
	}
}

func TestSlottedPage_MarkDeleted_PreservesEverythingElse(t *testing.T) {
	_, sp := newSlottedPage(t)

	doc := []byte(`record original`)
	slotID, _ := sp.Insert(RecordHeader{
		Valid:        true,
		CreateLSN:    10,
		DeleteLSN:    0,
		PrevRecordID: 42, // simula chain MVCC
	}, doc)

	// Delete LAZY: marca invalid com DeleteLSN, bytes ficam no lugar
	if err := sp.MarkDeleted(slotID, 20); err != nil {
		t.Fatalf("MarkDeleted: %v", err)
	}

	if sp.NumValid() != 0 {
		t.Fatalf("NumValid should be 0 after delete, é %d", sp.NumValid())
	}
	if sp.NumSlots() != 1 {
		t.Fatal("slot cannot desaparecer (SlotIDs são estáveis)")
	}

	// Read ainda devolve o record — invariante crítico pro MVCC:
	// uma transação antiga (LSN < DeleteLSN) precisa conseguir ver a versão.
	gotDoc, gotHdr, err := sp.Read(slotID)
	if err != nil {
		t.Fatalf("Read after delete: %v", err)
	}
	if string(gotDoc) != string(doc) {
		t.Fatal("doc foi perdido no delete lazy")
	}
	if gotHdr.Valid {
		t.Fatal("Valid should be false after MarkDeleted")
	}
	if gotHdr.DeleteLSN != 20 {
		t.Fatalf("DeleteLSN expected 20, got %d", gotHdr.DeleteLSN)
	}
	if gotHdr.CreateLSN != 10 {
		t.Fatalf("CreateLSN should be preservado (10), got %d", gotHdr.CreateLSN)
	}
	if gotHdr.PrevRecordID != 42 {
		t.Fatalf("PrevRecordID should be preservado (42), got %d", gotHdr.PrevRecordID)
	}
}

func TestSlottedPage_MarkDeleted_InvalidSlot(t *testing.T) {
	_, sp := newSlottedPage(t)
	if err := sp.MarkDeleted(999, 1); err == nil {
		t.Fatal("expected erro em slot inexistsnte")
	}
}

func TestSlottedPage_Iterate_IncludesInvalidSlots(t *testing.T) {
	// Invariante crítica pro vacuum: iterar TODOS os slots (válidos ou
	// not) na ordem do SlotID. Vacuum precisa ver tombstones pra poder
	// decidir se reclaim.
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
		t.Fatalf("expected 3 slots visitados, got %d", len(visited))
	}
	// Ordem e content
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
			t.Fatalf("slot %d: expected %+v, got %+v", i, e, visited[i])
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
		t.Fatalf("expected erro 'stop', got %v", err)
	}
	if count != 2 {
		t.Fatalf("expected parar na segunda iteração (count=2), got %d", count)
	}
}

func TestSlottedPage_ErrPageFull(t *testing.T) {
	_, sp := newSlottedPage(t)

	// Insere records até esgotar o espaço.
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
		t.Fatalf("expected ErrPageFull, got: %v", lastErr)
	}
	if inserted < 5 {
		t.Fatalf("inseriu muito pouco (%d) — algo está errado no cálculo de espaço", inserted)
	}

	// Depois de cheio, ainda é possível ler todos os records inseridos
	for i := 0; i < inserted; i++ {
		if _, _, err := sp.Read(uint16(i)); err != nil {
			t.Fatalf("Read %d after full: %v", i, err)
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
		// SlotIDs mustm ser monotônicos e únicos (invariante MVCC)
		if id != uint16(i) {
			t.Fatalf("expected slotID %d, got %d", i, id)
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
			t.Fatalf("slot %d: CreateLSN expected %d, got %d", i, i+1, gotHdr.CreateLSN)
		}
	}

	if sp.NumSlots() != 3 || sp.NumValid() != 3 {
		t.Fatalf("expected NumSlots=3 NumValid=3, got %d/%d", sp.NumSlots(), sp.NumValid())
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
		t.Fatalf("primeiro slotID should be 0, got %d", slotID)
	}
	if sp.NumSlots() != 1 || sp.NumValid() != 1 {
		t.Fatalf("expected NumSlots=1 NumValid=1, got %d/%d", sp.NumSlots(), sp.NumValid())
	}

	gotDoc, gotHdr, err := sp.Read(slotID)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(gotDoc) != string(doc) {
		t.Fatalf("doc divergente: expected %q, got %q", doc, gotDoc)
	}
	if gotHdr != hdr {
		t.Fatalf("header divergente: expected %+v, got %+v", hdr, gotHdr)
	}
}
