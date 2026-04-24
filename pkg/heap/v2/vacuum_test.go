package v2

import (
	"errors"
	"testing"
)

func TestSlottedPage_Compact_EmptyPage_NoOp(t *testing.T) {
	_, sp := newSlottedPage(t)
	n, err := sp.Compact(100)
	if err != nil {
		t.Fatalf("Compact em página vazia: %v", err)
	}
	if n != 0 {
		t.Fatalf("esperava 0 vacuumados em página vazia, recebi %d", n)
	}
}

func TestSlottedPage_Compact_NoTombstones_NoOp(t *testing.T) {
	_, sp := newSlottedPage(t)
	for i := 0; i < 3; i++ {
		sp.Insert(RecordHeader{Valid: true, CreateLSN: uint64(i + 1), PrevRecordID: NoRecordID}, []byte("vivo"))
	}
	n, err := sp.Compact(9999)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("esperava 0 vacuumados sem tombstones, recebi %d", n)
	}
	if sp.NumSlots() != 3 || sp.NumValid() != 3 {
		t.Fatalf("esperado 3/3, recebi %d/%d", sp.NumSlots(), sp.NumValid())
	}
}

func TestSlottedPage_Compact_ReclaimsTombstonesBelowMinLSN(t *testing.T) {
	_, sp := newSlottedPage(t)

	// Insere 4 registros, deleta 2 deles (slots 1 e 2)
	s0, _ := sp.Insert(RecordHeader{Valid: true, CreateLSN: 10, PrevRecordID: NoRecordID}, []byte("vivo-0"))
	s1, _ := sp.Insert(RecordHeader{Valid: true, CreateLSN: 20, PrevRecordID: NoRecordID}, []byte("morto-1"))
	s2, _ := sp.Insert(RecordHeader{Valid: true, CreateLSN: 30, PrevRecordID: NoRecordID}, []byte("morto-2"))
	s3, _ := sp.Insert(RecordHeader{Valid: true, CreateLSN: 40, PrevRecordID: NoRecordID}, []byte("vivo-3"))

	_ = sp.MarkDeleted(s1, 50) // DeleteLSN=50
	_ = sp.MarkDeleted(s2, 60) // DeleteLSN=60

	// minLSN=70: ambos tombstones podem ir (50, 60 <= 70)
	freeBefore := sp.FreeSpace()
	n, err := sp.Compact(70)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("esperava 2 vacuumados, recebi %d", n)
	}

	// Sobreviventes ainda legíveis com SlotID estável
	doc, hdr, err := sp.Read(s0)
	if err != nil || string(doc) != "vivo-0" || hdr.CreateLSN != 10 {
		t.Fatalf("s0 deveria sobreviver: %v %q %d", err, doc, hdr.CreateLSN)
	}
	doc, hdr, err = sp.Read(s3)
	if err != nil || string(doc) != "vivo-3" || hdr.CreateLSN != 40 {
		t.Fatalf("s3 deveria sobreviver: %v %q %d", err, doc, hdr.CreateLSN)
	}

	// Vacuumados retornam ErrVacuumed
	if _, _, err := sp.Read(s1); !errors.Is(err, ErrVacuumed) {
		t.Fatalf("s1: esperava ErrVacuumed, recebi %v", err)
	}
	if _, _, err := sp.Read(s2); !errors.Is(err, ErrVacuumed) {
		t.Fatalf("s2: esperava ErrVacuumed, recebi %v", err)
	}

	// Espaço foi reclamado
	if sp.FreeSpace() <= freeBefore {
		t.Fatalf("FreeSpace não aumentou: antes=%d depois=%d", freeBefore, sp.FreeSpace())
	}

	// Slot dir não encolheu — SlotIDs continuam estáveis (s3 ainda é slotID=3)
	if sp.NumSlots() != 4 {
		t.Fatalf("NumSlots deveria continuar em 4 (SlotIDs estáveis), é %d", sp.NumSlots())
	}
	if sp.NumValid() != 2 {
		t.Fatalf("NumValid esperado 2, recebi %d", sp.NumValid())
	}
}

func TestSlottedPage_Compact_RespeitsMinLSN(t *testing.T) {
	// Tombstones NOVOS (DeleteLSN > minLSN) NÃO podem ser vacuumados —
	// uma transação antiga pode precisar ler essas versões.
	_, sp := newSlottedPage(t)

	s0, _ := sp.Insert(RecordHeader{Valid: true, CreateLSN: 10, PrevRecordID: NoRecordID}, []byte("vivo"))
	s1, _ := sp.Insert(RecordHeader{Valid: true, CreateLSN: 20, PrevRecordID: NoRecordID}, []byte("morto-novo"))
	_ = sp.MarkDeleted(s1, 100) // deleted at LSN 100

	// minLSN=50: tombstone em LSN 100 é "futuro demais" pra vacuumar
	n, err := sp.Compact(50)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("tombstone jovem não deveria ser vacuumado com minLSN=50, vacuumou %d", n)
	}

	// Tanto s0 quanto s1 continuam legíveis (mesmo s1 sendo inválido)
	if _, _, err := sp.Read(s0); err != nil {
		t.Fatalf("s0: %v", err)
	}
	doc, hdr, err := sp.Read(s1)
	if err != nil {
		t.Fatalf("s1 deveria continuar legível (valid=false mas dados intactos): %v", err)
	}
	if string(doc) != "morto-novo" || hdr.Valid {
		t.Fatalf("s1 estado errado: doc=%q valid=%v", doc, hdr.Valid)
	}
}

func TestSlottedPage_Compact_AllowsFurtherInserts(t *testing.T) {
	_, sp := newSlottedPage(t)

	// Enche a página com registros médios, mata metade
	for i := 0; i < 6; i++ {
		sp.Insert(RecordHeader{Valid: true, CreateLSN: uint64(i + 1), PrevRecordID: NoRecordID}, make([]byte, 500))
	}
	for i := uint16(0); i < 3; i++ {
		_ = sp.MarkDeleted(i, 100)
	}

	_, err := sp.Compact(200)
	if err != nil {
		t.Fatal(err)
	}

	// Agora deveria caber novos registros no espaço reclamado
	newSlot, err := sp.Insert(RecordHeader{Valid: true, CreateLSN: 999, PrevRecordID: NoRecordID}, make([]byte, 500))
	if err != nil {
		t.Fatalf("insert pós-compact falhou: %v", err)
	}
	if newSlot != 6 {
		// SlotIDs 0..5 existem (3 vacuumados + 3 vivos); novo deve ser 6
		t.Fatalf("novo slot esperado 6 (SlotIDs não são reusados), recebi %d", newSlot)
	}

	// Sobreviventes continuam legíveis
	for i := uint16(3); i < 6; i++ {
		if _, _, err := sp.Read(i); err != nil {
			t.Fatalf("slot %d: %v", i, err)
		}
	}
}

func TestHeapV2_Vacuum_AggregatesPages(t *testing.T) {
	// Cria tombstones em duas páginas, chama Vacuum, verifica total.
	h := newHeap(t, nil)

	// Vai criar registros grandes pra forçar múltiplas páginas
	big := make([]byte, 2000)

	var rids []int64
	for i := 0; i < 12; i++ {
		rid, err := h.Write(big, uint64(i+1), NoRecordID)
		if err != nil {
			t.Fatal(err)
		}
		rids = append(rids, rid)
	}

	// Deleta 6 deles (metade)
	for i := 0; i < 6; i++ {
		if err := h.Delete(rids[i], uint64(100+i)); err != nil {
			t.Fatal(err)
		}
	}

	// Confere que estão em pelo menos 2 páginas
	pages := map[uint64]bool{}
	for _, rid := range rids {
		pid, _ := DecodeRecordID(rid)
		pages[uint64(pid)] = true
	}
	if len(pages) < 2 {
		t.Fatalf("precisa de pelo menos 2 páginas, tem %d", len(pages))
	}

	// Vacuum com minLSN bem alto (todos os tombstones são elegíveis)
	total, err := h.Vacuum(9999)
	if err != nil {
		t.Fatal(err)
	}
	if total != 6 {
		t.Fatalf("esperava 6 vacuumados, recebi %d", total)
	}

	// Sobreviventes continuam legíveis
	for i := 6; i < 12; i++ {
		_, _, err := h.Read(rids[i])
		if err != nil {
			t.Fatalf("sobrevivente %d: %v", i, err)
		}
	}
	// Vacuumados retornam ErrVacuumed
	for i := 0; i < 6; i++ {
		if _, _, err := h.Read(rids[i]); !errors.Is(err, ErrVacuumed) {
			t.Fatalf("rid %d deveria estar vacuumado: %v", i, err)
		}
	}
}

func TestHeapV2_MVCC_ChainWalkTerminatesAtVacuumed(t *testing.T) {
	// Cadeia: v1 → v2 → v3. Se v1 é vacuumado, um walk a partir de v3
	// deve chegar em v1 e receber ErrVacuumed — equivale a fim de cadeia.
	h := newHeap(t, nil)

	v1, _ := h.Write([]byte("antiga"), 10, NoRecordID)
	v2, _ := h.Write([]byte("meio"), 20, v1)
	v3, _ := h.Write([]byte("recente"), 30, v2)

	// Marca v1 como deletado e vacuuma
	_ = h.Delete(v1, 40)
	n, err := h.Vacuum(100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("esperava 1 vacuumado, recebi %d", n)
	}

	// Walk a partir de v3
	//   v3 (ok) → v2 (ok) → v1 (ErrVacuumed, tratado como fim)
	rid := v3
	visited := 0
	for rid != NoRecordID {
		_, hdr, err := h.Read(rid)
		if errors.Is(err, ErrVacuumed) {
			// Fim da cadeia — o que sobrou foi vacuumado
			break
		}
		if err != nil {
			t.Fatalf("Read inesperado no passo %d: %v", visited, err)
		}
		visited++
		rid = hdr.PrevRecordID
	}
	if visited != 2 {
		t.Fatalf("esperava 2 versões visitadas antes do vacuumed, recebi %d", visited)
	}
}

func TestSlottedPage_Read_VacuumedSlot_ReturnsErrVacuumed(t *testing.T) {
	_, sp := newSlottedPage(t)

	id, _ := sp.Insert(RecordHeader{Valid: true, CreateLSN: 1, PrevRecordID: NoRecordID}, []byte("x"))
	// Simula slot vacuumado: length=0 no slot dir
	sp.writeSlot(id, 0, 0)

	_, _, err := sp.Read(id)
	if !errors.Is(err, ErrVacuumed) {
		t.Fatalf("esperava ErrVacuumed, recebi: %v", err)
	}
}
