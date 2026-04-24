package storage_test

import (
	"fmt"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
	"path/filepath"
	"testing"
)

// TestRecovery_CrashMidWrite_RecoversAllCommittedWrites simula crash
// mid-operação: engine writes → WAL fsync → process "morre" antes de
// flushar tree/heap → reopen → Recover → todos os writes fsync'ados
// devem estar visíveis.
//
// Este é o teste MÃE de produção: "confirma que dados que o Put
// retornou como successful estão lá depois de kill -9".
func TestRecovery_CrashMidWrite_RecoversAllCommittedWrites(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.v2")
	btreePath := filepath.Join(tmpDir, "idx.btree.v2")

	const N = 100

	// FASE 1: escrever N entries e "crashar" (só fecha WAL, não tree/heap)
	{
		hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath, nil)
		idxTree, _ := storage.NewBTreeForIndex(
			storage.BTreeFormatV2, true, storage.TypeInt, btreePath, nil,
		)
		tm := storage.NewTableMenager()
		err := tm.NewTable("t", []storage.Index{
			{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree},
		}, 3, hm)
		if err != nil {
			t.Fatal(err)
		}

		// WAL com SyncEveryWrite (default) — cada Put fsync antes de retornar
		ww, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
		se, err := storage.NewStorageEngine(tm, ww)
		if err != nil {
			ww.Close()
			t.Fatal(err)
		}

		// Escreve N entries
		for i := 1; i <= N; i++ {
			err := se.Put("t", "id", types.IntKey(int64(i)), fmt.Sprintf(`{"id":%d}`, i))
			if err != nil {
				t.Fatalf("Put %d: %v", i, err)
			}
		}

		// "Crash": fecha SÓ o WAL (pra garantir fsync dos entries).
		// NÃO chamamos se.Close() nem hm.Close() nem idxTree.Close() —
		// simula kill -9 onde BufferPool não é flushado.
		if err := ww.Close(); err != nil {
			t.Fatalf("WAL close: %v", err)
		}
	}

	// FASE 2: reopen e recover
	hm2, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath, nil)
	idxTree2, _ := storage.NewBTreeForIndex(
		storage.BTreeFormatV2, true, storage.TypeInt, btreePath, nil,
	)
	tm2 := storage.NewTableMenager()
	tm2.NewTable("t", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree2},
	}, 3, hm2)

	ww2, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se2, err := storage.NewStorageEngine(tm2, ww2)
	if err != nil {
		ww2.Close()
		t.Fatal(err)
	}
	defer se2.Close()

	// Recovery
	if err := se2.Recover(walPath); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// FASE 3: valida — TODOS os N writes devem estar visíveis
	for i := 1; i <= N; i++ {
		doc, found, err := se2.Get("t", "id", types.IntKey(int64(i)))
		if err != nil {
			t.Fatalf("Get %d pós-recovery: %v", i, err)
		}
		if !found {
			t.Errorf("key %d MISSING após recovery — dado perdido!", i)
			continue
		}
		want := fmt.Sprintf(`{"id":%d}`, i)
		if doc != want {
			t.Errorf("key %d: esperado %q, recebi %q (corrupção)", i, want, doc)
		}
	}
}

// TestRecovery_IdempotentMultipleCalls: chamar Recover duas vezes em
// sequência não deve duplicar nada nem corromper estado.
func TestRecovery_IdempotentMultipleCalls(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.v2")
	btreePath := filepath.Join(tmpDir, "idx.btree.v2")

	// Setup inicial
	hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath, nil)
	idxTree, _ := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, btreePath, nil)
	tm := storage.NewTableMenager()
	tm.NewTable("t", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree},
	}, 3, hm)

	ww, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, _ := storage.NewStorageEngine(tm, ww)

	for i := 1; i <= 10; i++ {
		se.Put("t", "id", types.IntKey(int64(i)), fmt.Sprintf(`{"id":%d}`, i))
	}
	se.Close()

	// Reabre
	hm2, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath, nil)
	idxTree2, _ := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, btreePath, nil)
	tm2 := storage.NewTableMenager()
	tm2.NewTable("t", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree2},
	}, 3, hm2)

	ww2, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se2, _ := storage.NewStorageEngine(tm2, ww2)
	defer se2.Close()

	// Recover 2x
	if err := se2.Recover(walPath); err != nil {
		t.Fatalf("Recover 1: %v", err)
	}
	if err := se2.Recover(walPath); err != nil {
		t.Fatalf("Recover 2 (should be idempotent): %v", err)
	}

	// Todas as keys devem estar visíveis exatamente uma vez
	for i := 1; i <= 10; i++ {
		doc, found, _ := se2.Get("t", "id", types.IntKey(int64(i)))
		if !found {
			t.Errorf("key %d missing", i)
		}
		want := fmt.Sprintf(`{"id":%d}`, i)
		if doc != want {
			t.Errorf("key %d: esperado %q, recebi %q", i, want, doc)
		}
	}
}

// TestRecovery_WithoutRecoverDataIsLost: prova por contradição que
// Recover NÃO é opcional em produção. Sem ele, após crash, queries
// contra o engine retornam "not found" pra dados que o Put confirmou.
//
// Este teste documenta o "pegadinha" que motiva NewProductionStorageEngine
// (que chama Recover automaticamente).
func TestRecovery_WithoutRecoverDataIsLost(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.v2")
	btreePath := filepath.Join(tmpDir, "idx.btree.v2")

	// FASE 1: escreve 5 entries, "crasha" (só fecha WAL)
	{
		hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath, nil)
		idxTree, _ := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, btreePath, nil)
		tm := storage.NewTableMenager()
		tm.NewTable("t", []storage.Index{
			{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree},
		}, 3, hm)

		ww, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
		se, _ := storage.NewStorageEngine(tm, ww)

		for i := 1; i <= 5; i++ {
			se.Put("t", "id", types.IntKey(int64(i)), fmt.Sprintf(`{"id":%d}`, i))
		}
		ww.Close() // crash: WAL fsync'd, mas tree/heap em BufferPool
	}

	// FASE 2: reopen SEM Recover (uso errado em produção)
	hm2, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath, nil)
	idxTree2, _ := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, btreePath, nil)
	tm2 := storage.NewTableMenager()
	tm2.NewTable("t", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree2},
	}, 3, hm2)

	ww2, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se2, _ := storage.NewStorageEngine(tm2, ww2) // ← NÃO chama Recover
	defer se2.Close()

	// Sem Recover, TREE está vazia no disco (BufferPool do processo
	// anterior sumiu). Queries retornam "not found". Dados "perdidos"
	// do ponto de vista do engine, mesmo que o WAL tenha tudo.
	lost := 0
	for i := 1; i <= 5; i++ {
		_, found, _ := se2.Get("t", "id", types.IntKey(int64(i)))
		if !found {
			lost++
		}
	}
	if lost == 0 {
		t.Skip("auto-scan LSN ou eviction do BufferPool tornou os dados " +
			"visíveis sem Recover — teste não aplicável neste cenário")
	}
	// Chega aqui = comportamento esperado: dados aparentemente perdidos
	// porque Recover não foi chamado.
}

// TestProductionStorageEngine_AutoRecovery: o construtor de produção
// chama Recover automaticamente. Depois do "crash", abrir via
// NewProductionStorageEngine restaura os dados sem intervenção manual.
func TestProductionStorageEngine_AutoRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.v2")
	btreePath := filepath.Join(tmpDir, "idx.btree.v2")

	// FASE 1: escreve, crasha
	{
		hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath, nil)
		idxTree, _ := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, btreePath, nil)
		tm := storage.NewTableMenager()
		tm.NewTable("t", []storage.Index{
			{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree},
		}, 3, hm)

		ww, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
		se, _ := storage.NewProductionStorageEngine(tm, ww)

		for i := 1; i <= 50; i++ {
			se.Put("t", "id", types.IntKey(int64(i)), fmt.Sprintf(`{"id":%d}`, i))
		}
		ww.Close() // crash
	}

	// FASE 2: reopen VIA API de produção — faz auto-recovery
	hm2, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath, nil)
	idxTree2, _ := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, btreePath, nil)
	tm2 := storage.NewTableMenager()
	tm2.NewTable("t", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree2},
	}, 3, hm2)

	ww2, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se2, err := storage.NewProductionStorageEngine(tm2, ww2)
	if err != nil {
		t.Fatalf("NewProductionStorageEngine: %v", err)
	}
	defer se2.Close()

	// Todas as 50 keys devem estar visíveis sem chamar nada extra
	for i := 1; i <= 50; i++ {
		doc, found, _ := se2.Get("t", "id", types.IntKey(int64(i)))
		if !found {
			t.Errorf("key %d MISSING após NewProductionStorageEngine — auto-recovery falhou", i)
			continue
		}
		want := fmt.Sprintf(`{"id":%d}`, i)
		if doc != want {
			t.Errorf("key %d corrupção: %q", i, doc)
		}
	}
}

// TestProductionStorageEngine_RejectsNilWAL: produção exige WAL.
func TestProductionStorageEngine_RejectsNilWAL(t *testing.T) {
	tm := storage.NewTableMenager()
	_, err := storage.NewProductionStorageEngine(tm, nil)
	if err == nil {
		t.Fatal("NewProductionStorageEngine deveria rejeitar WAL=nil")
	}
}

// TestRecovery_EmptyWAL: engine com WAL vazio (nunca escreveu) não
// deve quebrar no Recover.
func TestRecovery_EmptyWAL(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.v2")
	btreePath := filepath.Join(tmpDir, "idx.btree.v2")

	hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath, nil)
	idxTree, _ := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, btreePath, nil)
	tm := storage.NewTableMenager()
	tm.NewTable("t", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree},
	}, 3, hm)

	ww, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, err := storage.NewStorageEngine(tm, ww)
	if err != nil {
		ww.Close()
		t.Fatal(err)
	}
	defer se.Close()

	// WAL vazio (nada foi escrito ainda) — Recover deve ser no-op
	if err := se.Recover(walPath); err != nil {
		t.Fatalf("Recover em WAL vazio: %v", err)
	}

	// Escrevendo depois funciona normalmente
	if err := se.Put("t", "id", types.IntKey(1), `{"id":1}`); err != nil {
		t.Fatal(err)
	}
	_, found, _ := se.Get("t", "id", types.IntKey(1))
	if !found {
		t.Fatal("Put após Recover em WAL vazio não funcionou")
	}
}
