package v2

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
)

// visibleAt é o filtro MVCC que o engine aplica quando uma transação
// quer ler num snapshot específico. Caminha a cadeia de versões a
// partir de `headRID` e devolve a primeira versão visible no LSN dado.
//
// Regra de visibilidade:
//   - CreateLSN <= snapshotLSN (criada antes do snapshot)
//   - DeleteLSN == 0  OU  DeleteLSN > snapshotLSN (not deleted ainda
//     no ponto de vista do snapshot)
//
// Retorna (doc, hdr, true) se achou versão visible; (nil, nil, false)
// se a cadeia terminou sem encontrar. Propaga erros de I/O.
func visibleAt(t *testing.T, h *HeapV2, headRID int64, snapshotLSN uint64) ([]byte, *RecordHeader, bool) {
	t.Helper()
	rid := headRID
	for rid != NoRecordID {
		doc, hdr, err := h.Read(rid)
		if err != nil {
			t.Fatalf("walk: Read(%d) failed: %v", rid, err)
		}
		createdBefore := hdr.CreateLSN <= snapshotLSN
		notDeleted := hdr.DeleteLSN == 0 || hdr.DeleteLSN > snapshotLSN
		if createdBefore && notDeleted {
			return doc, hdr, true
		}
		rid = hdr.PrevRecordID
	}
	return nil, nil, false
}

func TestHeapV2_MVCC_ConcurrentWalksAndUpdates(t *testing.T) {
	// Stress: writers atualizando uma linha em cadeia (criando novas
	// versões continuamente) enquanto readers caminham cadeias antigas.
	// Valida ausência de races no BufferPool durante walks cross-page.
	h := newHeap(t, nil)

	// Pré-popula N cadeias, cada uma com 3 versões.
	const numChains = 10
	heads := make([]int64, numChains)
	for i := 0; i < numChains; i++ {
		v1, _ := h.Write([]byte("seed-v1"), uint64(i*10+1), NoRecordID)
		v2, _ := h.Write([]byte("seed-v2"), uint64(i*10+2), v1)
		v3, _ := h.Write([]byte("seed-v3"), uint64(i*10+3), v2)
		heads[i] = v3
	}

	// heads é mutável pelos writers — um lock protege só o slice.
	var headsMu sync.Mutex

	const writers = 4
	const readers = 8
	const opsPerG = 50

	var wg sync.WaitGroup
	var errCount atomic.Int64

	// Writers: pegam uma cadeia existsnte, adicionam nova versão no topo
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				idx := (g*opsPerG + i) % numChains

				headsMu.Lock()
				prev := heads[idx]
				headsMu.Unlock()

				newV, err := h.Write([]byte("updated"), uint64(1000+g*1000+i), prev)
				if err != nil {
					errCount.Add(1)
					return
				}

				headsMu.Lock()
				heads[idx] = newV
				headsMu.Unlock()
			}
		}(w)
	}

	// Readers: pegam um head aleatório, caminham a cadeia inteira.
	// Not validam content — só que not explode sob -race.
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				idx := (g*opsPerG + i) % numChains

				headsMu.Lock()
				rid := heads[idx]
				headsMu.Unlock()

				// Caminha a cadeia inteira
				steps := 0
				for rid != NoRecordID {
					_, hdr, err := h.Read(rid)
					if err != nil {
						errCount.Add(1)
						return
					}
					rid = hdr.PrevRecordID
					steps++
					if steps > 10000 {
						errCount.Add(1) // cadeia infinita = bug
						return
					}
				}
			}
		}(r)
	}

	wg.Wait()

	if errCount.Load() != 0 {
		t.Fatalf("%d erros durante chain walks concurrent", errCount.Load())
	}
}

func TestHeapV2_MVCC_DeleteTimeTravel(t *testing.T) {
	// Cenário: uma linha é criada, atualizada, depois deleted.
	// Transações com snapshot ANTES do delete mustm continuar vendo.
	//
	// Timeline:
	//   LSN 10: v1 criado
	//   LSN 20: v2 criado (update, prev=v1)
	//   LSN 30: v2 deleted in-place (Valid=false, DeleteLSN=30)
	h := newHeap(t, nil)

	v1, _ := h.Write([]byte("antes"), 10, NoRecordID)
	v2, _ := h.Write([]byte("depois"), 20, v1)
	if err := h.Delete(v2, 30); err != nil {
		t.Fatal(err)
	}

	// Snapshot antes do delete vê v2 (HEAD vivo)
	if doc, hdr, ok := visibleAt(t, h, v2, 25); !ok {
		t.Fatal("snapshot LSN=25 should ver v2")
	} else if string(doc) != "depois" || hdr.CreateLSN != 20 {
		t.Fatalf("LSN=25: expected 'depois'/LSN=20, got %q/LSN=%d", doc, hdr.CreateLSN)
	}

	// Snapshot em LSN=10 vê v1 (v2 nem existia ainda)
	if doc, hdr, ok := visibleAt(t, h, v2, 10); !ok {
		t.Fatal("snapshot LSN=10 should ver v1")
	} else if string(doc) != "antes" || hdr.CreateLSN != 10 {
		t.Fatalf("LSN=10: expected 'antes'/LSN=10, got %q/LSN=%d", doc, hdr.CreateLSN)
	}

	// Snapshot em LSN=30 (no momento do delete) — v2.DeleteLSN=30 is not
	// > 30, então v2 NOT é visible. Cadeia cai pra v1, que está vivo.
	// Esse é o invariante "delete é visible no próprio LSN do delete".
	if doc, hdr, ok := visibleAt(t, h, v2, 30); !ok {
		t.Fatal("snapshot LSN=30 should cair em v1")
	} else if string(doc) != "antes" || hdr.CreateLSN != 10 {
		t.Fatalf("LSN=30: expected fallback pra v1 ('antes'/LSN=10), got %q/LSN=%d", doc, hdr.CreateLSN)
	}

	// Snapshot em LSN=40 (after delete) — v2 já deleted.
	// Cadeia cai em v1 (v1.DeleteLSN=0, visible sempre).
	// Em um sistema real, v1 também teria sido marcada invalid por
	// "superseded" — mas a semântica do v1 atual NOT faz isso (v1 fica
	// com Valid=true mesmo after update). Então v1 fica visible.
	if doc, hdr, ok := visibleAt(t, h, v2, 40); !ok {
		t.Fatal("snapshot LSN=40 should ver v1 (chain fallback)")
	} else if string(doc) != "antes" || hdr.CreateLSN != 10 {
		t.Fatalf("LSN=40: expected fallback pra v1, got %q/LSN=%d", doc, hdr.CreateLSN)
	}
}

func TestHeapV2_MVCC_VisibilityFilter(t *testing.T) {
	// Simula transações com snapshots em LSNs diferentes. Cada snapshot
	// must ver a versão "visible naquele momento": nem futura nem já
	// deleted-pra-ela.
	//
	// Timeline de versões:
	//   LSN 10: v1 criado
	//   LSN 20: v2 criado (update, prev=v1)
	//   LSN 30: v3 criado (update, prev=v2)
	h := newHeap(t, nil)

	v1, _ := h.Write([]byte("v1"), 10, NoRecordID)
	v2, _ := h.Write([]byte("v2"), 20, v1)
	v3, _ := h.Write([]byte("v3"), 30, v2)

	cases := []struct {
		name         string
		snapshotLSN  uint64
		expectedDoc  string
		expectedLSN  uint64
		shouldBeSeen bool
	}{
		{"snapshot antes de tudo (LSN=5)", 5, "", 0, false},
		{"snapshot em v1 (LSN=10)", 10, "v1", 10, true},
		{"snapshot entre v1 e v2 (LSN=15)", 15, "v1", 10, true},
		{"snapshot em v2 (LSN=20)", 20, "v2", 20, true},
		{"snapshot entre v2 e v3 (LSN=25)", 25, "v2", 20, true},
		{"snapshot em v3 (LSN=30)", 30, "v3", 30, true},
		{"snapshot no futuro (LSN=999)", 999, "v3", 30, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc, hdr, ok := visibleAt(t, h, v3, tc.snapshotLSN)
			if ok != tc.shouldBeSeen {
				t.Fatalf("visibilidade esperada %v, got %v", tc.shouldBeSeen, ok)
			}
			if !ok {
				return
			}
			if string(doc) != tc.expectedDoc {
				t.Fatalf("expected doc %q, got %q", tc.expectedDoc, doc)
			}
			if hdr.CreateLSN != tc.expectedLSN {
				t.Fatalf("expected CreateLSN %d, got %d", tc.expectedLSN, hdr.CreateLSN)
			}
		})
	}
}

func TestHeapV2_ChainWalk_CrossPage(t *testing.T) {
	// Força v1 numa page e v2 em outra. O BufferPool precisa alternar
	// as pages durante o walk. Valida que o chain segue corretamente
	// mesmo cruzando fronteiras de page.
	h := newHeap(t, nil)

	// v1 — pequeno, entra na page ativa inicial
	v1, err := h.Write([]byte("versão-antiga"), 10, NoRecordID)
	if err != nil {
		t.Fatal(err)
	}
	v1Page, _ := DecodeRecordID(v1)

	// Enche a page com records dummy pra forçar rotação
	filler := make([]byte, 2000)
	for i := 0; i < 6; i++ {
		if _, err := h.Write(filler, uint64(100+i), NoRecordID); err != nil {
			t.Fatal(err)
		}
	}

	// v2 must cair numa page nova, apontando pra v1 na page antiga
	v2, err := h.Write([]byte("versão-nova"), 50, v1)
	if err != nil {
		t.Fatal(err)
	}
	v2Page, _ := DecodeRecordID(v2)

	if v1Page == v2Page {
		t.Fatalf("teste invalid: v1 e v2 shouldm estar em pages diferentes (ambos em %d)", v1Page)
	}

	// Caminha: v2 → v1 (cruza page)
	doc, hdr, err := h.Read(v2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(doc, []byte("versão-nova")) {
		t.Fatalf("HEAD: doc %q inexpected", doc)
	}
	if hdr.PrevRecordID != v1 {
		t.Fatalf("HEAD.prev should apontar pra v1 (%d), aponta pra %d", v1, hdr.PrevRecordID)
	}

	doc, hdr, err = h.Read(hdr.PrevRecordID)
	if err != nil {
		t.Fatalf("cross-page Read failed: %v", err)
	}
	if !bytes.Equal(doc, []byte("versão-antiga")) {
		t.Fatalf("v1: doc %q inexpected", doc)
	}
	if hdr.PrevRecordID != NoRecordID {
		t.Fatalf("v1 should be inicial, prev=%d", hdr.PrevRecordID)
	}
}

func TestHeapV2_ChainWalk_ThreeVersions_SamePage(t *testing.T) {
	h := newHeap(t, nil)

	// Insere três versões encadeadas. v1 → v2 → v3 (v3 mais recente).
	v1, err := h.Write([]byte("alice@v1"), 10, NoRecordID)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := h.Write([]byte("alice@v2"), 20, v1)
	if err != nil {
		t.Fatal(err)
	}
	v3, err := h.Write([]byte("alice@v3"), 30, v2)
	if err != nil {
		t.Fatal(err)
	}

	// Caminha a cadeia começando do HEAD (v3), espera ver v3, v2, v1.
	expected := []struct {
		doc []byte
		lsn uint64
	}{
		{[]byte("alice@v3"), 30},
		{[]byte("alice@v2"), 20},
		{[]byte("alice@v1"), 10},
	}

	rid := v3
	for i, want := range expected {
		doc, hdr, err := h.Read(rid)
		if err != nil {
			t.Fatalf("passo %d: %v", i, err)
		}
		if !bytes.Equal(doc, want.doc) {
			t.Fatalf("passo %d: doc %q != %q", i, doc, want.doc)
		}
		if hdr.CreateLSN != want.lsn {
			t.Fatalf("passo %d: LSN %d != %d", i, hdr.CreateLSN, want.lsn)
		}
		rid = hdr.PrevRecordID
	}

	// Terminou na sentinela
	if rid != NoRecordID {
		t.Fatalf("cadeia should haveminar em NoRecordID, terminou em %d", rid)
	}
}
