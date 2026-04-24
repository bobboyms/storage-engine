package pagestore

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func newPoolWithFile(t testing.TB, capacity int) (*BufferPool, *PageFile) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pages.db")
	pf, err := NewPageFile(path, newCipher(t))
	if err != nil {
		t.Fatal(err)
	}
	bp := NewBufferPool(pf, capacity)
	t.Cleanup(func() {
		bp.Close()
		pf.Close()
	})
	return bp, pf
}

// allocAndWrite aloca uma página via pool, escreve bytes determinísticos
// no body e devolve o pageID. Garante que já está persistida em disco.
func allocAndWrite(t testing.TB, bp *BufferPool, seed byte) PageID {
	t.Helper()
	h, err := bp.NewPage()
	if err != nil {
		t.Fatal(err)
	}
	usable := bp.pf.cipher.UsableBodySize()
	for i := 0; i < usable; i++ {
		h.Page().Body()[i] = seed + byte(i%251)
	}
	id := h.ID()
	h.Release()
	if err := bp.FlushAll(); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestBufferPool_FetchRoundTrip(t *testing.T) {
	bp, _ := newPoolWithFile(t, 8)
	id := allocAndWrite(t, bp, 0x42)

	h, err := bp.Fetch(id)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()

	usable := bp.pf.cipher.UsableBodySize()
	expected := make([]byte, usable)
	for i := range expected {
		expected[i] = 0x42 + byte(i%251)
	}
	if !bytes.Equal(h.Page().Body()[:usable], expected) {
		t.Fatal("body divergente após fetch")
	}
}

func TestBufferPool_DirtyFlushesOnFlushAll(t *testing.T) {
	bp, pf := newPoolWithFile(t, 8)
	id := allocAndWrite(t, bp, 1)

	// Modifica e marca suja
	h, _ := bp.FetchForWrite(id)
	h.Page().Body()[0] = 0xFE
	h.MarkDirty()
	h.Release()

	if err := bp.FlushAll(); err != nil {
		t.Fatal(err)
	}

	// Lê direto do disco (sem pool) e confirma mudança persistida
	p, err := pf.ReadPage(id)
	if err != nil {
		t.Fatal(err)
	}
	if p.Body()[0] != 0xFE {
		t.Fatalf("esperava 0xFE no disco, recebi 0x%02X", p.Body()[0])
	}
}

func TestBufferPool_DirtyFlushesOnEviction(t *testing.T) {
	bp, pf := newPoolWithFile(t, 2)

	// Pool com capacity=2. Vamos criar 3 páginas pra forçar eviction.
	id1 := allocAndWrite(t, bp, 1)
	id2 := allocAndWrite(t, bp, 2)

	// Traz id1 e id2 pro pool (hits — eram recém-alocadas mas post-flush
	// ainda estão no pool; vamos forçar reload limpo)
	bp.Close()
	bp = NewBufferPool(pf, 2)
	t.Cleanup(func() { bp.Close() })

	// Pega id1 pra write, modifica, deixa sujo
	h1, _ := bp.FetchForWrite(id1)
	h1.Page().Body()[5] = 0xAB
	h1.MarkDirty()
	h1.Release()

	// Pega id2 — ainda cabe
	h2, _ := bp.Fetch(id2)
	h2.Release()

	// Agora cria uma terceira página — forçará eviction de id1 (LRU + suja).
	// id1 deve ser gravada antes de sair do pool.
	h3, _ := bp.NewPage()
	h3.Release()

	// Sem FlushAll: a mudança em id1 foi persistida durante a eviction?
	p, err := pf.ReadPage(id1)
	if err != nil {
		t.Fatal(err)
	}
	if p.Body()[5] != 0xAB {
		t.Fatalf("eviction não flushou página suja: disco tem 0x%02X", p.Body()[5])
	}
}

func TestBufferPool_LRUEviction_OrderCorrect(t *testing.T) {
	bp, _ := newPoolWithFile(t, 3)

	id1 := allocAndWrite(t, bp, 1)
	id2 := allocAndWrite(t, bp, 2)
	id3 := allocAndWrite(t, bp, 3)
	id4 := allocAndWrite(t, bp, 4)

	// Close e reabre o pool (não o pf) pra começar com pool vazio
	bp.Close()
	bp = NewBufferPool(bp.pf, 3)
	t.Cleanup(func() { bp.Close() })

	// Traz id1, id2, id3 — pool cheio agora
	h1, _ := bp.Fetch(id1)
	h1.Release()
	h2, _ := bp.Fetch(id2)
	h2.Release()
	h3, _ := bp.Fetch(id3)
	h3.Release()

	// Toca id1 pra mandá-lo pro topo da LRU
	h1, _ = bp.Fetch(id1)
	h1.Release()

	// Agora id2 é o mais antigo. Trazer id4 deve evictar id2.
	h4, _ := bp.Fetch(id4)
	h4.Release()

	if bp.Size() != 3 {
		t.Fatalf("pool deveria ter 3 frames, tem %d", bp.Size())
	}

	// id2 não deve mais estar no pool — conferir via Size permanece 3
	// e via verificação no map interno:
	bp.mu.Lock()
	_, id2Present := bp.frames[id2]
	bp.mu.Unlock()
	if id2Present {
		t.Fatal("id2 deveria ter sido evictada (era a LRU)")
	}
}

func TestBufferPool_PinnedPagesNeverEvicted(t *testing.T) {
	bp, _ := newPoolWithFile(t, 2)

	id1 := allocAndWrite(t, bp, 1)
	id2 := allocAndWrite(t, bp, 2)

	bp.Close()
	bp = NewBufferPool(bp.pf, 2)
	t.Cleanup(func() { bp.Close() })

	// Pina ambas
	h1, _ := bp.Fetch(id1)
	h2, _ := bp.Fetch(id2)
	defer h1.Release()
	defer h2.Release()

	// Tenta alocar uma terceira — todas as existentes estão pinadas,
	// eviction deve falhar com ErrBufferPoolFull.
	_, err := bp.NewPage()
	if !errors.Is(err, ErrBufferPoolFull) {
		t.Fatalf("esperava ErrBufferPoolFull, recebi: %v", err)
	}
}

func TestBufferPool_NewPage_PersistsAfterFlush(t *testing.T) {
	bp, pf := newPoolWithFile(t, 4)

	h, err := bp.NewPage()
	if err != nil {
		t.Fatal(err)
	}
	id := h.ID()
	usable := bp.pf.cipher.UsableBodySize()
	for i := 0; i < usable; i++ {
		h.Page().Body()[i] = byte(i)
	}
	h.Release()

	if err := bp.FlushAll(); err != nil {
		t.Fatal(err)
	}

	// Lê direto do pf (sem pool)
	p, err := pf.ReadPage(id)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < usable; i++ {
		if p.Body()[i] != byte(i) {
			t.Fatalf("byte %d: disco tem 0x%02X, esperado 0x%02X", i, p.Body()[i], byte(i))
		}
	}
}

func TestBufferPool_ReleaseIsIdempotent(t *testing.T) {
	bp, _ := newPoolWithFile(t, 4)
	id := allocAndWrite(t, bp, 1)

	h, _ := bp.Fetch(id)
	h.Release()
	h.Release() // não deve panicar nem causar unlock duplo

	// Pin count deve estar em 0, logo evictável
	bp.mu.Lock()
	f, ok := bp.frames[id]
	bp.mu.Unlock()
	if !ok {
		t.Fatal("frame sumiu")
	}
	if pc := f.pinCount.Load(); pc != 0 {
		t.Fatalf("pinCount esperado 0, recebi %d", pc)
	}
}

// TestBufferPool_Concurrent valida o critério de pronto da Fase 2:
// 100 goroutines lendo/escrevendo concorrentemente, -race limpo.
func TestBufferPool_Concurrent(t *testing.T) {
	bp, _ := newPoolWithFile(t, 16)

	const numPages = 50
	ids := make([]PageID, numPages)
	for i := range ids {
		ids[i] = allocAndWrite(t, bp, byte(i))
	}

	const goroutines = 100
	const opsPerG = 40

	var wg sync.WaitGroup
	var errs atomic.Int64

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				id := ids[(gid+i)%numPages]
				if gid%3 == 0 {
					// Escrita
					h, err := bp.FetchForWrite(id)
					if err != nil {
						errs.Add(1)
						return
					}
					h.Page().Body()[0] = byte(gid)
					h.MarkDirty()
					h.Release()
				} else {
					// Leitura
					h, err := bp.Fetch(id)
					if err != nil {
						errs.Add(1)
						return
					}
					_ = h.Page().Body()[0] // read
					h.Release()
				}
			}
		}(g)
	}

	wg.Wait()

	if errs.Load() != 0 {
		t.Fatalf("%d erros durante execução concorrente", errs.Load())
	}
	if err := bp.FlushAll(); err != nil {
		t.Fatal(err)
	}
}

func TestBufferPool_CapacityZeroDefaultsToOne(t *testing.T) {
	pf, _ := newPoolWithFile(t, 0)
	if pf.Capacity() != 1 {
		t.Fatalf("esperava capacity=1, recebi %d", pf.Capacity())
	}
}

func TestBufferPool_SizeTracking(t *testing.T) {
	bp, _ := newPoolWithFile(t, 4)
	if bp.Size() != 0 {
		t.Fatalf("pool vazio deveria ter Size 0, tem %d", bp.Size())
	}

	for i := 0; i < 3; i++ {
		h, _ := bp.NewPage()
		h.Release()
	}

	if bp.Size() != 3 {
		t.Fatalf("pool deveria ter 3 frames, tem %d", bp.Size())
	}
}

// Sanity: garante que um write num handle de leitura não corrompe,
// só por documentação — este teste ilustra o contrato.
func TestBufferPool_DirtyWithoutWriteLatch_DoesNotCorrupt(t *testing.T) {
	bp, pf := newPoolWithFile(t, 4)
	id := allocAndWrite(t, bp, 1)

	h, _ := bp.Fetch(id) // latch de leitura
	h.MarkDirty()        // semanticamente errado, mas não panica
	h.Release()

	// FlushAll vai tentar gravar com RLock. Não deve corromper o arquivo.
	if err := bp.FlushAll(); err != nil {
		t.Fatal(err)
	}

	// E a página ainda deve ser legível
	p, err := pf.ReadPage(id)
	if err != nil {
		t.Fatal(err)
	}
	if p.Body()[0] != 1 {
		t.Fatalf("byte 0: esperado 1, recebi %d", p.Body()[0])
	}
}

// Teste de smoke: exercita muitas evictions em sequência pra pegar
// race no código de eviction + pin/unpin.
func TestBufferPool_HeavyEviction(t *testing.T) {
	bp, _ := newPoolWithFile(t, 4)

	const total = 100
	ids := make([]PageID, total)
	for i := 0; i < total; i++ {
		h, err := bp.NewPage()
		if err != nil {
			t.Fatalf("NewPage %d: %v", i, err)
		}
		ids[i] = h.ID()
		for j := 0; j < 16; j++ {
			h.Page().Body()[j] = byte(i)
		}
		h.Release()
	}

	if err := bp.FlushAll(); err != nil {
		t.Fatal(err)
	}

	// Lê todas de volta, verifica persistência apesar de muita eviction.
	for i, id := range ids {
		h, err := bp.Fetch(id)
		if err != nil {
			t.Fatalf("fetch %d: %v", i, err)
		}
		if h.Page().Body()[0] != byte(i) {
			t.Fatalf("page %d corrompida: esperado %d, recebi %d", i, byte(i), h.Page().Body()[0])
		}
		h.Release()
	}
	_ = fmt.Sprint // evita import-not-used
}
