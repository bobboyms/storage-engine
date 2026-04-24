package pagestore

import (
	"container/list"
	"errors"
	"sync"
	"sync/atomic"
)

// ErrBufferPoolFull ocorre quando todas as páginas do pool estão pinadas
// e não há espaço para trazer uma nova.
var ErrBufferPoolFull = errors.New("pagestore: buffer pool sem frames livres (todas as páginas pinadas)")

// BufferPool é um cache LRU de páginas em RAM em cima de um PageFile.
//
// Contratos:
//   - Fetch/FetchForWrite INCREMENTA pinCount e adquire latch.
//   - Release DECREMENTA pinCount e libera o latch. Obrigatório chamar
//     pra cada Fetch, senão a página nunca é evictada.
//   - MarkDirty avisa o pool que o body foi modificado. No próximo flush
//     (eviction ou FlushAll) a página vai ao disco.
//   - Uma página pinada NUNCA é evictada.
//
// Concorrência:
//   - pool.mu serializa mudanças estruturais (map + LRU).
//   - frame.rw protege o conteúdo da página (RW por frame).
//   - Ordem de aquisição: pool.mu → frame.rw (nunca o contrário).
//   - Fetch solta pool.mu antes de adquirir frame.rw para não serializar
//     leituras concorrentes em páginas diferentes.
//
// Simplificações assumidas (aceitáveis para Fase 2, revisitar depois):
//   - I/O de disco em Fetch (miss) acontece enquanto pool.mu está
//     segurada. Trocar por "loading marker" em fase posterior se
//     o miss rate for alto.
//   - Eviction é síncrona: se a página evictada está suja, é gravada
//     antes de Fetch retornar. Pode travar o caminho de leitura em
//     I/O bursty. Flush assíncrono fica pra fase futura.
type BufferPool struct {
	pf       *PageFile
	capacity int

	mu     sync.Mutex
	frames map[PageID]*frame
	lru    *list.List // front = mais recente, back = menos recente
}

type frame struct {
	page     Page
	pageID   PageID
	dirty    atomic.Bool
	pinCount atomic.Int32

	rw sync.RWMutex // protege `page`

	// só é tocado com pool.mu segurado
	lruElem *list.Element
}

// NewBufferPool cria um pool com capacidade fixa. Capacidade mínima 1.
func NewBufferPool(pf *PageFile, capacity int) *BufferPool {
	if capacity < 1 {
		capacity = 1
	}
	return &BufferPool{
		pf:       pf,
		capacity: capacity,
		frames:   make(map[PageID]*frame, capacity),
		lru:      list.New(),
	}
}

// Capacity devolve a capacidade configurada.
func (bp *BufferPool) Capacity() int { return bp.capacity }

// Size devolve quantos frames estão ocupados no momento.
func (bp *BufferPool) Size() int {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return len(bp.frames)
}

// Fetch traz a página pageID pro pool com latch COMPARTILHADO (leitura).
// Múltiplos Fetch concorrentes na mesma página podem prosseguir em paralelo.
func (bp *BufferPool) Fetch(pageID PageID) (*PageHandle, error) {
	return bp.fetch(pageID, false)
}

// FetchForWrite traz a página pageID pro pool com latch EXCLUSIVO (escrita).
// Serializa com outros fetches (leitura ou escrita) na mesma página.
func (bp *BufferPool) FetchForWrite(pageID PageID) (*PageHandle, error) {
	return bp.fetch(pageID, true)
}

func (bp *BufferPool) fetch(pageID PageID, write bool) (*PageHandle, error) {
	bp.mu.Lock()

	if f, ok := bp.frames[pageID]; ok {
		bp.lru.MoveToFront(f.lruElem)
		f.pinCount.Add(1)
		bp.mu.Unlock()

		bp.acquireLatch(f, write)
		return &PageHandle{bp: bp, frame: f, write: write}, nil
	}

	// Miss: garante espaço antes de carregar.
	for len(bp.frames) >= bp.capacity {
		if !bp.tryEvictLocked() {
			bp.mu.Unlock()
			return nil, ErrBufferPoolFull
		}
	}

	// Carrega do disco com pool.mu segurada (simplificação Fase 2).
	p, err := bp.pf.ReadPage(pageID)
	if err != nil {
		bp.mu.Unlock()
		return nil, err
	}

	f := &frame{pageID: pageID, page: *p}
	f.pinCount.Add(1)
	f.lruElem = bp.lru.PushFront(f)
	bp.frames[pageID] = f
	bp.mu.Unlock()

	bp.acquireLatch(f, write)
	return &PageHandle{bp: bp, frame: f, write: write}, nil
}

func (bp *BufferPool) acquireLatch(f *frame, write bool) {
	if write {
		f.rw.Lock()
	} else {
		f.rw.RLock()
	}
}

// tryEvictLocked tenta evictar uma página não-pinada, varrendo do tail
// (LRU) pra frente. Retorna false se todas as páginas estão pinadas.
// DEVE ser chamado com pool.mu segurada.
func (bp *BufferPool) tryEvictLocked() bool {
	for e := bp.lru.Back(); e != nil; e = e.Prev() {
		f := e.Value.(*frame)
		if f.pinCount.Load() != 0 {
			continue
		}

		// Flush síncrono se suja.
		if f.dirty.Load() {
			// Segura RLock enquanto flusheamos (ninguém está escrevendo
			// porque pinCount == 0, mas o contrato exige lock).
			f.rw.RLock()
			err := bp.pf.WritePage(f.pageID, &f.page)
			f.rw.RUnlock()
			if err != nil {
				// Não evicta se flush falhou — melhor manter do que perder dados.
				return false
			}
			f.dirty.Store(false)
		}

		delete(bp.frames, f.pageID)
		bp.lru.Remove(e)
		return true
	}
	return false
}

// NewPage aloca um novo pageID no PageFile, cria um frame vazio no
// pool, marca como suja (pra forçar escrita inicial) e devolve handle
// com latch exclusivo.
func (bp *BufferPool) NewPage() (*PageHandle, error) {
	pageID, err := bp.pf.AllocatePage()
	if err != nil {
		return nil, err
	}

	bp.mu.Lock()
	for len(bp.frames) >= bp.capacity {
		if !bp.tryEvictLocked() {
			bp.mu.Unlock()
			return nil, ErrBufferPoolFull
		}
	}

	f := &frame{pageID: pageID}
	f.pinCount.Add(1)
	f.dirty.Store(true) // garante escrita inicial no flush
	f.lruElem = bp.lru.PushFront(f)
	bp.frames[pageID] = f
	bp.mu.Unlock()

	f.rw.Lock()
	return &PageHandle{bp: bp, frame: f, write: true}, nil
}

// FlushAll grava todas as páginas sujas no PageFile e chama fsync.
// Não evicta — as páginas continuam no pool, apenas deixam de estar sujas.
func (bp *BufferPool) FlushAll() error {
	bp.mu.Lock()
	dirty := make([]*frame, 0, len(bp.frames))
	for _, f := range bp.frames {
		if f.dirty.Load() {
			dirty = append(dirty, f)
		}
	}
	bp.mu.Unlock()

	for _, f := range dirty {
		f.rw.RLock()
		err := bp.pf.WritePage(f.pageID, &f.page)
		f.rw.RUnlock()
		if err != nil {
			return err
		}
		f.dirty.Store(false)
	}
	return bp.pf.Sync()
}

// Close flusha e libera todos os frames. Não fecha o PageFile — isso
// é responsabilidade do dono do PageFile.
func (bp *BufferPool) Close() error {
	if err := bp.FlushAll(); err != nil {
		return err
	}
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.frames = make(map[PageID]*frame)
	bp.lru = list.New()
	return nil
}

// ─────────────────────────────────────────────────────────────────────
// PageHandle
// ─────────────────────────────────────────────────────────────────────

// PageHandle é o ponteiro efêmero que um chamador recebe pra acessar
// uma página. Enquanto não for Release-ado, a página fica pinada
// (não pode ser evictada) e o latch fica segurado.
type PageHandle struct {
	bp       *BufferPool
	frame    *frame
	write    bool
	released atomic.Bool
}

// Page devolve o buffer da página. Modificar exige latch de escrita
// (vindo de FetchForWrite ou NewPage).
func (h *PageHandle) Page() *Page { return &h.frame.page }

// ID devolve o PageID do frame.
func (h *PageHandle) ID() PageID { return h.frame.pageID }

// MarkDirty sinaliza que o conteúdo foi modificado. Só faz sentido
// depois de um FetchForWrite ou NewPage — marcar com latch de leitura
// é um bug no chamador mas não causa corrupção (só flush desnecessário).
func (h *PageHandle) MarkDirty() { h.frame.dirty.Store(true) }

// Release libera o latch e decrementa o pinCount. Idempotente.
// Em caso de PAGES de escrita sujas, a gravação só acontece em
// FlushAll ou durante eviction — Release é barato.
func (h *PageHandle) Release() {
	if !h.released.CompareAndSwap(false, true) {
		return
	}
	if h.write {
		h.frame.rw.Unlock()
	} else {
		h.frame.rw.RUnlock()
	}
	h.frame.pinCount.Add(-1)
}
