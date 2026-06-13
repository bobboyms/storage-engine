package pagestore

import (
	"container/list"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ErrBufferPoolFull occurs when every page in the pool stays pinned for the
// whole frame-wait timeout, so a new page can never be brought in. Under
// normal contention a fetch waits for a pinned frame to be released; this
// error means the working set genuinely exceeds the pool capacity.
var ErrBufferPoolFull = errors.New("pagestore: buffer pool has no free frames (all pages are pinned)")

// defaultFrameWaitTimeout bounds how long a fetch waits for a pinned frame to
// be released before giving up with ErrBufferPoolFull. It is generous because
// buffer-pool pins are short-lived (one page access); reaching it signals
// real over-subscription, not transient contention.
const defaultFrameWaitTimeout = 10 * time.Second

// BufferPool é um cache LRU de pages em RAM em cima de um PageFile.
//
// Contratos:
//   - Fetch/FetchForWrite INCREMENTA pinCount e adquire latch.
//   - Release DECREMENTA pinCount e libera o latch. Obrigatório chamar
//     pra cada Fetch, otherwise a page nunca é evictada.
//   - MarkDirty avisa o pool que o body foi modificado. No próximo flush
//     (eviction ou FlushAll) a page vai ao disco.
//   - Uma page pinada NUNCA é evictada.
//
// Concorrência:
//   - pool.mu serializa mudanças estruturais (map + LRU).
//   - frame.rw protege o content da page (RW por frame).
//   - Ordem de aquisição: pool.mu → frame.rw (nunca o contrário).
//   - Fetch solta pool.mu antes de adquirir frame.rw para not serializar
//     reads concurrent em pages diferentes.
//
// Simplificações assumidas (aceitáveis para Fase 2, revisitar depois):
//   - I/O de disco em Fetch (miss) acontece enquanto pool.mu está
//     segurada. Trocar por "loading marker" em fase posterior se
//     o miss rate for alto.
//   - Eviction é síncrona: se a page evictada está suja, é gravada
//     antes de Fetch retornar. Pode travar o caminho de read em
//     I/O bursty. Flush assíncrono fica pra fase futura.
type BufferPool struct {
	pf       *PageFile
	capacity int

	mu     sync.Mutex
	frames map[PageID]*frame
	lru    *list.List // front = mais recente, back = menos recente

	// frameWaitTimeout bounds how long fetch/NewPage wait for a pinned frame
	// to be released before failing with ErrBufferPoolFull.
	frameWaitTimeout time.Duration

	beforeFlush func(pageID PageID, page *Page) error

	// Cache effectiveness counters. Updated with atomics so Stats can be
	// read without taking pool.mu.
	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
}

// BufferPoolStats is a point-in-time snapshot of cache counters.
type BufferPoolStats struct {
	// Hits counts Fetch/FetchForWrite calls served from a resident page.
	Hits uint64
	// Misses counts Fetch/FetchForWrite calls that had to load the page
	// from disk.
	Misses uint64
	// Evictions counts pages removed from the pool to make room.
	Evictions uint64
}

// HitRatio returns Hits / (Hits + Misses), or 0 when there were no
// lookups. It is the primary signal of cache effectiveness.
func (s BufferPoolStats) HitRatio() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// Stats returns a snapshot of the cache counters. It does not take the
// pool lock, so the three values are individually consistent but may
// reflect slightly different instants under concurrency.
func (bp *BufferPool) Stats() BufferPoolStats {
	return BufferPoolStats{
		Hits:      bp.hits.Load(),
		Misses:    bp.misses.Load(),
		Evictions: bp.evictions.Load(),
	}
}

type DirtyPageInfo struct {
	PageID  PageID
	PageLSN uint64
	// RecLSN is the oldest unflushed LSN that has modified this page
	// since it transitioned from clean → dirty. ARIES recovery uses
	// the minimum RecLSN across the dirty page table to bound the
	// physical redo start.
	RecLSN uint64
}

type frame struct {
	page     Page
	pageID   PageID
	dirty    atomic.Bool
	recLSN   atomic.Uint64 // oldest unflushed LSN; 0 when clean
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
		pf:               pf,
		capacity:         capacity,
		frames:           make(map[PageID]*frame, capacity),
		lru:              list.New(),
		frameWaitTimeout: defaultFrameWaitTimeout,
	}
}

// Capacity devolve a capacidade configurada.
func (bp *BufferPool) Capacity() int { return bp.capacity }

// SetFrameWaitTimeout overrides how long fetch/NewPage wait for a pinned
// frame to be released before failing with ErrBufferPoolFull. A non-positive
// value restores the default. Primarily a tuning/testing knob.
func (bp *BufferPool) SetFrameWaitTimeout(d time.Duration) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if d <= 0 {
		d = defaultFrameWaitTimeout
	}
	bp.frameWaitTimeout = d
}

// Size devolve quantos frames estão ocupados no momento.
func (bp *BufferPool) Size() int {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return len(bp.frames)
}

// SetBeforeFlushHook registra um callback executado imediatamente antes
// de cada flush de page suja para o PageFile. O hook pode gravar WAL,
// métricas ou outros side-effects e DEVE obedecer WAL-before-data.
func (bp *BufferPool) SetBeforeFlushHook(hook func(pageID PageID, page *Page) error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.beforeFlush = hook
}

// DirtyPages devolve um snapshot das pages sujas atualmente em cache,
// incluindo o pageLSN visto no frame.
func (bp *BufferPool) DirtyPages() []DirtyPageInfo {
	bp.mu.Lock()
	frames := make([]*frame, 0, len(bp.frames))
	for _, f := range bp.frames {
		if f.dirty.Load() {
			frames = append(frames, f)
		}
	}
	bp.mu.Unlock()

	dirty := make([]DirtyPageInfo, 0, len(frames))
	for _, f := range frames {
		f.rw.RLock()
		hdr, err := f.page.GetHeader()
		f.rw.RUnlock()
		if err != nil {
			continue
		}
		dirty = append(dirty, DirtyPageInfo{
			PageID:  f.pageID,
			PageLSN: hdr.PageLSN,
			RecLSN:  f.recLSN.Load(),
		})
	}
	return dirty
}

// ReplacePageImage atualiza o frame em cache, se exist, com uma imagem
// reaplicada pelo recovery. Not cria novo frame nem toca disco.
func (bp *BufferPool) ReplacePageImage(pageID PageID, page *Page) {
	bp.mu.Lock()
	f := bp.frames[pageID]
	bp.mu.Unlock()
	if f == nil {
		return
	}
	f.rw.Lock()
	copy(f.page[:], page[:])
	f.rw.Unlock()
}

// Fetch traz a page pageID pro pool com latch COMPARTILHADO (read).
// Múltiplos Fetch concurrent na mesma page podem prosseguir em paralelo.
func (bp *BufferPool) Fetch(pageID PageID) (*PageHandle, error) {
	return bp.fetch(pageID, false)
}

// FetchForWrite traz a page pageID pro pool com latch EXCLUSIVO (write).
// Serializa com outros fetches (read ou write) na mesma page.
func (bp *BufferPool) FetchForWrite(pageID PageID) (*PageHandle, error) {
	return bp.fetch(pageID, true)
}

func (bp *BufferPool) fetch(pageID PageID, write bool) (*PageHandle, error) {
	bp.mu.Lock()

	if f, ok := bp.frames[pageID]; ok {
		bp.hits.Add(1)
		bp.lru.MoveToFront(f.lruElem)
		f.pinCount.Add(1)
		bp.mu.Unlock()

		bp.acquireLatch(f, write)
		return &PageHandle{bp: bp, frame: f, write: write}, nil
	}

	bp.misses.Add(1)

	// Miss: garante espaço antes de carregar.
	if err := bp.makeRoomLocked(); err != nil {
		bp.mu.Unlock()
		return nil, err
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

// makeRoomLocked ensures there is room for one new frame. Called with bp.mu
// held. If the pool is full it evicts an unpinned frame; if every frame is
// pinned it releases bp.mu, waits briefly for some other goroutine to release
// a pin, and retries — up to frameWaitTimeout. This turns a transient
// concurrency spike into added latency instead of an ErrBufferPoolFull that
// would degrade the engine. Only genuine over-subscription (a working set
// larger than the pool) reaches the timeout and returns the error. On return
// (nil) bp.mu is held and there is room; on error bp.mu is held too.
func (bp *BufferPool) makeRoomLocked() error {
	if len(bp.frames) < bp.capacity {
		return nil
	}
	deadline := time.Now().Add(bp.frameWaitTimeout)
	backoff := 50 * time.Microsecond
	for {
		if len(bp.frames) < bp.capacity || bp.tryEvictLocked() {
			return nil
		}
		if time.Now().After(deadline) {
			return ErrBufferPoolFull
		}
		// Every resident frame is pinned right now. Drop the lock so the
		// goroutines holding those pins can finish and release them, then
		// retry. The backoff caps quickly so latency stays low.
		bp.mu.Unlock()
		time.Sleep(backoff)
		if backoff < 2*time.Millisecond {
			backoff *= 2
		}
		bp.mu.Lock()
	}
}

// tryEvictLocked tenta evictar uma page not-pinada, varrendo do tail
// (LRU) pra frente. Retorna false se todas as pages estão pinadas.
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
			if bp.beforeFlush != nil {
				if err := bp.beforeFlush(f.pageID, &f.page); err != nil {
					f.rw.RUnlock()
					return false
				}
			}
			err := bp.pf.WritePage(f.pageID, &f.page)
			f.rw.RUnlock()
			if err != nil {
				// Not evicta se flush failed — melhor manter do que perder dados.
				return false
			}
			f.dirty.Store(false)
			f.recLSN.Store(0)
		}

		delete(bp.frames, f.pageID)
		bp.lru.Remove(e)
		bp.evictions.Add(1)
		return true
	}
	return false
}

// NewPage aloca um novo pageID no PageFile, cria um frame empty no
// pool, marca como suja (pra forçar write inicial) e devolve handle
// com latch exclusivo.
func (bp *BufferPool) NewPage() (*PageHandle, error) {
	pageID, err := bp.pf.AllocatePage()
	if err != nil {
		return nil, err
	}

	bp.mu.Lock()
	if err := bp.makeRoomLocked(); err != nil {
		bp.mu.Unlock()
		return nil, err
	}

	f := &frame{pageID: pageID}
	f.pinCount.Add(1)
	f.dirty.Store(true) // garante write inicial no flush
	f.lruElem = bp.lru.PushFront(f)
	bp.frames[pageID] = f
	bp.mu.Unlock()

	f.rw.Lock()
	return &PageHandle{bp: bp, frame: f, write: true}, nil
}

// FlushAll grava todas as pages sujas no PageFile e chama fsync.
// Not evicta — as pages continuam no pool, apenas deixam de estar sujas.
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
		if bp.beforeFlush != nil {
			if err := bp.beforeFlush(f.pageID, &f.page); err != nil {
				f.rw.RUnlock()
				return err
			}
		}
		err := bp.pf.WritePage(f.pageID, &f.page)
		f.rw.RUnlock()
		if err != nil {
			return err
		}
		f.dirty.Store(false)
	}
	return bp.pf.Sync()
}

// Close flusha e libera todos os frames. Not fecha o PageFile — isso
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
// uma page. Enquanto not for Release-ado, a page fica pinada
// (not pode ser evictada) e o latch fica segurado.
type PageHandle struct {
	bp       *BufferPool
	frame    *frame
	write    bool
	released atomic.Bool
}

// Page devolve o buffer da page. Modificar exige latch de write
// (vindo de FetchForWrite ou NewPage).
func (h *PageHandle) Page() *Page { return &h.frame.page }

// ID devolve o PageID do frame.
func (h *PageHandle) ID() PageID { return h.frame.pageID }

// MarkDirty sinaliza que o content foi modificado. Só faz sentido
// depois de um FetchForWrite ou NewPage — marcar com latch de read
// é um bug no chamador mas does not cause corruption (só flush desnecessário).
//
// On a clean → dirty transition the frame snapshots the page's current
// PageLSN as its ARIES recLSN (oldest unflushed LSN). The recLSN is
// reset to 0 when the page is flushed back to disk. Callers should
// therefore call AdvancePageLSN before MarkDirty so the captured LSN
// matches the modifying entry.
func (h *PageHandle) MarkDirty() {
	if h.frame.dirty.CompareAndSwap(false, true) {
		if hdr, err := h.frame.page.GetHeader(); err == nil {
			h.frame.recLSN.Store(hdr.PageLSN)
		}
	}
}

// Release libera o latch e decrementa o pinCount. Idempotente.
// Em caso de PAGES de write sujas, a gravação só acontece em
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
