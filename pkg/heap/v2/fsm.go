package v2

import (
	"sync"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// FreeSpaceMap é uma estrutura hint em memória que rastreia espaço livre
// aproximado por page. Not é persistida — é reconstruída pelo Vacuum
// quando as pages são compactadas.
//
// Objetivo: evitar scan linear de todas as pages durante inserts.
// Sem FSM, HeapV2.Write sempre vai para activePageID e aloca nova page
// quando está cheia. Com FSM, pages liberadas pelo Vacuum são reutilizadas.
//
// Contrato de aproximação: o value em freeBytes pode estar desatualizado
// (write concurrent pode ter consumido espaço). O Write path trata
// ErrPageFull como "remover do FSM e tentar próximo" — seguro.
type FreeSpaceMap struct {
	mu    sync.Mutex
	pages map[pagestore.PageID]int // pageID → espaço livre aproximado em bytes
}

func newFreeSpaceMap() *FreeSpaceMap {
	return &FreeSpaceMap{
		pages: make(map[pagestore.PageID]int),
	}
}

// Register registra (ou atualiza) o espaço livre de uma page.
// Se freeBytes <= 0, a page é removida do FSM (sem sentido trackear cheia).
func (fsm *FreeSpaceMap) Register(pageID pagestore.PageID, freeBytes int) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	if freeBytes <= 0 {
		delete(fsm.pages, pageID)
		return
	}
	fsm.pages[pageID] = freeBytes
}

// FindPage retorna a PageID de uma page com pelo menos neededBytes livres.
// Retorna (InvalidPageID, false) se nenhuma candidata foi encontrada.
// A busca not tem ordem garantida — retorna a primeira que satisfaz.
func (fsm *FreeSpaceMap) FindPage(neededBytes int) (pagestore.PageID, bool) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	for pid, free := range fsm.pages {
		if free >= neededBytes {
			return pid, true
		}
	}
	return pagestore.InvalidPageID, false
}

// Remove elimina uma page do FSM (ex: detectou que está cheia no Write path).
func (fsm *FreeSpaceMap) Remove(pageID pagestore.PageID) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	delete(fsm.pages, pageID)
}

// Len retorna o número de pages rastreadas. Útil para testes.
func (fsm *FreeSpaceMap) Len() int {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	return len(fsm.pages)
}
