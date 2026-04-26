package v2

import (
	"sync"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

func TestFSM_RegisterAndFind(t *testing.T) {
	fsm := newFreeSpaceMap()

	fsm.Register(pagestore.PageID(1), 500)
	fsm.Register(pagestore.PageID(2), 200)
	fsm.Register(pagestore.PageID(3), 1000)

	pid, ok := fsm.FindPage(400)
	if !ok {
		t.Fatal("expected encontrar page com >=400 bytes livres")
	}
	if pid != pagestore.PageID(1) && pid != pagestore.PageID(3) {
		t.Fatalf("page retornada %d not satisfaz >= 400 bytes", pid)
	}

	_, ok = fsm.FindPage(1500)
	if ok {
		t.Fatal("not should encontrar page com >=1500 bytes livres")
	}
}

func TestFSM_RegisterZeroRemoves(t *testing.T) {
	fsm := newFreeSpaceMap()
	fsm.Register(pagestore.PageID(5), 300)
	if fsm.Len() != 1 {
		t.Fatalf("expected 1 entrada, tem %d", fsm.Len())
	}

	fsm.Register(pagestore.PageID(5), 0)
	if fsm.Len() != 0 {
		t.Fatalf("expected 0 entradas after register com 0 bytes, tem %d", fsm.Len())
	}
}

func TestFSM_Remove(t *testing.T) {
	fsm := newFreeSpaceMap()
	fsm.Register(pagestore.PageID(1), 100)
	fsm.Register(pagestore.PageID(2), 200)

	fsm.Remove(pagestore.PageID(1))
	if fsm.Len() != 1 {
		t.Fatalf("expected 1 entrada after Remove, tem %d", fsm.Len())
	}

	_, ok := fsm.FindPage(100)
	if !ok {
		t.Fatal("pageID=2 ainda should be no FSM")
	}
}

func TestFSM_ConcurrentAccess(t *testing.T) {
	fsm := newFreeSpaceMap()
	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Writers
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			fsm.Register(pagestore.PageID(id), (id+1)*100)
		}(i)
	}

	// Readers
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			fsm.FindPage(50)
		}()
	}

	wg.Wait()
	// Sem panic ou race → correto.
}

func TestFSM_UpdateAfterInsert(t *testing.T) {
	fsm := newFreeSpaceMap()
	// Simula: page tem 800 bytes livres, insert consome 300.
	fsm.Register(pagestore.PageID(10), 800)

	fsm.mu.Lock()
	current := fsm.pages[pagestore.PageID(10)]
	remaining := current - 300
	if remaining > SlotSize {
		fsm.pages[pagestore.PageID(10)] = remaining
	} else {
		delete(fsm.pages, pagestore.PageID(10))
	}
	fsm.mu.Unlock()

	pid, ok := fsm.FindPage(400)
	if !ok {
		t.Fatal("expected encontrar page com >=400 bytes livres (restam 500)")
	}
	if pid != pagestore.PageID(10) {
		t.Fatalf("expected pageID=10, got %d", pid)
	}
}
