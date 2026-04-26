package v2

import (
	"errors"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// errorsIs é wrapper pra encurtar chamadas de errors.Is nos testes.
func errorsIs(err, target error) bool { return errors.Is(err, target) }

// Helper: cria um NodePage empty (folha). Usa cmp default (int64 ordering).
func newLeafPage(t *testing.T) (*pagestore.Page, *NodePage) {
	t.Helper()
	var p pagestore.Page
	np := InitLeafPage(&p, pagestore.BodySize, nil)
	return &p, np
}

func TestNodePage_LeafInsertAndGet(t *testing.T) {
	_, np := newLeafPage(t)

	if err := np.LeafInsert(30, 3000); err != nil {
		t.Fatal(err)
	}
	if err := np.LeafInsert(10, 1000); err != nil {
		t.Fatal(err)
	}
	if err := np.LeafInsert(20, 2000); err != nil {
		t.Fatal(err)
	}

	if np.NumKeys() != 3 {
		t.Fatalf("expected 3 keys, got %d", np.NumKeys())
	}

	type kv struct {
		k uint64
		v int64
	}
	want := []kv{{10, 1000}, {20, 2000}, {30, 3000}}
	for i, w := range want {
		k, v := np.LeafAt(i)
		if k != w.k || v != w.v {
			t.Fatalf("slot %d: expected (%d,%d), got (%d,%d)", i, w.k, w.v, k, v)
		}
	}

	if v, ok := np.LeafGet(20); !ok || v != 2000 {
		t.Fatalf("Get(20): expected (2000,true), got (%d,%v)", v, ok)
	}
	if _, ok := np.LeafGet(99); ok {
		t.Fatal("Get(99) em key inexistsnte should return ok=false")
	}
}

func TestNodePage_LeafDuplicateKey_Updates(t *testing.T) {
	_, np := newLeafPage(t)

	_ = np.LeafInsert(42, 100)
	_ = np.LeafInsert(42, 999)

	if np.NumKeys() != 1 {
		t.Fatalf("inserção duplicada not should criar slot novo; NumKeys=%d", np.NumKeys())
	}
	if v, _ := np.LeafGet(42); v != 999 {
		t.Fatalf("value was not atualizado: %d", v)
	}
}

func TestNodePage_LeafDelete(t *testing.T) {
	_, np := newLeafPage(t)

	for _, pair := range []struct {
		key uint64
		val int64
	}{
		{10, 100}, {20, 200}, {30, 300}, {40, 400},
	} {
		if err := np.LeafInsert(pair.key, pair.val); err != nil {
			t.Fatal(err)
		}
	}

	removed, err := np.LeafDelete(20)
	if err != nil {
		t.Fatalf("LeafDelete: %v", err)
	}
	if !removed {
		t.Fatal("LeafDelete should remover key existsnte")
	}

	if np.NumKeys() != 3 {
		t.Fatalf("expected 3 keys after delete, got %d", np.NumKeys())
	}

	want := []struct {
		key uint64
		val int64
	}{
		{10, 100}, {30, 300}, {40, 400},
	}
	for i, w := range want {
		k, v := np.LeafAt(i)
		if k != w.key || v != w.val {
			t.Fatalf("slot %d: expected (%d,%d), got (%d,%d)", i, w.key, w.val, k, v)
		}
	}

	if _, found := np.LeafGet(20); found {
		t.Fatal("key deleted not should exist")
	}
	if removed, err := np.LeafDelete(999); err != nil || removed {
		t.Fatalf("delete de key ausente: removed=%v err=%v", removed, err)
	}
}

func TestNodePage_LeafFull(t *testing.T) {
	_, np := newLeafPage(t)
	max := np.MaxLeafSlots()

	for i := 0; i < max; i++ {
		if err := np.LeafInsert(uint64(i), int64(i*10)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	if err := np.LeafInsert(uint64(max), 999); !errorsIs(err, ErrLeafFull) {
		t.Fatalf("expected ErrLeafFull, got: %v", err)
	}

	if err := np.LeafInsert(0, 7777); err != nil {
		t.Fatalf("update de key existsnte em leaf cheia should funcionar: %v", err)
	}
	if v, _ := np.LeafGet(0); v != 7777 {
		t.Fatal("update in-place failed")
	}
}

func TestNodePage_InitLeaf_Empty(t *testing.T) {
	_, np := newLeafPage(t)

	if !np.IsLeaf() {
		t.Fatal("page freshly initialized como leaf should reportar IsLeaf=true")
	}
	if np.NumKeys() != 0 {
		t.Fatalf("leaf freshly initialized should have 0 keys, tem %d", np.NumKeys())
	}
	if np.NextLeafPageID() != pagestore.InvalidPageID {
		t.Fatal("NextLeaf should be Invalid (sem sibling)")
	}
}
