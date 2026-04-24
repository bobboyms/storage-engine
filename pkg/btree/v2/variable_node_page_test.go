package v2

import (
	"bytes"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

func newVarLeaf(t *testing.T) (*pagestore.Page, *VariableNodePage) {
	t.Helper()
	var p pagestore.Page
	vp := InitLeafPageVar(&p, pagestore.BodySize, bytes.Compare)
	return &p, vp
}

func newVarInternal(t *testing.T, leftmost pagestore.PageID) (*pagestore.Page, *VariableNodePage) {
	t.Helper()
	var p pagestore.Page
	vp := InitInternalPageVar(&p, pagestore.BodySize, leftmost, bytes.Compare)
	return &p, vp
}

func TestVarNode_InitLeaf_Empty(t *testing.T) {
	_, vp := newVarLeaf(t)
	if !vp.IsLeaf() {
		t.Fatal("leaf?")
	}
	if vp.NumKeys() != 0 {
		t.Fatalf("esperado 0, recebi %d", vp.NumKeys())
	}
	if vp.NextLeafPageID() != pagestore.InvalidPageID {
		t.Fatal("sibling deveria ser invalid")
	}
}

func TestVarNode_LeafInsertAndGet(t *testing.T) {
	_, vp := newVarLeaf(t)

	// Insere fora de ordem lexicográfica
	pairs := []struct {
		k string
		v int64
	}{
		{"charlie", 3},
		{"alice", 1},
		{"bob", 2},
		{"dave", 4},
	}
	for _, p := range pairs {
		if err := vp.LeafInsertVar([]byte(p.k), p.v); err != nil {
			t.Fatalf("Insert %q: %v", p.k, err)
		}
	}

	if vp.NumKeys() != 4 {
		t.Fatalf("esperado 4, recebi %d", vp.NumKeys())
	}

	// Ordem lexicográfica interna
	want := []string{"alice", "bob", "charlie", "dave"}
	for i, w := range want {
		k, _ := vp.LeafAtVar(i)
		if string(k) != w {
			t.Fatalf("slot %d: esperado %q, recebi %q", i, w, string(k))
		}
	}

	// Get
	if v, ok := vp.LeafGetVar([]byte("bob")); !ok || v != 2 {
		t.Fatalf("Get bob: ok=%v v=%d", ok, v)
	}
	if _, ok := vp.LeafGetVar([]byte("zeta")); ok {
		t.Fatal("Get zeta deveria falhar")
	}
}

func TestVarNode_LeafInsertDuplicate_UpdatesInPlace(t *testing.T) {
	_, vp := newVarLeaf(t)
	_ = vp.LeafInsertVar([]byte("foo"), 1)
	_ = vp.LeafInsertVar([]byte("foo"), 999)

	if vp.NumKeys() != 1 {
		t.Fatalf("duplicata não deveria criar slot novo; NumKeys=%d", vp.NumKeys())
	}
	v, _ := vp.LeafGetVar([]byte("foo"))
	if v != 999 {
		t.Fatalf("update falhou: %d", v)
	}
}

func TestVarNode_LeafDelete_CompactsPage(t *testing.T) {
	_, vp := newVarLeaf(t)

	initial := []struct {
		k string
		v int64
	}{
		{"alpha", 1},
		{"bravo", 2},
		{"charlie", 3},
		{"delta", 4},
	}
	for _, item := range initial {
		if err := vp.LeafInsertVar([]byte(item.k), item.v); err != nil {
			t.Fatalf("insert %q: %v", item.k, err)
		}
	}

	freeBefore := vp.FreeSpace()
	removed, err := vp.LeafDeleteVar([]byte("bravo"))
	if err != nil {
		t.Fatalf("LeafDeleteVar: %v", err)
	}
	if !removed {
		t.Fatal("LeafDeleteVar deveria remover chave existente")
	}
	if vp.NumKeys() != 3 {
		t.Fatalf("esperava 3 chaves após delete, recebi %d", vp.NumKeys())
	}
	if vp.FreeSpace() <= freeBefore {
		t.Fatalf("delete deveria liberar espaço: before=%d after=%d", freeBefore, vp.FreeSpace())
	}
	if _, found := vp.LeafGetVar([]byte("bravo")); found {
		t.Fatal("chave deletada não deveria existir")
	}

	want := []string{"alpha", "charlie", "delta"}
	for i, k := range want {
		got, _ := vp.LeafAtVar(i)
		if string(got) != k {
			t.Fatalf("slot %d: esperado %q, recebi %q", i, k, string(got))
		}
	}

	if removed, err := vp.LeafDeleteVar([]byte("missing")); err != nil || removed {
		t.Fatalf("delete ausente: removed=%v err=%v", removed, err)
	}
}

func TestVarNode_LeafFull(t *testing.T) {
	_, vp := newVarLeaf(t)

	// Insere chaves de 100 bytes até a folha encher
	big := bytes.Repeat([]byte("x"), 100)
	var inserted int
	for i := 0; i < 1000; i++ {
		k := append([]byte{byte(i), byte(i >> 8)}, big...)
		if err := vp.LeafInsertVar(k, int64(i)); err != nil {
			break
		}
		inserted++
	}

	if inserted < 20 {
		t.Fatalf("encheu cedo demais: %d", inserted)
	}

	// Próxima insert com nova key deve ErrLeafFull
	newK := append([]byte("zzzzzzzz"), big...)
	if err := vp.LeafInsertVar(newK, 9999); err != ErrLeafFull {
		t.Fatalf("esperava ErrLeafFull, recebi %v", err)
	}
}

func TestVarNode_InitInternal_FindChild(t *testing.T) {
	_, vp := newVarInternal(t, 100)

	// Monta: leftmost=100 | sep="bob"->200 | sep="dan"->300
	vp.InsertSeparatorVar([]byte("bob"), 200)
	vp.InsertSeparatorVar([]byte("dan"), 300)

	cases := []struct {
		k    string
		want pagestore.PageID
	}{
		{"alice", 100}, // < "bob"
		{"bob", 200},   // == primeiro sep: desce no filho à direita
		{"carlos", 200},
		{"dan", 300},
		{"zara", 300},
	}
	for _, c := range cases {
		got := vp.FindChildVar([]byte(c.k))
		if got != c.want {
			t.Errorf("FindChild(%q) = %d, esperado %d", c.k, got, c.want)
		}
	}
}

func TestVarNode_SplitLeaf(t *testing.T) {
	_, left := newVarLeaf(t)

	// Insere chaves pra ter 10 slots (ordem lexicográfica: "a0".."a9")
	for i := 0; i < 10; i++ {
		k := []byte{'a', byte('0' + i)}
		if err := left.LeafInsertVar(k, int64(i)); err != nil {
			t.Fatal(err)
		}
	}

	var rightP pagestore.Page
	right := InitLeafPageVar(&rightP, pagestore.BodySize, bytes.Compare)

	sep := left.splitLeafIntoVar(right)

	if left.NumKeys() != 5 || right.NumKeys() != 5 {
		t.Fatalf("split desbalanceado: left=%d right=%d", left.NumKeys(), right.NumKeys())
	}

	// sep deveria ser "a5" (primeira chave da metade direita)
	if string(sep) != "a5" {
		t.Fatalf("sep esperado \"a5\", recebi %q", string(sep))
	}

	// left: a0..a4
	for i := 0; i < 5; i++ {
		k, v := left.LeafAtVar(i)
		want := string([]byte{'a', byte('0' + i)})
		if string(k) != want || v != int64(i) {
			t.Fatalf("left[%d]: esperado (%s,%d), recebi (%s,%d)", i, want, i, string(k), v)
		}
	}
	// right: a5..a9
	for i := 0; i < 5; i++ {
		k, v := right.LeafAtVar(i)
		want := string([]byte{'a', byte('0' + i + 5)})
		if string(k) != want || v != int64(i+5) {
			t.Fatalf("right[%d]: esperado (%s,%d), recebi (%s,%d)", i, want, i+5, string(k), v)
		}
	}
}

func TestVarNode_SplitInternal_MiddleKeyPromoted(t *testing.T) {
	_, left := newVarInternal(t, 100)

	// 7 separadores: c, d, e, f, g, h, i
	for i, k := range []string{"c", "d", "e", "f", "g", "h", "i"} {
		left.InsertSeparatorVar([]byte(k), pagestore.PageID(101+i))
	}

	var rightP pagestore.Page
	right := InitInternalPageVar(&rightP, pagestore.BodySize, pagestore.InvalidPageID, bytes.Compare)

	promoted := left.splitInternalIntoVar(right)

	// mid = 7/2 = 3 → promovida = slot[3] = "f"
	if string(promoted) != "f" {
		t.Fatalf("promoted esperado \"f\", recebi %q", string(promoted))
	}

	// left: c, d, e (3 slots, leftmost=100)
	if left.NumKeys() != 3 || right.NumKeys() != 3 {
		t.Fatalf("desbalanceado: left=%d right=%d", left.NumKeys(), right.NumKeys())
	}
	if lm := left.LeftmostChild(); lm != 100 {
		t.Fatalf("left.leftmost esperado 100, recebi %d", lm)
	}

	// right: leftmost = c_4 = child do slot[3] (f) = 104
	if lm := right.LeftmostChild(); lm != 104 {
		t.Fatalf("right.leftmost esperado 104, recebi %d", lm)
	}

	wantLeft := []string{"c", "d", "e"}
	for i, w := range wantLeft {
		k, _ := left.InternalAtVar(i)
		if string(k) != w {
			t.Fatalf("left[%d]: esperado %q, recebi %q", i, w, string(k))
		}
	}
	wantRight := []string{"g", "h", "i"}
	for i, w := range wantRight {
		k, _ := right.InternalAtVar(i)
		if string(k) != w {
			t.Fatalf("right[%d]: esperado %q, recebi %q", i, w, string(k))
		}
	}
}

func TestVarNode_OpenVariableNodePage(t *testing.T) {
	// Init, salva algo, reabre.
	var p pagestore.Page
	vp := InitLeafPageVar(&p, pagestore.BodySize, bytes.Compare)
	vp.LeafInsertVar([]byte("chave-persistida"), 42)

	// Reabre
	reopened, err := OpenVariableNodePage(&p, pagestore.BodySize, bytes.Compare)
	if err != nil {
		t.Fatal(err)
	}
	v, ok := reopened.LeafGetVar([]byte("chave-persistida"))
	if !ok || v != 42 {
		t.Fatalf("reopen: ok=%v v=%d", ok, v)
	}
}
