package v2

import (
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// Helper: cria uma page de internal node empty com cmp default.
func newInternalPage(t *testing.T, leftmost pagestore.PageID) (*pagestore.Page, *NodePage) {
	t.Helper()
	var p pagestore.Page
	np := InitInternalPage(&p, pagestore.BodySize, leftmost, nil)
	return &p, np
}

func TestInternalPage_InitEmpty(t *testing.T) {
	_, np := newInternalPage(t, 42)

	if np.IsLeaf() {
		t.Fatal("internal node reportando IsLeaf=true")
	}
	if np.NumKeys() != 0 {
		t.Fatalf("internal freshly initialized should have 0 keys, tem %d", np.NumKeys())
	}
	if lm := np.LeftmostChild(); lm != 42 {
		t.Fatalf("leftmost child expected 42, got %d", lm)
	}
}

func TestInternalPage_InsertSeparatorAndFindChild(t *testing.T) {
	// Árvore lógica:
	//   leftmost=100 | sep=10 -> 200 | sep=20 -> 300 | sep=30 -> 400
	// Ranges:
	//   child 100: key < 10
	//   child 200: 10 <= key < 20
	//   child 300: 20 <= key < 30
	//   child 400: 30 <= key
	_, np := newInternalPage(t, 100)

	// Insere fora de ordem — InsertSeparator must manter ordenação
	if err := np.InsertSeparator(30, 400); err != nil {
		t.Fatal(err)
	}
	if err := np.InsertSeparator(10, 200); err != nil {
		t.Fatal(err)
	}
	if err := np.InsertSeparator(20, 300); err != nil {
		t.Fatal(err)
	}

	if np.NumKeys() != 3 {
		t.Fatalf("NumKeys expected 3, got %d", np.NumKeys())
	}

	cases := []struct {
		key        uint64
		wantChild  pagestore.PageID
		descricao  string
	}{
		{0, 100, "key antes de todos os seps → leftmost"},
		{5, 100, "key ainda < 10 → leftmost"},
		{10, 200, "key == primeiro sep → desce na primeira faixa à direita"},
		{15, 200, "meio da primeira faixa"},
		{20, 300, "== segundo sep"},
		{25, 300, "meio da segunda faixa"},
		{30, 400, "== terceiro sep → última faixa"},
		{999, 400, "key acima de todos → última faixa"},
	}

	for _, c := range cases {
		got := np.FindChild(c.key)
		if got != c.wantChild {
			t.Errorf("%s: FindChild(%d) = %d, expected %d", c.descricao, c.key, got, c.wantChild)
		}
	}
}

func TestInternalPage_MaxCapacity(t *testing.T) {
	_, np := newInternalPage(t, 999)

	max := np.MaxInternalSlots()
	for i := 0; i < max; i++ {
		if err := np.InsertSeparator(uint64(i*10), pagestore.PageID(100+i)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	if err := np.InsertSeparator(uint64(max*10), 9999); !errorsIs(err, ErrInternalFull) {
		t.Fatalf("expected ErrInternalFull, got: %v", err)
	}
}
