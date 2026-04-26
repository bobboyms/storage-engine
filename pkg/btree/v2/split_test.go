package v2

import (
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

func TestSplitLeafInto_HalfAndHalf(t *testing.T) {
	_, left := newLeafPage(t)
	for i := 0; i < 10; i++ {
		k := uint64(i * 2)
		if err := left.LeafInsert(k, int64(k)*10); err != nil {
			t.Fatal(err)
		}
	}

	var rightP pagestore.Page
	right := InitLeafPage(&rightP, pagestore.BodySize, nil)

	sep := left.splitLeafInto(right)

	if left.NumKeys() != 5 || right.NumKeys() != 5 {
		t.Fatalf("split desequilibrado: left=%d right=%d", left.NumKeys(), right.NumKeys())
	}
	if sep != 10 {
		t.Fatalf("separador expected 10, got %d", sep)
	}

	wantLeft := []uint64{0, 2, 4, 6, 8}
	for i, w := range wantLeft {
		k, v := left.LeafAt(i)
		if k != w || v != int64(w)*10 {
			t.Fatalf("left[%d]: expected (%d,%d), got (%d,%d)", i, w, int64(w)*10, k, v)
		}
	}
	wantRight := []uint64{10, 12, 14, 16, 18}
	for i, w := range wantRight {
		k, v := right.LeafAt(i)
		if k != w || v != int64(w)*10 {
			t.Fatalf("right[%d]: expected (%d,%d), got (%d,%d)", i, w, int64(w)*10, k, v)
		}
	}
}

func TestSplitLeafInto_PreservesSiblingLink(t *testing.T) {
	_, left := newLeafPage(t)
	h := left.header()
	h.nextLeafPageID = 99
	left.writeHeader(h)

	for i := 0; i < 6; i++ {
		left.LeafInsert(uint64(i), int64(i))
	}

	var rightP pagestore.Page
	right := InitLeafPage(&rightP, pagestore.BodySize, nil)
	_ = left.splitLeafInto(right)

	if right.NextLeafPageID() != 99 {
		t.Fatalf("right.NextLeaf expected 99 (herdado), got %d", right.NextLeafPageID())
	}
}

func TestSplitInternalInto_MiddleKeyPromoted(t *testing.T) {
	_, left := newInternalPage(t, 100)
	for i, k := range []uint64{10, 20, 30, 40, 50, 60, 70} {
		left.InsertSeparator(k, pagestore.PageID(101+i))
	}

	var rightP pagestore.Page
	right := InitInternalPage(&rightP, pagestore.BodySize, pagestore.InvalidPageID, nil)
	promoted := left.splitInternalInto(right)

	if promoted != 40 {
		t.Fatalf("promoted expected 40, got %d", promoted)
	}
	if left.NumKeys() != 3 || right.NumKeys() != 3 {
		t.Fatalf("split desbalanceado: left=%d right=%d", left.NumKeys(), right.NumKeys())
	}

	if lm := left.LeftmostChild(); lm != 100 {
		t.Fatalf("left.leftmost expected 100, got %d", lm)
	}
	wantLeftSlots := []struct {
		k uint64
		c pagestore.PageID
	}{{10, 101}, {20, 102}, {30, 103}}
	for i, w := range wantLeftSlots {
		k, c := left.InternalAt(i)
		if k != w.k || c != w.c {
			t.Fatalf("left[%d]: expected (%d,%d), got (%d,%d)", i, w.k, w.c, k, c)
		}
	}

	if lm := right.LeftmostChild(); lm != 104 {
		t.Fatalf("right.leftmost expected 104, got %d", lm)
	}
	wantRightSlots := []struct {
		k uint64
		c pagestore.PageID
	}{{50, 105}, {60, 106}, {70, 107}}
	for i, w := range wantRightSlots {
		k, c := right.InternalAt(i)
		if k != w.k || c != w.c {
			t.Fatalf("right[%d]: expected (%d,%d), got (%d,%d)", i, w.k, w.c, k, c)
		}
	}
}

func TestSplitLeafInto_OddNumberOfKeys(t *testing.T) {
	_, left := newLeafPage(t)
	for i := 0; i < 7; i++ {
		left.LeafInsert(uint64(i), int64(i))
	}

	var rightP pagestore.Page
	right := InitLeafPage(&rightP, pagestore.BodySize, nil)
	sep := left.splitLeafInto(right)

	if left.NumKeys() != 3 || right.NumKeys() != 4 {
		t.Fatalf("split ímpar: left=%d right=%d", left.NumKeys(), right.NumKeys())
	}
	if sep != 3 {
		t.Fatalf("separador expected 3, got %d", sep)
	}
}
