package pagestore

import (
	"math"
	"testing"
)

func pageWithLSN(t *testing.T, lsn uint64) *Page {
	t.Helper()
	var p Page
	hdr, err := p.GetHeader()
	if err != nil {
		t.Fatalf("get header: %v", err)
	}
	hdr.PageLSN = lsn
	p.SetHeader(hdr)
	return &p
}

func pageLSN(t *testing.T, p *Page) uint64 {
	t.Helper()
	hdr, err := p.GetHeader()
	if err != nil {
		t.Fatalf("get header: %v", err)
	}
	return hdr.PageLSN
}

func TestAdvancePageLSN_Monotonic(t *testing.T) {
	p := pageWithLSN(t, 10)
	p.AdvancePageLSN(7)
	if got := pageLSN(t, p); got != 10 {
		t.Fatalf("PageLSN = %d after advancing with a lower LSN, want 10", got)
	}
	p.AdvancePageLSN(12)
	if got := pageLSN(t, p); got != 12 {
		t.Fatalf("PageLSN = %d, want 12", got)
	}
}

// TestAdvancePageLSN_HealsPoisonedLSN pins the recovery path for pages whose
// header carries the MaxUint64 sentinel (stamped by a pre-clamp vacuum). The
// sentinel is never a real LSN, so the next real LSN must replace it instead
// of being discarded as "older" — otherwise the page stays poisoned forever
// and keeps skipping physical redo.
func TestAdvancePageLSN_HealsPoisonedLSN(t *testing.T) {
	p := pageWithLSN(t, math.MaxUint64)
	p.AdvancePageLSN(42)
	if got := pageLSN(t, p); got != 42 {
		t.Fatalf("PageLSN = %d after advancing a poisoned page, want 42", got)
	}
}

func TestIsPoisonedPageLSN(t *testing.T) {
	if !IsPoisonedPageLSN(math.MaxUint64) {
		t.Fatal("IsPoisonedPageLSN(MaxUint64) = false, want true")
	}
	if IsPoisonedPageLSN(0) || IsPoisonedPageLSN(math.MaxUint64-1) {
		t.Fatal("IsPoisonedPageLSN must only match the MaxUint64 sentinel")
	}
}
