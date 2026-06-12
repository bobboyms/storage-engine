package v2

import (
	"context"
	"math"
	"testing"
)

// TestHealPoisonedPageLSNs pins the fsck repair for pages stamped with the
// MaxUint64 sentinel by a pre-clamp vacuum: the sentinel is replaced with the
// supplied LSN, healthy pages are untouched, and the result passes the scrub.
func TestHealPoisonedPageLSNs(t *testing.T) {
	h := newCheckedHeap(t)
	ctx := context.Background()

	rid, err := h.Write([]byte("doc"), 3, NoRecordID)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	pid, _ := DecodeRecordID(rid)

	handle, err := h.bp.FetchForWrite(pid)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	handle.Page().AdvancePageLSN(math.MaxUint64)
	handle.MarkDirty()
	handle.Release()
	if err := h.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	healed, err := h.HealPoisonedPageLSNs(ctx, 9)
	if err != nil {
		t.Fatalf("HealPoisonedPageLSNs: %v", err)
	}
	if healed != 1 {
		t.Fatalf("healed = %d, want 1", healed)
	}

	if issues := mustCheck(t, h); len(issues) != 0 {
		t.Fatalf("heap still reports issues after healing: %v", issues)
	}

	// Idempotent: a second pass finds nothing to heal.
	healed, err = h.HealPoisonedPageLSNs(ctx, 9)
	if err != nil {
		t.Fatalf("second heal: %v", err)
	}
	if healed != 0 {
		t.Fatalf("second heal touched %d pages, want 0", healed)
	}
}
