package storage

import (
	"math"
	"testing"
)

func TestLSNTracker_NextAdvances(t *testing.T) {
	lt := NewLSNTracker(7)
	if got := lt.Next(); got != 8 {
		t.Fatalf("Next() = %d, want 8", got)
	}
	if got := lt.Current(); got != 8 {
		t.Fatalf("Current() = %d, want 8", got)
	}
}

// TestLSNTracker_NextPanicsOnOverflow pins the fail-stop behavior: an LSN
// counter at MaxUint64 (e.g. recovered from a WAL poisoned by a pre-clamp
// vacuum) must refuse to allocate rather than wrap to 0, where every new
// snapshot would silently stop seeing committed data.
func TestLSNTracker_NextPanicsOnOverflow(t *testing.T) {
	lt := NewLSNTracker(math.MaxUint64)
	defer func() {
		if recover() == nil {
			t.Fatal("Next() at MaxUint64 wrapped silently, want panic (fail-stop)")
		}
	}()
	_ = lt.Next()
}
