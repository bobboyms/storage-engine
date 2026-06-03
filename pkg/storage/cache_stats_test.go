package storage

import (
	"context"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// TestStats_AggregatesBufferPoolCacheMetrics verifies the engine surfaces
// aggregated buffer-pool cache counters (hits/misses/evictions and the
// derived hit ratio) so operators can see cache effectiveness.
func TestStats_AggregatesBufferPoolCacheMetrics(t *testing.T) {
	se := newObservabilityTestEngine(t, Options{})
	ctx := context.Background()

	// Write a handful of rows, then read them back repeatedly to drive
	// cache lookups (the warm reads should register as hits).
	for i := 1; i <= 5; i++ {
		if err := se.Put(ctx, "obs", "id", types.IntKey(i), `{"id":`+itoaCache(i)+`}`); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	for round := 0; round < 3; round++ {
		for i := 1; i <= 5; i++ {
			if _, _, err := getDocString(t, se, "obs", "id", types.IntKey(i)); err != nil {
				t.Fatalf("get %d: %v", i, err)
			}
		}
	}

	st := se.Stats()
	if st.CacheHits+st.CacheMisses == 0 {
		t.Fatalf("expected some cache lookups, got hits=%d misses=%d", st.CacheHits, st.CacheMisses)
	}
	if st.CacheHits == 0 {
		t.Fatalf("expected cache hits from repeated reads, got 0 (misses=%d)", st.CacheMisses)
	}
	if r := st.CacheHitRatio; r < 0 || r > 1 {
		t.Fatalf("CacheHitRatio out of range: %f", r)
	}
}

func itoaCache(i int) string {
	return string(rune('0' + i))
}
