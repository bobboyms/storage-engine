package pagestore

import "testing"

// TestBufferPool_StatsHitsMissesEvictions verifies the buffer pool
// tracks cache hits, misses, and evictions so callers can compute the
// hit ratio — the primary signal of cache effectiveness.
func TestBufferPool_StatsHitsMissesEvictions(t *testing.T) {
	// Capacity 2 so the third distinct page forces an eviction.
	bp, _ := newPoolWithFile(t, 2)

	id0 := allocAndWrite(t, bp, 0x10)
	id1 := allocAndWrite(t, bp, 0x20)
	id2 := allocAndWrite(t, bp, 0x30)

	// allocAndWrite goes through NewPage + FlushAll, not Fetch, so the
	// hit/miss counters start clean for the lookups below.
	start := bp.Stats()

	// First fetch of id0: it may or may not still be cached depending on
	// prior allocations, so drive a deterministic sequence instead.
	mustFetch := func(id PageID) {
		h, err := bp.Fetch(id)
		if err != nil {
			t.Fatalf("fetch %d: %v", id, err)
		}
		h.Release()
	}

	// Warm the cache with id1 and id2 (capacity 2). These are misses if
	// not resident.
	mustFetch(id1)
	mustFetch(id2)
	// Re-fetch id2: now resident → hit.
	mustFetch(id2)
	// Fetch id0: not resident (capacity 2 holds id1,id2) → miss + eviction.
	mustFetch(id0)

	s := bp.Stats()
	hits := s.Hits - start.Hits
	misses := s.Misses - start.Misses
	evictions := s.Evictions - start.Evictions

	if hits < 1 {
		t.Fatalf("expected at least 1 hit, got %d", hits)
	}
	if misses < 1 {
		t.Fatalf("expected at least 1 miss, got %d", misses)
	}
	if evictions < 1 {
		t.Fatalf("expected at least 1 eviction (capacity 2, 3 pages), got %d", evictions)
	}

	// HitRatio is hits / (hits+misses) and must be within [0,1].
	if r := s.HitRatio(); r < 0 || r > 1 {
		t.Fatalf("HitRatio out of range: %f", r)
	}
}
