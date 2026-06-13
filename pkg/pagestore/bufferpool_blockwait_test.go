package pagestore

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// TestBufferPool_WaitsForFrameInsteadOfFailing pins the concurrency fix: when
// every frame is pinned, a fetch for a new page must wait for a frame to be
// released rather than failing immediately with ErrBufferPoolFull. This is
// the availability bug the banking simulation surfaced — a burst of
// concurrent pins must degrade latency, not brick the pool.
func TestBufferPool_WaitsForFrameInsteadOfFailing(t *testing.T) {
	pf, err := NewPageFile(filepath.Join(t.TempDir(), "wait.pages"), nil)
	if err != nil {
		t.Fatalf("create page file: %v", err)
	}
	defer func() { _ = pf.Close() }()
	// Pre-create three pages on disk so fetches are hits-from-disk.
	for id := PageID(1); id <= 3; id++ {
		var p Page
		if err := pf.WritePage(id, &p); err != nil {
			t.Fatalf("write page %d: %v", id, err)
		}
	}

	bp := NewBufferPool(pf, 2) // capacity 2

	// Fill the pool: both frames pinned.
	h1, err := bp.Fetch(1)
	if err != nil {
		t.Fatalf("fetch 1: %v", err)
	}
	h2, err := bp.Fetch(2)
	if err != nil {
		t.Fatalf("fetch 2: %v", err)
	}

	// A concurrent fetch for a third page cannot get a frame yet; it must
	// block, not fail.
	type result struct {
		h   *PageHandle
		err error
	}
	done := make(chan result, 1)
	go func() {
		h, err := bp.Fetch(3)
		done <- result{h, err}
	}()

	// Give the waiter time to discover the pool is full and start waiting.
	select {
	case r := <-done:
		t.Fatalf("fetch 3 returned early (err=%v) instead of waiting for a frame", r.err)
	case <-time.After(100 * time.Millisecond):
	}

	// Release one frame; the waiter must now succeed.
	h1.Release()
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("fetch 3 after release failed: %v", r.err)
		}
		r.h.Release()
	case <-time.After(5 * time.Second):
		t.Fatal("fetch 3 did not complete after a frame was released")
	}
	h2.Release()
}

// TestBufferPool_FailsAfterWaitTimeout pins that the fail-stop is preserved:
// when frames stay pinned past the wait timeout (genuine over-subscription),
// the fetch still returns ErrBufferPoolFull rather than blocking forever.
func TestBufferPool_FailsAfterWaitTimeout(t *testing.T) {
	pf, err := NewPageFile(filepath.Join(t.TempDir(), "timeout.pages"), nil)
	if err != nil {
		t.Fatalf("create page file: %v", err)
	}
	defer func() { _ = pf.Close() }()
	for id := PageID(1); id <= 2; id++ {
		var p Page
		if err := pf.WritePage(id, &p); err != nil {
			t.Fatalf("write page %d: %v", id, err)
		}
	}

	bp := NewBufferPool(pf, 1)
	bp.SetFrameWaitTimeout(50 * time.Millisecond)

	h1, err := bp.Fetch(1)
	if err != nil {
		t.Fatalf("fetch 1: %v", err)
	}
	defer h1.Release() // never released before the second fetch times out

	start := time.Now()
	_, err = bp.Fetch(2)
	if !errors.Is(err, ErrBufferPoolFull) {
		t.Fatalf("fetch 2 error = %v, want ErrBufferPoolFull", err)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("fetch 2 failed after %v, want it to wait ~50ms first", elapsed)
	}
}
