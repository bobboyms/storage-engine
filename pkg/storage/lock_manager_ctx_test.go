package storage

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestLockManager_AcquireCancelledBeforeStart(t *testing.T) {
	t.Parallel()

	lm := NewLockManager(LockManagerConfig{WaitTimeout: time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := lm.Acquire(ctx, 1, lockResourceID("users", "id", "1"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestLockManager_AcquireCancelledWhileWaiting(t *testing.T) {
	t.Parallel()

	lm := NewLockManager(LockManagerConfig{WaitTimeout: 5 * time.Second})
	resource := lockResourceID("users", "id", "1")

	if err := lm.Acquire(context.Background(), 1, resource); err != nil {
		t.Fatalf("tx1 acquire: %v", err)
	}
	defer lm.ReleaseAll(1)

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- lm.Acquire(ctx, 2, resource)
	}()
	<-started
	time.Sleep(20 * time.Millisecond) // ensure tx2 actually parked itself
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Acquire did not honor ctx cancellation within 1s")
	}
}

// TestLockManager_AcquireRaceCancelVsGrant locks in the race protection
// described in spec section A.4: if grantNextWaiterLocked and ctx
// cancellation fire concurrently, the caller must either accept the
// grant (and own the lock) or back out cleanly without leaking it.
func TestLockManager_AcquireRaceCancelVsGrant(t *testing.T) {
	t.Parallel()

	const iterations = 50
	for i := 0; i < iterations; i++ {
		lm := NewLockManager(LockManagerConfig{WaitTimeout: 2 * time.Second})
		resource := lockResourceID("users", "id", "1")

		if err := lm.Acquire(context.Background(), 1, resource); err != nil {
			t.Fatalf("seed acquire: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			result <- lm.Acquire(ctx, 2, resource)
		}()

		// Race grant vs cancel.
		time.Sleep(10 * time.Millisecond)
		go cancel()
		lm.Release(1, resource)

		err := <-result
		switch {
		case err == nil:
			// tx2 owns the lock now: must be releasable cleanly.
			lm.ReleaseAll(2)
		case errors.Is(err, context.Canceled):
			// tx2 backed out: lock stays released, no holder leak.
		default:
			t.Fatalf("unexpected race outcome: %v", err)
		}

		// Sanity: a third tx can always acquire immediately afterwards.
		if err := lm.Acquire(context.Background(), 3, resource); err != nil {
			t.Fatalf("post-race tx3 acquire (iter %d): %v", i, err)
		}
		lm.ReleaseAll(3)
	}
}

func TestLockManager_WaitTimeoutMinusOneOnlyContextEndsAcquire(t *testing.T) {
	t.Parallel()

	lm := NewLockManager(LockManagerConfig{WaitTimeout: -1})
	resource := lockResourceID("users", "id", "1")

	if err := lm.Acquire(context.Background(), 1, resource); err != nil {
		t.Fatalf("tx1 acquire: %v", err)
	}
	defer lm.ReleaseAll(1)

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := lm.Acquire(ctx, 2, resource)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	// Sanity: we waited at least the ctx timeout, proving the engine
	// did not time out on its own.
	if elapsed < 60*time.Millisecond {
		t.Fatalf("Acquire returned too early: %s", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Acquire blocked too long; timer was supposed to be disabled: %s", elapsed)
	}
}

// TestLockManager_ConcurrentCancelDoesNotLeakLock stresses the
// cancel-vs-grant invariant under load.
func TestLockManager_ConcurrentCancelDoesNotLeakLock(t *testing.T) {
	t.Parallel()

	lm := NewLockManager(LockManagerConfig{WaitTimeout: 2 * time.Second})
	resource := lockResourceID("users", "id", "1")

	if err := lm.Acquire(context.Background(), 1, resource); err != nil {
		t.Fatalf("seed acquire: %v", err)
	}

	const waiters = 20
	var acquired atomic.Int64
	results := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		txID := uint64(100 + i)
		go func() {
			err := lm.Acquire(ctx, txID, resource)
			if err == nil {
				acquired.Add(1)
				lm.ReleaseAll(txID)
			}
			results <- err
		}()
		// Cancel half of them shortly after they start waiting.
		if i%2 == 0 {
			go func() {
				time.Sleep(5 * time.Millisecond)
				cancel()
			}()
		} else {
			defer cancel()
		}
	}

	lm.Release(1, resource)
	for i := 0; i < waiters; i++ {
		<-results
	}
	// After everything drains, the resource must be releasable and
	// re-acquirable — proves no leaked holder.
	if err := lm.Acquire(context.Background(), 999, resource); err != nil {
		t.Fatalf("post-storm acquire: %v", err)
	}
	lm.ReleaseAll(999)
}
