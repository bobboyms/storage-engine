package storage

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestLockManager_DeadlockAbortsYoungestVictim(t *testing.T) {
	t.Parallel()

	lm := NewLockManager(LockManagerConfig{
		WaitTimeout: 250 * time.Millisecond,
	})

	resourceA := lockResourceID("users", "id", "1")
	resourceB := lockResourceID("users", "id", "2")

	if err := lm.Acquire(1, resourceA); err != nil {
		t.Fatalf("tx1 acquire A: %v", err)
	}
	if err := lm.Acquire(2, resourceB); err != nil {
		t.Fatalf("tx2 acquire B: %v", err)
	}

	tx1Ready := make(chan struct{})
	tx1Result := make(chan error, 1)
	go func() {
		close(tx1Ready)
		tx1Result <- lm.Acquire(1, resourceB)
	}()
	<-tx1Ready

	time.Sleep(20 * time.Millisecond)

	tx2Err := lm.Acquire(2, resourceA)
	if !errors.Is(tx2Err, ErrDeadlockVictim) {
		t.Fatalf("expected deadlock victim error, got %v", tx2Err)
	}

	select {
	case err := <-tx1Result:
		if err != nil {
			t.Fatalf("tx1 should survive deadlock, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("tx1 did not acquire second lock after victim abort")
	}

	lm.ReleaseAll(1)

	if err := lm.Acquire(3, resourceA); err != nil {
		t.Fatalf("tx3 acquire A after release: %v", err)
	}
	if err := lm.Acquire(3, resourceB); err != nil {
		t.Fatalf("tx3 acquire B after release: %v", err)
	}
	lm.ReleaseAll(3)
}

func TestLockManager_TimesOutWaiting(t *testing.T) {
	t.Parallel()

	lm := NewLockManager(LockManagerConfig{
		WaitTimeout: 40 * time.Millisecond,
	})

	resource := lockResourceID("users", "id", "1")
	if err := lm.Acquire(1, resource); err != nil {
		t.Fatalf("tx1 acquire: %v", err)
	}
	defer lm.ReleaseAll(1)

	start := time.Now()
	err := lm.Acquire(2, resource)
	if !errors.Is(err, ErrLockWaitTimeout) {
		t.Fatalf("expected wait timeout, got %v", err)
	}
	if waited := time.Since(start); waited < 30*time.Millisecond {
		t.Fatalf("lock wait returned too early: %s", waited)
	}
}

func TestLockManager_MakesProgressUnderContention(t *testing.T) {
	t.Parallel()

	lm := NewLockManager(LockManagerConfig{
		WaitTimeout: 500 * time.Millisecond,
	})

	resource := lockResourceID("users", "id", "hot")
	const workers = 12
	const iterations = 20

	var wg sync.WaitGroup
	errCh := make(chan error, workers*iterations)

	for worker := 0; worker < workers; worker++ {
		workerID := worker + 1
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				txID := uint64(workerID*1000 + i)
				if err := lm.Acquire(txID, resource); err != nil {
					errCh <- err
					return
				}
				time.Sleep(time.Millisecond)
				lm.ReleaseAll(txID)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("contention test did not complete")
	}

	close(errCh)
	for err := range errCh {
		t.Fatalf("unexpected contention error: %v", err)
	}
}
