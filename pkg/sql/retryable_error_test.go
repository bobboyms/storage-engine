package sql

import (
	"context"
	"errors"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/storage"
)

// TestDeadlock_IsDetectableThroughSQL pins that a retryable storage error
// stays detectable with errors.Is after the SQL layer wraps it. Two
// transactions cross-lock two rows FOR UPDATE, forcing a waits-for cycle; the
// engine aborts a victim with a deadlock error. An application (and the
// banking simulation) must be able to recognize that error to retry — which
// requires the SQL wrapper to preserve the cause chain, not flatten it.
func TestDeadlock_IsDetectableThroughSQL(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(ctx, "CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for _, id := range []int{1, 2} {
		if _, err := db.Exec(ctx, "INSERT INTO accounts (id, balance) VALUES (?, 0)", id); err != nil {
			t.Fatalf("insert %d: %v", id, err)
		}
	}

	t1 := db.Begin()
	t2 := db.Begin()
	defer func() { _ = t1.Rollback(ctx) }()
	defer func() { _ = t2.Rollback(ctx) }()

	// T1 locks row 1; T2 locks row 2.
	if _, err := t1.Query(ctx, "SELECT balance FROM accounts WHERE id = 1 FOR UPDATE"); err != nil {
		t.Fatalf("t1 lock 1: %v", err)
	}
	if _, err := t2.Query(ctx, "SELECT balance FROM accounts WHERE id = 2 FOR UPDATE"); err != nil {
		t.Fatalf("t2 lock 2: %v", err)
	}

	// Cross: T1 wants row 2 (held by T2), T2 wants row 1 (held by T1) -> a
	// waits-for cycle the deadlock detector must break by aborting a victim.
	e1 := make(chan error, 1)
	e2 := make(chan error, 1)
	go func() {
		_, err := t1.Query(ctx, "SELECT balance FROM accounts WHERE id = 2 FOR UPDATE")
		e1 <- err
	}()
	go func() {
		_, err := t2.Query(ctx, "SELECT balance FROM accounts WHERE id = 1 FOR UPDATE")
		e2 <- err
	}()
	err1 := <-e1
	err2 := <-e2

	// Exactly the victim sees an error; it must be recognizable as a deadlock.
	victimErr := err1
	if victimErr == nil {
		victimErr = err2
	}
	if victimErr == nil {
		t.Fatal("cross-lock did not produce a deadlock victim error")
	}
	if !errors.Is(victimErr, storage.ErrDeadlockVictim) {
		t.Fatalf("deadlock not detectable via errors.Is(err, ErrDeadlockVictim); err = %v", victimErr)
	}
}
