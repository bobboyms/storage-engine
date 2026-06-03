package storage

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// getForUpdateString is a small decoding wrapper mirroring getDocStringWTx
// but for the locking read path.
func getForUpdateString(t testing.TB, tx *WriteTransaction, table, idx string, key types.Comparable) (string, bool, error) {
	t.Helper()
	raw, found, err := tx.GetForUpdate(context.Background(), table, idx, key)
	if err != nil || !found {
		return "", found, err
	}
	return legacyDecode(t, raw), true, nil
}

// TestWriteTransaction_GetForUpdate_ReadsLatestCommitted verifies that,
// unlike GetBytes (which honors the transaction's fixed snapshot),
// GetForUpdate returns the latest committed version of the row. This is
// the property that lets a locking read see another transaction's
// just-committed change and is what makes FOR-UPDATE locking actually
// prevent write skew under snapshot isolation.
func TestWriteTransaction_GetForUpdate_ReadsLatestCommitted(t *testing.T) {
	ctx := context.Background()
	se := openIsolationTestEngine(t)
	defer se.Close()

	if err := se.Put(ctx, "accounts", "id", types.IntKey(1), `{"id":1,"v":"first"}`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tx := se.BeginWriteTransaction()

	// Another writer commits a new version after tx fixed its snapshot.
	if err := se.Put(ctx, "accounts", "id", types.IntKey(1), `{"id":1,"v":"second"}`); err != nil {
		t.Fatalf("concurrent put: %v", err)
	}

	// Snapshot read still sees the old version.
	if doc, found, err := getDocStringWTx(t, tx, "accounts", "id", types.IntKey(1)); err != nil || !found || doc != `{"id":1,"v":"first"}` {
		t.Fatalf("GetBytes should see snapshot 'first', got found=%v doc=%q err=%v", found, doc, err)
	}

	// Locking read sees the latest committed version.
	if doc, found, err := getForUpdateString(t, tx, "accounts", "id", types.IntKey(1)); err != nil || !found || doc != `{"id":1,"v":"second"}` {
		t.Fatalf("GetForUpdate should see latest 'second', got found=%v doc=%q err=%v", found, doc, err)
	}

	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
}

// TestWriteTransaction_GetForUpdate_PreventsWriteSkew demonstrates the
// classic on-call write skew being prevented when both transactions take
// FOR-UPDATE locks on the rows their invariant depends on. tx1 holds the
// locks and commits; tx2 blocks until tx1 releases them, then observes
// the updated state and refuses to violate the "at least one on call"
// rule.
func TestWriteTransaction_GetForUpdate_PreventsWriteSkew(t *testing.T) {
	ctx := context.Background()
	se := openIsolationTestEngine(t)
	defer se.Close()

	if err := se.Put(ctx, "shifts", "id", types.IntKey(1), `{"id":1,"on_call":true}`); err != nil {
		t.Fatalf("seed 1: %v", err)
	}
	if err := se.Put(ctx, "shifts", "id", types.IntKey(2), `{"id":2,"on_call":true}`); err != nil {
		t.Fatalf("seed 2: %v", err)
	}

	// goOffCall reads both rows FOR UPDATE; only removes `self` from
	// on-call if at least one other doctor remains on call. Returns
	// whether it changed state.
	goOffCall := func(tx *WriteTransaction, self int) (bool, error) {
		onCall := 0
		for _, id := range []int{1, 2} {
			doc, found, err := getForUpdateString(t, tx, "shifts", "id", types.IntKey(id))
			if err != nil {
				return false, err
			}
			if found && contains(doc, `"on_call":true`) {
				onCall++
			}
		}
		if onCall <= 1 {
			return false, nil // refuse: would leave nobody on call
		}
		if err := tx.Put(ctx, "shifts", "id", types.IntKey(self), `{"id":`+strconv.Itoa(self)+`,"on_call":false}`); err != nil {
			return false, err
		}
		return true, nil
	}

	tx1 := se.BeginWriteTransaction()

	tx1Changed, err := goOffCall(tx1, 1)
	if err != nil {
		t.Fatalf("tx1 goOffCall: %v", err)
	}
	if !tx1Changed {
		t.Fatalf("tx1 should be allowed to go off call (2 on call)")
	}

	// tx2 runs concurrently and must block on the row locks tx1 holds,
	// proceeding only after tx1 commits.
	var (
		wg         sync.WaitGroup
		tx2Changed bool
		tx2Err     error
	)
	tx2Started := make(chan struct{})
	wg.Go(func() {
		tx2 := se.BeginWriteTransaction()
		close(tx2Started)
		tx2Changed, tx2Err = goOffCall(tx2, 2)
		if tx2Err == nil {
			tx2Err = tx2.Commit(ctx)
		} else {
			_ = tx2.Rollback(ctx)
		}
	})

	<-tx2Started
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("tx1 commit: %v", err)
	}
	wg.Wait()

	if tx2Err != nil && !errors.Is(tx2Err, ErrSerializationConflict) {
		t.Fatalf("tx2 unexpected error: %v", tx2Err)
	}
	if tx2Changed {
		t.Fatalf("tx2 must NOT go off call: tx1 already did, only one remains")
	}

	// Final invariant: exactly one doctor still on call.
	d1, _, _ := getDocString(t, se, "shifts", "id", types.IntKey(1))
	d2, _, _ := getDocString(t, se, "shifts", "id", types.IntKey(2))
	onCall := 0
	if contains(d1, `"on_call":true`) {
		onCall++
	}
	if contains(d2, `"on_call":true`) {
		onCall++
	}
	if onCall != 1 {
		t.Fatalf("write skew not prevented: expected exactly 1 on call, got %d (d1=%q d2=%q)", onCall, d1, d2)
	}
}
