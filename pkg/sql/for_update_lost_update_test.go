package sql

import (
	"context"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// TestForUpdate_ReturnsLatestCommittedNotSnapshot pins the lost-update fix:
// a SELECT ... FOR UPDATE inside a transaction must return the latest
// committed value of the locked row, not the transaction's (now stale)
// snapshot. A read-modify-write built on FOR UPDATE — the canonical money
// transfer — otherwise silently loses the other transaction's update.
//
// The banking simulation surfaced this as money disappearing under
// concurrency with no crash involved.
func TestForUpdate_ReturnsLatestCommittedNotSnapshot(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(ctx, "CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO accounts (id, balance) VALUES (1, 100)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Both transactions take their snapshot now, while balance == 100.
	t1 := db.Begin()
	t2 := db.Begin()

	// T1 increments by 10 under FOR UPDATE and commits.
	rs1, err := t1.Query(ctx, "SELECT balance FROM accounts WHERE id = 1 FOR UPDATE")
	if err != nil {
		t.Fatalf("t1 select for update: %v", err)
	}
	v1 := int64(rs1.Rows[0][0].(types.IntKey))
	if v1 != 100 {
		t.Fatalf("t1 read balance %d, want 100", v1)
	}
	if _, err := t1.Exec(ctx, "UPDATE accounts SET balance = ? WHERE id = 1", v1+10); err != nil {
		t.Fatalf("t1 update: %v", err)
	}
	if err := t1.Commit(ctx); err != nil {
		t.Fatalf("t1 commit: %v", err)
	}

	// T2 now reads the same row FOR UPDATE. T1 has committed balance=110, so a
	// correct FOR UPDATE must observe 110 (not T2's snapshot of 100). T2 then
	// adds 10 and commits.
	rs2, err := t2.Query(ctx, "SELECT balance FROM accounts WHERE id = 1 FOR UPDATE")
	if err != nil {
		t.Fatalf("t2 select for update: %v", err)
	}
	v2 := int64(rs2.Rows[0][0].(types.IntKey))
	if v2 != 110 {
		t.Fatalf("t2 FOR UPDATE read balance %d, want 110 (latest committed, not the stale snapshot 100)", v2)
	}
	if _, err := t2.Exec(ctx, "UPDATE accounts SET balance = ? WHERE id = 1", v2+10); err != nil {
		t.Fatalf("t2 update: %v", err)
	}
	if err := t2.Commit(ctx); err != nil {
		t.Fatalf("t2 commit: %v", err)
	}

	// Both increments must survive: 100 + 10 + 10 = 120.
	rs, err := db.Query(ctx, "SELECT balance FROM accounts WHERE id = 1")
	if err != nil {
		t.Fatalf("final read: %v", err)
	}
	final := int64(rs.Rows[0][0].(types.IntKey))
	if final != 120 {
		t.Fatalf("final balance %d, want 120 (a concurrent increment was lost)", final)
	}
}
