package sql

import (
	"context"
	"testing"
)

func TestParseSelectForUpdate(t *testing.T) {
	sel := parseSelect(t, "SELECT * FROM users WHERE id = 1 FOR UPDATE")
	if !sel.ForUpdate {
		t.Fatal("ForUpdate = false, want true")
	}
}

func TestTxInsertCommitVisibility(t *testing.T) {
	e := newExecutor(t)
	ctx := context.Background()

	tx := e.Begin()
	if _, err := tx.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (5, 'erin', 22)"); err != nil {
		t.Fatalf("tx.Exec INSERT: %v", err)
	}

	// Not visible to an auto-commit read before commit.
	rs, _ := e.Query(ctx, "SELECT id FROM users WHERE id = 5")
	if len(rs.Rows) != 0 {
		t.Fatalf("uncommitted insert visible externally: %d rows", len(rs.Rows))
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	rs, _ = e.Query(ctx, "SELECT id FROM users WHERE id = 5")
	if len(rs.Rows) != 1 {
		t.Fatalf("committed insert not visible: %d rows", len(rs.Rows))
	}
}

func TestTxReadYourWrites(t *testing.T) {
	e := newExecutor(t)
	ctx := context.Background()

	tx := e.Begin()
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (5, 'erin', 22)"); err != nil {
		t.Fatalf("tx.Exec INSERT: %v", err)
	}
	if _, err := tx.Exec(ctx, "UPDATE users SET age = 99 WHERE id = 2"); err != nil {
		t.Fatalf("tx.Exec UPDATE: %v", err)
	}

	rs, err := tx.Query(ctx, "SELECT id, age FROM users WHERE id = 5")
	if err != nil {
		t.Fatalf("tx.Query: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("read-your-writes insert not seen: %d rows", len(rs.Rows))
	}
	if got := intColumn(t, rs, "age"); got[0] != 22 {
		t.Fatalf("age = %d, want 22", got[0])
	}

	rs, _ = tx.Query(ctx, "SELECT age FROM users WHERE id = 2")
	if got := intColumn(t, rs, "age"); len(got) != 1 || got[0] != 99 {
		t.Fatalf("read-your-writes update not seen: ages=%v", got)
	}
}

func TestTxRollbackDiscards(t *testing.T) {
	e := newExecutor(t)
	ctx := context.Background()

	tx := e.Begin()
	if _, err := tx.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (5, 'erin', 22)"); err != nil {
		t.Fatalf("tx.Exec: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	rs, _ := e.Query(ctx, "SELECT id FROM users WHERE id = 5")
	if len(rs.Rows) != 0 {
		t.Fatalf("rolled-back insert visible: %d rows", len(rs.Rows))
	}
}

func TestTxDeleteWithinTransaction(t *testing.T) {
	e := newExecutor(t)
	ctx := context.Background()

	tx := e.Begin()
	if _, err := tx.Exec(ctx, "DELETE FROM users WHERE id = 3"); err != nil {
		t.Fatalf("tx.Exec DELETE: %v", err)
	}
	// Read-your-writes: gone inside the tx.
	rs, _ := tx.Query(ctx, "SELECT id FROM users WHERE id = 3")
	if len(rs.Rows) != 0 {
		t.Fatalf("deleted row still visible in tx: %d rows", len(rs.Rows))
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	rs, _ = e.Query(ctx, "SELECT id FROM users WHERE id = 3")
	if len(rs.Rows) != 0 {
		t.Fatalf("deleted row visible after commit: %d rows", len(rs.Rows))
	}
}

func TestTxSavepointRollback(t *testing.T) {
	e := newExecutor(t)
	ctx := context.Background()

	tx := e.Begin()
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (5, 'e', 1)"); err != nil {
		t.Fatalf("insert 5: %v", err)
	}
	if err := tx.Savepoint("sp1"); err != nil {
		t.Fatalf("Savepoint: %v", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (6, 'f', 1)"); err != nil {
		t.Fatalf("insert 6: %v", err)
	}
	if err := tx.RollbackToSavepoint("sp1"); err != nil {
		t.Fatalf("RollbackToSavepoint: %v", err)
	}

	rs, _ := tx.Query(ctx, "SELECT id FROM users WHERE id = 5")
	if len(rs.Rows) != 1 {
		t.Fatalf("row before savepoint should survive: %d rows", len(rs.Rows))
	}
	rs, _ = tx.Query(ctx, "SELECT id FROM users WHERE id = 6")
	if len(rs.Rows) != 0 {
		t.Fatalf("row after savepoint should be rolled back: %d rows", len(rs.Rows))
	}
}

func TestTxSelectForUpdate(t *testing.T) {
	e := newExecutor(t)
	ctx := context.Background()

	tx := e.Begin()
	defer tx.Rollback(ctx)
	rs, err := tx.Query(ctx, "SELECT id, name FROM users WHERE id = 2 FOR UPDATE")
	if err != nil {
		t.Fatalf("SELECT FOR UPDATE: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rs.Rows))
	}
	if got := strColumn(t, rs, "name"); got[0] != "bob" {
		t.Fatalf("name = %q, want bob", got[0])
	}
}
