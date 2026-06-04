package sql

import (
	"context"
	"errors"
	"testing"
)

func TestUniqueSurvivesReopenAndStillEnforces(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR UNIQUE)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 1, "a@x.com"); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 2, "b@x.com"); err != nil {
		t.Fatalf("insert 2: %v", err)
	}
	_ = db.Close()

	// Reopen triggers WAL recovery; redo must not spuriously reject the
	// already-committed rows, and the rebuilt unique index must still enforce.
	db2, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("reopen (recovery): %v", err)
	}
	defer db2.Close()

	rs, err := db2.Query(ctx, "SELECT id FROM users")
	if err != nil {
		t.Fatalf("query after reopen: %v", err)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("rows after recovery = %d, want 2", len(rs.Rows))
	}

	// The constraint is still enforced against the recovered rows.
	_, err = db2.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 3, "a@x.com")
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation after recovery, got %v", err)
	}
	// A fresh value still inserts.
	if _, err := db2.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 3, "c@x.com"); err != nil {
		t.Fatalf("non-conflicting insert after recovery: %v", err)
	}
}

func TestUniqueTxRejectsDuplicateOfCommitted(t *testing.T) {
	db, ctx := openUniqueDB(t, "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR UNIQUE)")
	if _, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 1, "a@x.com"); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	tx := db.Begin()
	defer func() { _ = tx.Rollback(ctx) }()
	_, err := tx.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 2, "a@x.com")
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation inserting a committed duplicate in a tx, got %v", err)
	}
}

func TestUniqueTxRejectsDuplicateWithinTx(t *testing.T) {
	db, ctx := openUniqueDB(t, "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR UNIQUE)")

	tx := db.Begin()
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 1, "a@x.com"); err != nil {
		t.Fatalf("first staged insert: %v", err)
	}
	_, err := tx.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 2, "a@x.com")
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation for a duplicate staged in the same tx, got %v", err)
	}
}

func TestUniqueTxAllowsDistinctValues(t *testing.T) {
	db, ctx := openUniqueDB(t, "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR UNIQUE)")

	tx := db.Begin()
	if _, err := tx.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 1, "a@x.com"); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 2, "b@x.com"); err != nil {
		t.Fatalf("insert 2: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	rs, err := db.Query(ctx, "SELECT id FROM users")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rs.Rows))
	}
}
