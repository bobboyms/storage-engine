package sql

import (
	"errors"
	"testing"
)

func TestUniqueTxUpdateToCollidingValueRejected(t *testing.T) {
	db, ctx := openUniqueDB(t, "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR UNIQUE)")
	if _, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 1, "a@x.com"); err != nil {
		t.Fatalf("seed 1: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 2, "b@x.com"); err != nil {
		t.Fatalf("seed 2: %v", err)
	}

	tx := db.Begin()
	defer func() { _ = tx.Rollback(ctx) }()
	_, err := tx.Exec(ctx, "UPDATE users SET email = ? WHERE id = ?", "a@x.com", 2)
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation updating into an existing value in a tx, got %v", err)
	}
}

func TestUniqueTxUpdateSameRowAllowed(t *testing.T) {
	db, ctx := openUniqueDB(t, "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR UNIQUE, age INT)")
	if _, err := db.Exec(ctx, "INSERT INTO users (id, email, age) VALUES (?, ?, ?)", 1, "a@x.com", 30); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tx := db.Begin()
	// Updating a non-unique column must not trip the row's own unique value.
	if _, err := tx.Exec(ctx, "UPDATE users SET age = ? WHERE id = ?", 31, 1); err != nil {
		t.Fatalf("self update in tx should be allowed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	rs, _ := db.Query(ctx, "SELECT age FROM users WHERE id = ?", 1)
	if got := intColumn(t, rs, "age"); len(got) != 1 || got[0] != 31 {
		t.Fatalf("age = %v, want [31]", got)
	}
}
