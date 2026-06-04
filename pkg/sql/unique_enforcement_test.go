package sql

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func openUniqueDB(t *testing.T, ddl string) (*Executor, context.Context) {
	t.Helper()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, t.TempDir(), OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(ctx, ddl); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	return db, ctx
}

func TestUniqueSingleColumnRejectsDuplicate(t *testing.T) {
	db, ctx := openUniqueDB(t, "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR UNIQUE)")

	if _, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 1, "a@x.com"); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	_, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 2, "a@x.com")
	if err == nil {
		t.Fatal("expected a unique violation inserting a duplicate email")
	}
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got %v", err)
	}
	// A different value still inserts.
	if _, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 3, "b@x.com"); err != nil {
		t.Fatalf("non-conflicting insert: %v", err)
	}
}

func TestUniqueCompositeRejectsDuplicate(t *testing.T) {
	db, ctx := openUniqueDB(t, "CREATE TABLE users (id INT PRIMARY KEY, environment_id INT, email VARCHAR, UNIQUE (environment_id, email))")

	if _, err := db.Exec(ctx, "INSERT INTO users (id, environment_id, email) VALUES (?, ?, ?)", 1, 7, "a@x.com"); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	// Same tuple -> violation.
	_, err := db.Exec(ctx, "INSERT INTO users (id, environment_id, email) VALUES (?, ?, ?)", 2, 7, "a@x.com")
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation for duplicate tuple, got %v", err)
	}
	// Same email but different environment_id -> allowed (only the tuple is unique).
	if _, err := db.Exec(ctx, "INSERT INTO users (id, environment_id, email) VALUES (?, ?, ?)", 3, 8, "a@x.com"); err != nil {
		t.Fatalf("different environment should be allowed: %v", err)
	}
}

func TestUniqueUpdateSameRowAllowed(t *testing.T) {
	db, ctx := openUniqueDB(t, "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR UNIQUE, age INT)")
	if _, err := db.Exec(ctx, "INSERT INTO users (id, email, age) VALUES (?, ?, ?)", 1, "a@x.com", 30); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Updating a non-unique column on the same row must not trip its own unique value.
	if _, err := db.Exec(ctx, "UPDATE users SET age = ? WHERE id = ?", 31, 1); err != nil {
		t.Fatalf("self update should be allowed: %v", err)
	}
}

func TestUniqueUpdateToCollidingValueRejected(t *testing.T) {
	db, ctx := openUniqueDB(t, "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR UNIQUE)")
	if _, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 1, "a@x.com"); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 2, "b@x.com"); err != nil {
		t.Fatalf("insert 2: %v", err)
	}
	_, err := db.Exec(ctx, "UPDATE users SET email = ? WHERE id = ?", "a@x.com", 2)
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation updating into an existing value, got %v", err)
	}
}

func TestUniqueDeleteThenReinsertAllowed(t *testing.T) {
	db, ctx := openUniqueDB(t, "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR UNIQUE)")
	if _, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 1, "a@x.com"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.Exec(ctx, "DELETE FROM users WHERE id = ?", 1); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// The value is free again after the row is gone.
	if _, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 2, "a@x.com"); err != nil {
		t.Fatalf("reinsert after delete should be allowed: %v", err)
	}
}

func TestUniqueConcurrentInsertsExactlyOneWins(t *testing.T) {
	db, ctx := openUniqueDB(t, "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR UNIQUE)")

	const goroutines = 16
	var wg sync.WaitGroup
	var success, violations int64
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			// Distinct primary keys, identical unique email.
			_, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", id+1, "race@x.com")
			switch {
			case err == nil:
				atomic.AddInt64(&success, 1)
			case errors.Is(err, ErrUniqueViolation):
				atomic.AddInt64(&violations, 1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if success != 1 {
		t.Fatalf("successes = %d, want exactly 1", success)
	}
	if violations != goroutines-1 {
		t.Fatalf("violations = %d, want %d", violations, goroutines-1)
	}
}
