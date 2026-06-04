package sql

import (
	"context"
	"testing"
)

func TestOpenDatabaseCreateAndPersist(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabase(ctx, dir)
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT INDEX)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (1, 'alice', 30)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (2, 'bob', 25)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	rs, err := db.Query(ctx, "SELECT name FROM users WHERE age >= 30")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rs.Rows))
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: schema and data must come back without any Go table setup.
	db2, err := OpenDatabase(ctx, dir)
	if err != nil {
		t.Fatalf("reopen OpenDatabase: %v", err)
	}
	defer db2.Close()

	rs, err = db2.Query(ctx, "SELECT name, age FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query after reopen: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows after reopen = %d, want 1", len(rs.Rows))
	}
	if got := strColumn(t, rs, "name"); got[0] != "alice" {
		t.Fatalf("name = %q, want alice", got[0])
	}

	// The secondary index declared in CREATE TABLE is usable after reopen.
	rs, _ = db2.Query(ctx, "SELECT id FROM users WHERE age = 25")
	if len(rs.Rows) != 1 {
		t.Fatalf("age index query rows = %d, want 1", len(rs.Rows))
	}
}

func TestCreateTableErrors(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabase(ctx, dir)
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("first CREATE: %v", err)
	}
	// Duplicate table.
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY)"); err == nil {
		t.Fatal("expected error creating duplicate table")
	}
	// No primary key.
	if _, err := db.Exec(ctx, "CREATE TABLE bad (x INT)"); err == nil {
		t.Fatal("expected error creating table without a primary key")
	}
}

func TestCreateTableRequiresDatabase(t *testing.T) {
	// An executor built directly (not via OpenDatabase) cannot create tables.
	e := newExecutor(t)
	if _, err := e.Exec(context.Background(), "CREATE TABLE t (id INT PRIMARY KEY)"); err == nil {
		t.Fatal("expected error: CREATE TABLE without an open database")
	}
}
