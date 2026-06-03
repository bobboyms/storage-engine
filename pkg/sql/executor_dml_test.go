package sql

import (
	"context"
	"testing"
)

func TestExecInsert(t *testing.T) {
	e := newExecutor(t)
	n, err := e.Exec(context.Background(), "INSERT INTO users (id, name, age) VALUES (5, 'erin', 22)")
	if err != nil {
		t.Fatalf("Exec INSERT: %v", err)
	}
	if n != 1 {
		t.Fatalf("affected = %d, want 1", n)
	}

	rs, err := e.Query(context.Background(), "SELECT name, age FROM users WHERE id = 5")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rs.Rows))
	}
	if got := strColumn(t, rs, "name"); got[0] != "erin" {
		t.Fatalf("name = %q, want erin", got[0])
	}
	if got := intColumn(t, rs, "age"); got[0] != 22 {
		t.Fatalf("age = %d, want 22", got[0])
	}
}

func TestExecUpdateSingleRow(t *testing.T) {
	e := newExecutor(t)
	n, err := e.Exec(context.Background(), "UPDATE users SET age = 99 WHERE id = 2")
	if err != nil {
		t.Fatalf("Exec UPDATE: %v", err)
	}
	if n != 1 {
		t.Fatalf("affected = %d, want 1", n)
	}

	rs, _ := e.Query(context.Background(), "SELECT age FROM users WHERE id = 2")
	if got := intColumn(t, rs, "age"); len(got) != 1 || got[0] != 99 {
		t.Fatalf("age after update = %v, want [99]", got)
	}
}

func TestExecUpdateMultipleRows(t *testing.T) {
	e := newExecutor(t)
	// alice (id 1) and dave (id 4) both have age 30.
	n, err := e.Exec(context.Background(), "UPDATE users SET name = 'x' WHERE age = 30")
	if err != nil {
		t.Fatalf("Exec UPDATE: %v", err)
	}
	if n != 2 {
		t.Fatalf("affected = %d, want 2", n)
	}

	rs, _ := e.Query(context.Background(), "SELECT name FROM users WHERE age = 30")
	names := strColumn(t, rs, "name")
	if len(names) != 2 {
		t.Fatalf("rows = %d, want 2", len(names))
	}
	for _, nm := range names {
		if nm != "x" {
			t.Fatalf("name = %q, want x for all updated rows", nm)
		}
	}
}

func TestExecDeleteSingleRow(t *testing.T) {
	e := newExecutor(t)
	n, err := e.Exec(context.Background(), "DELETE FROM users WHERE id = 3")
	if err != nil {
		t.Fatalf("Exec DELETE: %v", err)
	}
	if n != 1 {
		t.Fatalf("affected = %d, want 1", n)
	}

	rs, _ := e.Query(context.Background(), "SELECT id FROM users WHERE id = 3")
	if len(rs.Rows) != 0 {
		t.Fatalf("rows = %d, want 0 after delete", len(rs.Rows))
	}
}

func TestExecDeleteMultipleRows(t *testing.T) {
	e := newExecutor(t)
	n, err := e.Exec(context.Background(), "DELETE FROM users WHERE age = 30")
	if err != nil {
		t.Fatalf("Exec DELETE: %v", err)
	}
	if n != 2 {
		t.Fatalf("affected = %d, want 2", n)
	}

	rs, _ := e.Query(context.Background(), "SELECT id FROM users")
	if len(rs.Rows) != 2 {
		t.Fatalf("remaining rows = %d, want 2", len(rs.Rows))
	}
}

func TestExecErrors(t *testing.T) {
	e := newExecutor(t)
	cases := []string{
		"SELECT * FROM users",                             // SELECT not allowed in Exec
		"INSERT INTO users (id, name) VALUES (9, 'z', 1)", // value count mismatch
		"INSERT INTO ghosts (id) VALUES (1)",              // unknown table
		"INSERT INTO users (id, name) VALUES (9, 'z')",    // missing index column (age)
		"UPDATE users SET id = 100 WHERE id = 1",          // cannot update primary key
		"UPDATE users SET nope = 1 WHERE id = 1",          // unknown column
	}
	for _, q := range cases {
		t.Run(q, func(t *testing.T) {
			if _, err := e.Exec(context.Background(), q); err == nil {
				t.Fatalf("Exec(%q) expected error, got nil", q)
			}
		})
	}
}
