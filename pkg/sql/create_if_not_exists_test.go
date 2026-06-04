package sql

import (
	"context"
	"errors"
	"testing"
)

func TestParseCreateTableIfNotExists(t *testing.T) {
	stmt, err := Parse("CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ct, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("got %T, want *CreateTableStmt", stmt)
	}
	if ct.Table != "users" || !ct.IfNotExists {
		t.Fatalf("stmt = %+v, want users with IfNotExists", ct)
	}
}

func TestParseCreateTableWithoutIfNotExists(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if ct := stmt.(*CreateTableStmt); ct.IfNotExists {
		t.Fatalf("IfNotExists = true, want false for a plain CREATE TABLE")
	}
}

func TestParseCreateTableIfErrors(t *testing.T) {
	inputs := []string{
		"CREATE TABLE IF users (id INT PRIMARY KEY)",     // IF without NOT EXISTS
		"CREATE TABLE IF NOT users (id INT PRIMARY KEY)", // missing EXISTS
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			if _, err := Parse(in); !errors.Is(err, ErrParse) {
				t.Fatalf("Parse(%q) error = %v, want ErrParse", in, err)
			}
		})
	}
}

func TestExecCreateTableIfNotExistsIsNoop(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("first CREATE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'alice')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// A re-create with IF NOT EXISTS (even with a different definition) is a
	// silent no-op: no error, and the existing table/data are untouched.
	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name VARCHAR, age INT)"); err != nil {
		t.Fatalf("CREATE TABLE IF NOT EXISTS on existing table: %v", err)
	}

	// The phantom column from the ignored definition was not added.
	if _, err := db.Query(ctx, "SELECT age FROM users WHERE id = 1"); err == nil {
		t.Fatal("expected error querying column from the ignored re-create")
	}
	rs, err := db.Query(ctx, "SELECT name FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query existing row: %v", err)
	}
	if got := strColumn(t, rs, "name"); len(got) != 1 || got[0] != "alice" {
		t.Fatalf("name = %v, want [alice]", got)
	}

	// On a fresh name, IF NOT EXISTS creates the table.
	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS fresh (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE IF NOT EXISTS on new table: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO fresh (id) VALUES (7)"); err != nil {
		t.Fatalf("INSERT into fresh: %v", err)
	}
}

func TestExecCreateTableDuplicateStillErrors(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("first CREATE: %v", err)
	}
	// Without IF NOT EXISTS, a duplicate is still an error.
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY)"); !errors.Is(err, ErrDuplicateTable) {
		t.Fatalf("duplicate CREATE error = %v, want ErrDuplicateTable", err)
	}
}
