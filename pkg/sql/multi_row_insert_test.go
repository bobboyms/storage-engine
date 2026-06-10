package sql

import (
	"context"
	"errors"
	"testing"
)

func TestParseMultiRowInsert(t *testing.T) {
	stmt, err := Parse("INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ins, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("stmt = %T, want *InsertStmt", stmt)
	}
	if len(ins.Rows) != 3 {
		t.Fatalf("Rows len = %d, want 3", len(ins.Rows))
	}
	for i, want := range []string{"1", "2", "3"} {
		if len(ins.Rows[i]) != 2 {
			t.Fatalf("row %d has %d values, want 2", i, len(ins.Rows[i]))
		}
		if got := ins.Rows[i][0].String(); got != want {
			t.Fatalf("row %d id = %s, want %s", i, got, want)
		}
	}

	if _, err := Parse("INSERT INTO users (id) VALUES (1), "); !errors.Is(err, ErrParse) {
		t.Fatalf("trailing comma err = %v, want ErrParse", err)
	}
}

func TestMultiRowInsert(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	n, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	if err != nil {
		t.Fatalf("multi-row INSERT: %v", err)
	}
	if n != 3 {
		t.Fatalf("affected = %d, want 3", n)
	}

	rs, err := db.Query(ctx, "SELECT name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	got := strColumn(t, rs, "name")
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("rows = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d name = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestMultiRowInsertIsAtomicOnDuplicate(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (2, 'existing')"); err != nil {
		t.Fatalf("seed INSERT: %v", err)
	}

	// Row 2 duplicates an existing primary key: the whole statement must fail
	// and insert nothing.
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'x'), (2, 'dup'), (3, 'y')"); err == nil {
		t.Fatal("multi-row INSERT with duplicate succeeded, want error")
	}

	rs, err := db.Query(ctx, "SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows after failed insert = %d, want only the seeded row", len(rs.Rows))
	}
	if got := strColumn(t, rs, "name"); got[0] != "existing" {
		t.Fatalf("surviving row name = %q, want existing", got[0])
	}
}

func TestMultiRowInsertWithPlaceholders(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	n, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (?, ?), (?, ?)", 1, "a", 2, "b")
	if err != nil {
		t.Fatalf("INSERT with placeholders: %v", err)
	}
	if n != 2 {
		t.Fatalf("affected = %d, want 2", n)
	}

	rs, err := db.Query(ctx, "SELECT name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	got := strColumn(t, rs, "name")
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("names = %v, want [a b]", got)
	}
}

func TestMultiRowInsertMismatchedRow(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// The second row has a different arity than the column list; nothing may
	// be inserted.
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'a'), (2)"); !errors.Is(err, ErrExec) {
		t.Fatalf("mismatched row err = %v, want ErrExec", err)
	}
	rs, err := db.Query(ctx, "SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rs.Rows) != 0 {
		t.Fatalf("rows after failed insert = %d, want 0", len(rs.Rows))
	}
}

func TestMultiRowInsertInsideTransaction(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	tx := db.Begin()
	n, err := tx.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b')")
	if err != nil {
		t.Fatalf("tx multi-row INSERT: %v", err)
	}
	if n != 2 {
		t.Fatalf("affected = %d, want 2", n)
	}
	// Read-your-writes inside the transaction.
	rs, err := tx.Query(ctx, "SELECT name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("tx SELECT: %v", err)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("tx rows = %d, want 2", len(rs.Rows))
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	rs, err = db.Query(ctx, "SELECT name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT after commit: %v", err)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("rows after commit = %d, want 2", len(rs.Rows))
	}
}
