package sql

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestParseDropTable(t *testing.T) {
	stmt, err := Parse("DROP TABLE users")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	drop, ok := stmt.(*DropTableStmt)
	if !ok {
		t.Fatalf("stmt = %T, want *DropTableStmt", stmt)
	}
	if drop.Table != "users" || drop.IfExists {
		t.Fatalf("got %+v, want Table=users IfExists=false", drop)
	}

	stmt, err = Parse("DROP TABLE IF EXISTS users")
	if err != nil {
		t.Fatalf("Parse IF EXISTS: %v", err)
	}
	drop = stmt.(*DropTableStmt)
	if drop.Table != "users" || !drop.IfExists {
		t.Fatalf("got %+v, want Table=users IfExists=true", drop)
	}

	if _, err := Parse("DROP TABLE"); !errors.Is(err, ErrParse) {
		t.Fatalf("DROP TABLE without name err = %v, want ErrParse", err)
	}
	if _, err := Parse("DROP INDEXES users"); !errors.Is(err, ErrParse) {
		t.Fatalf("DROP INDEXES err = %v, want ErrParse", err)
	}
}

func TestDropTableRemovesTableAndFiles(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR INDEX)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'alice')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	if _, err := db.Exec(ctx, "DROP TABLE users"); err != nil {
		t.Fatalf("DROP TABLE: %v", err)
	}

	// The table is gone for queries and DML.
	if _, err := db.Query(ctx, "SELECT * FROM users"); err == nil {
		t.Fatal("SELECT after DROP TABLE succeeded, want unknown-table error")
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (2, 'bob')"); err == nil {
		t.Fatal("INSERT after DROP TABLE succeeded, want unknown-table error")
	}

	// The physical files are gone.
	if _, err := os.Stat(filepath.Join(dir, "users.heap")); !os.IsNotExist(err) {
		t.Fatalf("users.heap still exists after drop, stat err = %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(dir, "*users*btree*"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("index files still exist after drop: %v", matches)
	}

	// The name is immediately reusable and the new table starts empty.
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("re-CREATE TABLE: %v", err)
	}
	rs, err := db.Query(ctx, "SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT after re-create: %v", err)
	}
	if len(rs.Rows) != 0 {
		t.Fatalf("re-created table has %d rows, want 0", len(rs.Rows))
	}
}

func TestDropTableIfExists(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "DROP TABLE missing"); !errors.Is(err, ErrExec) {
		t.Fatalf("DROP TABLE missing err = %v, want ErrExec", err)
	}
	if _, err := db.Exec(ctx, "DROP TABLE IF EXISTS missing"); err != nil {
		t.Fatalf("DROP TABLE IF EXISTS missing err = %v, want nil", err)
	}

	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "DROP TABLE IF EXISTS t"); err != nil {
		t.Fatalf("DROP TABLE IF EXISTS t err = %v, want nil", err)
	}
	if _, err := db.Query(ctx, "SELECT * FROM t"); err == nil {
		t.Fatal("SELECT after DROP TABLE IF EXISTS succeeded, want unknown-table error")
	}
}

func TestDropTableSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'old')"); err != nil {
		t.Fatalf("INSERT old: %v", err)
	}
	if _, err := db.Exec(ctx, "DROP TABLE users"); err != nil {
		t.Fatalf("DROP TABLE: %v", err)
	}
	// Re-create under the same name and write a fresh row: after reopen the
	// table must contain only the new row, never resurrected old ones.
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("re-CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (7, 'new')"); err != nil {
		t.Fatalf("INSERT new: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	rs, err := db2.Query(ctx, "SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("SELECT after reopen: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows after reopen = %d, want 1", len(rs.Rows))
	}
	if got := strColumn(t, rs, "name"); got[0] != "new" {
		t.Fatalf("name = %q, want new", got[0])
	}
}

func TestDropTableOnDroppedTableAfterReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE gone (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "DROP TABLE gone"); err != nil {
		t.Fatalf("DROP TABLE: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The drop persisted: the reopened database does not know the table.
	db2, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	if _, err := db2.Query(ctx, "SELECT * FROM gone"); err == nil {
		t.Fatal("SELECT on dropped table after reopen succeeded, want error")
	}
}
