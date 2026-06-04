package sql

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestAlterAddColumn(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'alice')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE users ADD COLUMN nickname VARCHAR"); err != nil {
		t.Fatalf("ALTER ADD COLUMN: %v", err)
	}

	// The pre-existing row reads NULL for the new column.
	rs, err := db.Query(ctx, "SELECT nickname FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query nickname: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rs.Rows))
	}
	if _, ok := rs.Rows[0][0].(types.NullKey); !ok {
		t.Fatalf("nickname = %T, want NullKey", rs.Rows[0][0])
	}

	// It is writable and reads back.
	if _, err := db.Exec(ctx, "UPDATE users SET nickname = 'ali' WHERE id = 1"); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	rs, _ = db.Query(ctx, "SELECT nickname FROM users WHERE id = 1")
	if got := strColumn(t, rs, "nickname"); got[0] != "ali" {
		t.Fatalf("nickname = %q, want ali", got[0])
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The new column survives reopen.
	db2, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	rs, err = db2.Query(ctx, "SELECT nickname FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query after reopen: %v", err)
	}
	if got := strColumn(t, rs, "nickname"); got[0] != "ali" {
		t.Fatalf("nickname after reopen = %q, want ali", got[0])
	}
}

func TestAlterAddColumnIndex(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE accounts (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO accounts (id, name) VALUES (1, 'alice')"); err != nil {
		t.Fatalf("INSERT 1: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO accounts (id, name) VALUES (2, 'bob')"); err != nil {
		t.Fatalf("INSERT 2: %v", err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE accounts ADD COLUMN email VARCHAR INDEX"); err != nil {
		t.Fatalf("ALTER ADD COLUMN INDEX: %v", err)
	}
	// Populate the new indexed column on existing rows.
	if _, err := db.Exec(ctx, "UPDATE accounts SET email = 'alice@example.com' WHERE id = 1"); err != nil {
		t.Fatalf("UPDATE 1: %v", err)
	}
	if _, err := db.Exec(ctx, "UPDATE accounts SET email = 'bob@example.com' WHERE id = 2"); err != nil {
		t.Fatalf("UPDATE 2: %v", err)
	}

	// The user can now query by the new column.
	rs, err := db.Query(ctx, "SELECT id FROM accounts WHERE email = 'bob@example.com'")
	if err != nil {
		t.Fatalf("Query by email: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rs.Rows))
	}
	if got, ok := rs.Rows[0][0].(types.IntKey); !ok || int64(got) != 2 {
		t.Fatalf("id = %v (%T), want 2", rs.Rows[0][0], rs.Rows[0][0])
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The index survives reopen and remains queryable.
	db2, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	rs, err = db2.Query(ctx, "SELECT id FROM accounts WHERE email = 'alice@example.com'")
	if err != nil {
		t.Fatalf("Query by email after reopen: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows after reopen = %d, want 1", len(rs.Rows))
	}
}

func TestAlterDropColumn(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE t ADD COLUMN email VARCHAR INDEX"); err != nil {
		t.Fatalf("ALTER ADD: %v", err)
	}
	idxPath := filepath.Join(dir, "t.heap.t.email.btree.v2")
	if _, err := os.Stat(idxPath); err != nil {
		t.Fatalf("expected index file created: %v", err)
	}

	if _, err := db.Exec(ctx, "ALTER TABLE t DROP COLUMN email"); err != nil {
		t.Fatalf("ALTER DROP: %v", err)
	}
	// The column is gone: querying it errors.
	if _, err := db.Query(ctx, "SELECT email FROM t WHERE id = 1"); err == nil {
		t.Fatal("expected error querying dropped column")
	}
	// The index file is deleted.
	if _, err := os.Stat(idxPath); !os.IsNotExist(err) {
		t.Fatalf("expected index file deleted, stat err = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The drop survives reopen.
	db2, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	if _, err := db2.Query(ctx, "SELECT email FROM t WHERE id = 1"); err == nil {
		t.Fatal("expected error querying dropped column after reopen")
	}
	if _, err := db2.Query(ctx, "SELECT name FROM t WHERE id = 1"); err != nil {
		t.Fatalf("remaining column should still be queryable: %v", err)
	}
}

func TestAlterTableErrors(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	cases := map[string]string{
		"unknown table":    "ALTER TABLE missing ADD COLUMN x INT",
		"duplicate column": "ALTER TABLE t ADD COLUMN name VARCHAR",
		"add primary key":  "ALTER TABLE t ADD COLUMN k INT PRIMARY KEY",
		"drop primary":     "ALTER TABLE t DROP COLUMN id",
		"drop unknown":     "ALTER TABLE t DROP COLUMN nope",
	}
	for name, sql := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := db.Exec(ctx, sql); err == nil {
				t.Fatalf("expected error for %q", sql)
			}
		})
	}
}

func TestAlterAddColumnDuplicateIsTyped(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	_, err = db.Exec(ctx, "ALTER TABLE t ADD COLUMN name VARCHAR")
	if err == nil {
		t.Fatal("expected error adding a duplicate column")
	}
	if !errors.Is(err, ErrDuplicateColumn) {
		t.Fatalf("expected ErrDuplicateColumn, got %v", err)
	}
}

func TestAlterTableRequiresDatabase(t *testing.T) {
	e := newExecutor(t)
	if _, err := e.Exec(context.Background(), "ALTER TABLE users ADD COLUMN x INT"); err == nil {
		t.Fatal("expected error: ALTER TABLE without an open database")
	}
}
