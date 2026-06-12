package sql

import (
	"context"
	"errors"
	"testing"
)

// openIntegrityDB opens a fresh database in a temp dir with maintenance off.
func openIntegrityDB(t *testing.T) (*Executor, string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, dir
}

// --- CHECK constraints ---

func TestCheckConstraintRejectsInsert(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE products (id INT PRIMARY KEY, price INT, CHECK (price >= 0))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO products (id, price) VALUES (1, 10)"); err != nil {
		t.Fatalf("valid INSERT: %v", err)
	}
	_, err := db.Exec(ctx, "INSERT INTO products (id, price) VALUES (2, -5)")
	if !errors.Is(err, ErrCheckViolation) {
		t.Fatalf("negative price INSERT err = %v, want ErrCheckViolation", err)
	}
}

func TestCheckConstraintRejectsUpdate(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE products (id INT PRIMARY KEY, price INT, CHECK (price >= 0))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO products (id, price) VALUES (1, 10)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	_, err := db.Exec(ctx, "UPDATE products SET price = price - 100 WHERE id = 1")
	if !errors.Is(err, ErrCheckViolation) {
		t.Fatalf("UPDATE err = %v, want ErrCheckViolation", err)
	}
	// The failed update must not have changed the row.
	rs, err := db.Query(ctx, "SELECT price FROM products WHERE id = 1")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := intColumn(t, rs, "price"); got[0] != 10 {
		t.Fatalf("price = %d, want 10 (unchanged)", got[0])
	}
}

func TestCheckConstraintNullPasses(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE products (id INT PRIMARY KEY, price INT, CHECK (price >= 0))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	// SQL CHECK semantics: UNKNOWN (NULL operand) does not violate.
	if _, err := db.Exec(ctx, "INSERT INTO products (id, price) VALUES (1, NULL)"); err != nil {
		t.Fatalf("INSERT with NULL price: %v", err)
	}
}

func TestCheckConstraintPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE products (id INT PRIMARY KEY, price INT, CHECK (price >= 0))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db2.Close() }()
	_, err = db2.Exec(ctx, "INSERT INTO products (id, price) VALUES (1, -1)")
	if !errors.Is(err, ErrCheckViolation) {
		t.Fatalf("after reopen err = %v, want ErrCheckViolation", err)
	}
}

func TestCheckConstraintUnknownColumnRejected(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	_, err := db.Exec(ctx, "CREATE TABLE products (id INT PRIMARY KEY, price INT, CHECK (nosuch >= 0))")
	if err == nil {
		t.Fatal("CREATE TABLE with CHECK on unknown column succeeded, want error")
	}
}

func TestCheckConstraintInTransaction(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE products (id INT PRIMARY KEY, price INT, CHECK (price >= 0))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	tx := db.Begin()
	defer func() { _ = tx.Rollback(ctx) }()
	_, err := tx.Exec(ctx, "INSERT INTO products (id, price) VALUES (1, -3)")
	if !errors.Is(err, ErrCheckViolation) {
		t.Fatalf("tx INSERT err = %v, want ErrCheckViolation", err)
	}
}

// --- FOREIGN KEY constraints ---

func createParentChild(t *testing.T, db *Executor) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE owners (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("CREATE owners: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE pets (id INT PRIMARY KEY, owner_id INT REFERENCES owners(id), name VARCHAR)"); err != nil {
		t.Fatalf("CREATE pets: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO owners (id, name) VALUES (1, 'ana')"); err != nil {
		t.Fatalf("seed owner: %v", err)
	}
}

func TestForeignKeyInsertEnforced(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	createParentChild(t, db)

	if _, err := db.Exec(ctx, "INSERT INTO pets (id, owner_id, name) VALUES (1, 1, 'rex')"); err != nil {
		t.Fatalf("valid child INSERT: %v", err)
	}
	_, err := db.Exec(ctx, "INSERT INTO pets (id, owner_id, name) VALUES (2, 99, 'bidu')")
	if !errors.Is(err, ErrForeignKeyViolation) {
		t.Fatalf("orphan child INSERT err = %v, want ErrForeignKeyViolation", err)
	}
}

func TestForeignKeyNullAllowed(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	createParentChild(t, db)
	if _, err := db.Exec(ctx, "INSERT INTO pets (id, owner_id, name) VALUES (1, NULL, 'solto')"); err != nil {
		t.Fatalf("NULL FK INSERT: %v", err)
	}
}

func TestForeignKeyUpdateEnforced(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	createParentChild(t, db)
	if _, err := db.Exec(ctx, "INSERT INTO pets (id, owner_id, name) VALUES (1, 1, 'rex')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	_, err := db.Exec(ctx, "UPDATE pets SET owner_id = 42 WHERE id = 1")
	if !errors.Is(err, ErrForeignKeyViolation) {
		t.Fatalf("UPDATE err = %v, want ErrForeignKeyViolation", err)
	}
}

func TestForeignKeyDeleteRestricted(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	createParentChild(t, db)
	if _, err := db.Exec(ctx, "INSERT INTO pets (id, owner_id, name) VALUES (1, 1, 'rex')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	_, err := db.Exec(ctx, "DELETE FROM owners WHERE id = 1")
	if !errors.Is(err, ErrForeignKeyViolation) {
		t.Fatalf("DELETE parent err = %v, want ErrForeignKeyViolation", err)
	}
	// After removing the child the parent can go.
	if _, err := db.Exec(ctx, "DELETE FROM pets WHERE id = 1"); err != nil {
		t.Fatalf("DELETE child: %v", err)
	}
	if _, err := db.Exec(ctx, "DELETE FROM owners WHERE id = 1"); err != nil {
		t.Fatalf("DELETE parent after child removed: %v", err)
	}
}

func TestForeignKeyDropTableRestricted(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	createParentChild(t, db)
	if _, err := db.Exec(ctx, "DROP TABLE owners"); err == nil {
		t.Fatal("DROP referenced parent succeeded, want error")
	}
	// Dropping the child first releases the parent.
	if _, err := db.Exec(ctx, "DROP TABLE pets"); err != nil {
		t.Fatalf("DROP child: %v", err)
	}
	if _, err := db.Exec(ctx, "DROP TABLE owners"); err != nil {
		t.Fatalf("DROP parent after child dropped: %v", err)
	}
}

func TestForeignKeyTableLevelSyntax(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE owners (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("CREATE owners: %v", err)
	}
	stmt := "CREATE TABLE pets (id INT PRIMARY KEY, owner_id INT, FOREIGN KEY (owner_id) REFERENCES owners(id))"
	if _, err := db.Exec(ctx, stmt); err != nil {
		t.Fatalf("CREATE pets with table-level FK: %v", err)
	}
	_, err := db.Exec(ctx, "INSERT INTO pets (id, owner_id) VALUES (1, 7)")
	if !errors.Is(err, ErrForeignKeyViolation) {
		t.Fatalf("INSERT err = %v, want ErrForeignKeyViolation", err)
	}
}

func TestForeignKeyUnknownParentRejected(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	_, err := db.Exec(ctx, "CREATE TABLE pets (id INT PRIMARY KEY, owner_id INT REFERENCES owners(id))")
	if err == nil {
		t.Fatal("CREATE TABLE referencing unknown parent succeeded, want error")
	}
}

func TestForeignKeyMustReferencePrimaryKey(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE owners (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("CREATE owners: %v", err)
	}
	_, err := db.Exec(ctx, "CREATE TABLE pets (id INT PRIMARY KEY, owner_name VARCHAR REFERENCES owners(name))")
	if err == nil {
		t.Fatal("FK referencing non-primary column succeeded, want error")
	}
}

func TestForeignKeyInTransaction(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	createParentChild(t, db)

	tx := db.Begin()
	// A parent staged in the same transaction satisfies the FK.
	if _, err := tx.Exec(ctx, "INSERT INTO owners (id, name) VALUES (2, 'bia')"); err != nil {
		t.Fatalf("tx INSERT parent: %v", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO pets (id, owner_id, name) VALUES (1, 2, 'mel')"); err != nil {
		t.Fatalf("tx INSERT child of staged parent: %v", err)
	}
	_, err := tx.Exec(ctx, "INSERT INTO pets (id, owner_id, name) VALUES (2, 77, 'x')")
	if !errors.Is(err, ErrForeignKeyViolation) {
		t.Fatalf("tx orphan INSERT err = %v, want ErrForeignKeyViolation", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
}

func TestForeignKeyPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	createParentChild(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db2.Close() }()
	_, err = db2.Exec(ctx, "INSERT INTO pets (id, owner_id, name) VALUES (5, 123, 'zed')")
	if !errors.Is(err, ErrForeignKeyViolation) {
		t.Fatalf("after reopen err = %v, want ErrForeignKeyViolation", err)
	}
}

// --- ALTER TABLE RENAME COLUMN ---

func TestRenameColumn(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE notes (id INT PRIMARY KEY, body VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO notes (id, body) VALUES (1, 'hello')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE notes RENAME COLUMN body TO content"); err != nil {
		t.Fatalf("RENAME COLUMN: %v", err)
	}

	rs, err := db.Query(ctx, "SELECT content FROM notes WHERE id = 1")
	if err != nil {
		t.Fatalf("Query renamed column: %v", err)
	}
	if got := strColumn(t, rs, "content"); got[0] != "hello" {
		t.Fatalf("content = %q, want hello", got[0])
	}
	if _, err := db.Query(ctx, "SELECT body FROM notes"); err == nil {
		t.Fatal("old column name still resolves, want error")
	}
}

func TestRenameColumnPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE notes (id INT PRIMARY KEY, body VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO notes (id, body) VALUES (1, 'hi')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE notes RENAME COLUMN body TO content"); err != nil {
		t.Fatalf("RENAME: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db2.Close() }()
	rs, err := db2.Query(ctx, "SELECT content FROM notes WHERE id = 1")
	if err != nil {
		t.Fatalf("Query after reopen: %v", err)
	}
	if got := strColumn(t, rs, "content"); got[0] != "hi" {
		t.Fatalf("content = %q, want hi", got[0])
	}
}

func TestRenameColumnRejectsIndexedColumn(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE notes (id INT PRIMARY KEY, tag VARCHAR INDEX)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE notes RENAME COLUMN id TO ident"); err == nil {
		t.Fatal("renaming primary key column succeeded, want error")
	}
	if _, err := db.Exec(ctx, "ALTER TABLE notes RENAME COLUMN tag TO label"); err == nil {
		t.Fatal("renaming indexed column succeeded, want error")
	}
}

func TestRenameColumnConflicts(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE notes (id INT PRIMARY KEY, a VARCHAR, b VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE notes RENAME COLUMN a TO b"); err == nil {
		t.Fatal("renaming onto existing column succeeded, want error")
	}
	if _, err := db.Exec(ctx, "ALTER TABLE notes RENAME COLUMN nosuch TO c"); err == nil {
		t.Fatal("renaming unknown column succeeded, want error")
	}
}

func TestParseRenameColumn(t *testing.T) {
	stmt, err := Parse("ALTER TABLE notes RENAME COLUMN body TO content")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	alter, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("stmt = %T, want *AlterTableStmt", stmt)
	}
	if !alter.Rename || alter.Column.Name != "body" || alter.NewName != "content" {
		t.Fatalf("got %+v, want Rename body->content", alter)
	}
}
