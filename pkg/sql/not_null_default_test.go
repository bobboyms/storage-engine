package sql

import (
	"context"
	"errors"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestParseNotNullAndDefault(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR NOT NULL, age INT DEFAULT 18, city VARCHAR DEFAULT 'unknown', active BOOL NOT NULL DEFAULT true)")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ct := stmt.(*CreateTableStmt)
	byName := map[string]ColumnDef{}
	for _, c := range ct.Columns {
		byName[c.Name] = c
	}

	if !byName["name"].NotNull {
		t.Fatal("name should be NOT NULL")
	}
	if byName["name"].Default != nil {
		t.Fatal("name should have no default")
	}
	if d := byName["age"].Default; d == nil || d.Kind != LitInt || d.Int != 18 {
		t.Fatalf("age default = %+v, want int 18", d)
	}
	if d := byName["city"].Default; d == nil || d.Kind != LitString || d.Str != "unknown" {
		t.Fatalf("city default = %+v, want 'unknown'", d)
	}
	active := byName["active"]
	if !active.NotNull || active.Default == nil || active.Default.Kind != LitBool || !active.Default.Bool {
		t.Fatalf("active = %+v, want NOT NULL DEFAULT true", active)
	}

	if _, err := Parse("CREATE TABLE t (id INT PRIMARY KEY, v INT DEFAULT ?)"); !errors.Is(err, ErrParse) {
		t.Fatalf("DEFAULT ? err = %v, want ErrParse", err)
	}
}

func openConstraintDB(t *testing.T) (*Executor, context.Context) {
	t.Helper()
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, ctx
}

func TestInsertNullIntoNotNullColumnFails(t *testing.T) {
	db, ctx := openConstraintDB(t)
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR NOT NULL)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, NULL)"); !errors.Is(err, ErrExec) {
		t.Fatalf("INSERT NULL err = %v, want ErrExec", err)
	}
	// Omitting the NOT NULL column (no default) must also fail.
	if _, err := db.Exec(ctx, "INSERT INTO users (id) VALUES (1)"); !errors.Is(err, ErrExec) {
		t.Fatalf("INSERT missing err = %v, want ErrExec", err)
	}
	rs, err := db.Query(ctx, "SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rs.Rows) != 0 {
		t.Fatalf("rows = %d, want 0", len(rs.Rows))
	}
}

func TestInsertAppliesDefaults(t *testing.T) {
	db, ctx := openConstraintDB(t)
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, age INT DEFAULT 18, city VARCHAR DEFAULT 'unknown', active BOOL NOT NULL DEFAULT true)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id) VALUES (1)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	rs, err := db.Query(ctx, "SELECT age, city, active FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rs.Rows))
	}
	row := rs.Rows[0]
	if v, ok := row[0].(types.IntKey); !ok || int64(v) != 18 {
		t.Fatalf("age = %v (%T), want 18", row[0], row[0])
	}
	if v, ok := row[1].(types.VarcharKey); !ok || string(v) != "unknown" {
		t.Fatalf("city = %v (%T), want unknown", row[1], row[1])
	}
	if v, ok := row[2].(types.BoolKey); !ok || !bool(v) {
		t.Fatalf("active = %v (%T), want true", row[2], row[2])
	}

	// An explicit value still wins over the default.
	if _, err := db.Exec(ctx, "INSERT INTO users (id, age) VALUES (2, 40)"); err != nil {
		t.Fatalf("INSERT explicit: %v", err)
	}
	rs, _ = db.Query(ctx, "SELECT age FROM users WHERE id = 2")
	if v := rs.Rows[0][0].(types.IntKey); int64(v) != 40 {
		t.Fatalf("age = %v, want 40", v)
	}
}

func TestUpdateNullOnNotNullColumnFails(t *testing.T) {
	db, ctx := openConstraintDB(t)
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR NOT NULL)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'alice')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := db.Exec(ctx, "UPDATE users SET name = NULL WHERE id = 1"); !errors.Is(err, ErrExec) {
		t.Fatalf("UPDATE NULL err = %v, want ErrExec", err)
	}

	// The same rule holds inside a transaction.
	tx := db.Begin()
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "UPDATE users SET name = NULL WHERE id = 1"); !errors.Is(err, ErrExec) {
		t.Fatalf("tx UPDATE NULL err = %v, want ErrExec", err)
	}
}

func TestConstraintsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR NOT NULL, city VARCHAR DEFAULT 'porto')"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	// NOT NULL is still enforced and the default still applies after reopen.
	if _, err := db2.Exec(ctx, "INSERT INTO users (id) VALUES (1)"); !errors.Is(err, ErrExec) {
		t.Fatalf("INSERT missing NOT NULL err = %v, want ErrExec", err)
	}
	if _, err := db2.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'alice')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	rs, err := db2.Query(ctx, "SELECT city FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if got := strColumn(t, rs, "city"); got[0] != "porto" {
		t.Fatalf("city = %q, want porto", got[0])
	}
}

func TestAlterAddNotNullColumnRejected(t *testing.T) {
	db, ctx := openConstraintDB(t)
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id) VALUES (1)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	// Existing rows cannot satisfy NOT NULL on a new column (no backfill), so
	// the ALTER is rejected.
	if _, err := db.Exec(ctx, "ALTER TABLE users ADD COLUMN name VARCHAR NOT NULL"); !errors.Is(err, ErrExec) {
		t.Fatalf("ALTER ADD NOT NULL err = %v, want ErrExec", err)
	}
	// Adding a column with only a DEFAULT is fine: it applies to future inserts.
	if _, err := db.Exec(ctx, "ALTER TABLE users ADD COLUMN city VARCHAR DEFAULT 'lisboa'"); err != nil {
		t.Fatalf("ALTER ADD DEFAULT: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id) VALUES (2)"); err != nil {
		t.Fatalf("INSERT after ALTER: %v", err)
	}
	rs, err := db.Query(ctx, "SELECT city FROM users WHERE id = 2")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if got := strColumn(t, rs, "city"); got[0] != "lisboa" {
		t.Fatalf("city = %q, want lisboa", got[0])
	}
}

func TestCreateTableInvalidDefaultRejected(t *testing.T) {
	db, ctx := openConstraintDB(t)
	// Type-incompatible default.
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY, age INT DEFAULT 'abc')"); err == nil {
		t.Fatal("CREATE TABLE with mismatched default succeeded, want error")
	}
	// NOT NULL combined with DEFAULT NULL is contradictory.
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR NOT NULL DEFAULT NULL)"); err == nil {
		t.Fatal("CREATE TABLE with NOT NULL DEFAULT NULL succeeded, want error")
	}
}

func TestTxInsertAppliesDefaultsAndNotNull(t *testing.T) {
	db, ctx := openConstraintDB(t)
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR NOT NULL, city VARCHAR DEFAULT 'faro')"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	tx := db.Begin()
	if _, err := tx.Exec(ctx, "INSERT INTO users (id) VALUES (1)"); !errors.Is(err, ErrExec) {
		t.Fatalf("tx INSERT missing NOT NULL err = %v, want ErrExec", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'a')"); err != nil {
		t.Fatalf("tx INSERT: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	rs, err := db.Query(ctx, "SELECT city FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if got := strColumn(t, rs, "city"); got[0] != "faro" {
		t.Fatalf("city = %q, want faro", got[0])
	}
}
