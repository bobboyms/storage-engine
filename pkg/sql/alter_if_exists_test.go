package sql

import (
	"context"
	"reflect"
	"testing"
)

func TestParseAlterAddColumnIfNotExists(t *testing.T) {
	stmt, err := Parse("ALTER TABLE t ADD COLUMN IF NOT EXISTS x INT")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	a, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("got %T, want *AlterTableStmt", stmt)
	}
	if a.Drop || !a.IfExists || a.Column.Name != "x" {
		t.Fatalf("parsed = %+v, want ADD IfExists x", a)
	}
}

func TestParseAlterDropColumnIfExists(t *testing.T) {
	stmt, err := Parse("ALTER TABLE t DROP COLUMN IF EXISTS x")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	a, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("got %T, want *AlterTableStmt", stmt)
	}
	if !a.Drop || !a.IfExists || a.Column.Name != "x" {
		t.Fatalf("parsed = %+v, want DROP IfExists x", a)
	}
}

func openAlterDB(t *testing.T) (*Executor, context.Context) {
	t.Helper()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, t.TempDir(), OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	return db, ctx
}

func TestAlterAddColumnIfNotExistsIsIdempotent(t *testing.T) {
	db, ctx := openAlterDB(t)

	// First add creates the column.
	if _, err := db.Exec(ctx, "ALTER TABLE t ADD COLUMN IF NOT EXISTS email VARCHAR"); err != nil {
		t.Fatalf("first ADD: %v", err)
	}
	// Second add is a silent no-op rather than ErrDuplicateColumn.
	if _, err := db.Exec(ctx, "ALTER TABLE t ADD COLUMN IF NOT EXISTS email VARCHAR"); err != nil {
		t.Fatalf("second ADD should be a no-op, got: %v", err)
	}

	cols, _ := db.Columns("t")
	want := []string{"id", "name", "email"}
	if got := columnNames(cols); !reflect.DeepEqual(got, want) {
		t.Fatalf("columns = %v, want %v (added exactly once)", got, want)
	}
}

func TestAlterDropColumnIfExistsIsIdempotent(t *testing.T) {
	db, ctx := openAlterDB(t)

	// Dropping the existing column works.
	if _, err := db.Exec(ctx, "ALTER TABLE t DROP COLUMN IF EXISTS name"); err != nil {
		t.Fatalf("first DROP: %v", err)
	}
	// Dropping again is a silent no-op rather than an unknown-column error.
	if _, err := db.Exec(ctx, "ALTER TABLE t DROP COLUMN IF EXISTS name"); err != nil {
		t.Fatalf("second DROP should be a no-op, got: %v", err)
	}

	cols, _ := db.Columns("t")
	want := []string{"id"}
	if got := columnNames(cols); !reflect.DeepEqual(got, want) {
		t.Fatalf("columns = %v, want %v", got, want)
	}
}

func TestAlterAddColumnWithoutGuardStillConflicts(t *testing.T) {
	db, ctx := openAlterDB(t)
	// Without the guard, adding an existing column must still fail.
	if _, err := db.Exec(ctx, "ALTER TABLE t ADD COLUMN name VARCHAR"); err == nil {
		t.Fatal("expected an error adding a duplicate column without IF NOT EXISTS")
	}
}

func TestMigrateIdempotentWithIfGuards(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, t.TempDir(), OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	// A linear, idempotent ensureSchema: running it twice must succeed both
	// times and converge to the same schema.
	steps := []string{
		"CREATE TABLE IF NOT EXISTS accounts (id INT PRIMARY KEY, name VARCHAR)",
		"ALTER TABLE accounts ADD COLUMN IF NOT EXISTS email VARCHAR",
		"ALTER TABLE accounts DROP COLUMN IF EXISTS legacy",
	}
	if err := db.Migrate(ctx, steps); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	if err := db.Migrate(ctx, steps); err != nil {
		t.Fatalf("second Migrate should be a no-op, got: %v", err)
	}

	cols, _ := db.Columns("accounts")
	want := []string{"id", "name", "email"}
	if got := columnNames(cols); !reflect.DeepEqual(got, want) {
		t.Fatalf("columns = %v, want %v", got, want)
	}
}
