package sql

import (
	"context"
	"reflect"
	"testing"
)

func columnNames(cols []Column) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}

func TestMigrateAppliesAllSteps(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}

	if err := db.Migrate(ctx, []string{
		"CREATE TABLE orders (id INT PRIMARY KEY, total FLOAT)",
		"ALTER TABLE orders ADD COLUMN status VARCHAR",
		"ALTER TABLE orders ADD COLUMN note VARCHAR INDEX",
	}); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	want := []string{"id", "total", "status", "note"}
	cols, ok := db.Columns("orders")
	if !ok || !reflect.DeepEqual(columnNames(cols), want) {
		t.Fatalf("columns = %v (ok=%v), want %v", columnNames(cols), ok, want)
	}
	_ = db.Close()

	// The migration must survive a reopen (schema persisted exactly once).
	db2, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	cols2, ok := db2.Columns("orders")
	if !ok || !reflect.DeepEqual(columnNames(cols2), want) {
		t.Fatalf("after reopen columns = %v (ok=%v), want %v", columnNames(cols2), ok, want)
	}
}

func TestMigrateRollsBackOnLogicalError(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}

	// The third step is invalid (cannot add a primary key column). The whole
	// migration must be rejected and leave no trace: orders never exists.
	err = db.Migrate(ctx, []string{
		"CREATE TABLE orders (id INT PRIMARY KEY, total FLOAT)",
		"ALTER TABLE orders ADD COLUMN status VARCHAR",
		"ALTER TABLE orders ADD COLUMN bad INT PRIMARY KEY",
	})
	if err == nil {
		t.Fatal("expected Migrate to fail on the invalid step")
	}
	if _, ok := db.Describe("orders"); ok {
		t.Fatal("orders must not exist after a rolled-back migration")
	}
	_ = db.Close()

	db2, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	if _, ok := db2.Describe("orders"); ok {
		t.Fatal("orders must not exist after reopen of a rolled-back migration")
	}
}

func TestMigrateWithDropColumn(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}

	if err := db.Migrate(ctx, []string{
		"CREATE TABLE orders (id INT PRIMARY KEY, note VARCHAR INDEX, total FLOAT)",
		"ALTER TABLE orders DROP COLUMN note",
		"ALTER TABLE orders ADD COLUMN status VARCHAR",
	}); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	want := []string{"id", "total", "status"}
	cols, ok := db.Columns("orders")
	if !ok || !reflect.DeepEqual(columnNames(cols), want) {
		t.Fatalf("columns = %v (ok=%v), want %v", columnNames(cols), ok, want)
	}
	_ = db.Close()

	db2, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	cols2, _ := db2.Columns("orders")
	if !reflect.DeepEqual(columnNames(cols2), want) {
		t.Fatalf("after reopen columns = %v, want %v", columnNames(cols2), want)
	}
}

func TestMigrateIfNotExistsNoop(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(ctx, "CREATE TABLE orders (id INT PRIMARY KEY, total FLOAT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// CREATE IF NOT EXISTS on the existing table is a no-op; the following ALTER
	// must still apply.
	if err := db.Migrate(ctx, []string{
		"CREATE TABLE IF NOT EXISTS orders (id INT PRIMARY KEY, total FLOAT)",
		"ALTER TABLE orders ADD COLUMN status VARCHAR",
	}); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	want := []string{"id", "total", "status"}
	cols, _ := db.Columns("orders")
	if !reflect.DeepEqual(columnNames(cols), want) {
		t.Fatalf("columns = %v, want %v", columnNames(cols), want)
	}
}

func TestMigrateParseErrorAppliesNothing(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	err = db.Migrate(ctx, []string{
		"CREATE TABLE orders (id INT PRIMARY KEY)",
		"CREATE TABLE !!! not valid sql",
	})
	if err == nil {
		t.Fatal("expected a parse error to fail the migration")
	}
	if _, ok := db.Describe("orders"); ok {
		t.Fatal("orders must not exist when an earlier step fails to parse")
	}
}

func TestMigrateEmptyIsNoop(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()
	if err := db.Migrate(ctx, nil); err != nil {
		t.Fatalf("Migrate(nil) = %v, want nil", err)
	}
}

func TestMigrateRequiresDatabase(t *testing.T) {
	e := newExecutor(t)
	if err := e.Migrate(context.Background(), []string{"CREATE TABLE x (id INT PRIMARY KEY)"}); err == nil {
		t.Fatal("expected error: Migrate without an open database")
	}
}

func TestMigrateRejectsNonDDL(t *testing.T) {
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

	err = db.Migrate(ctx, []string{"INSERT INTO users (id, name) VALUES (1, 'a')"})
	if err == nil {
		t.Fatal("expected Migrate to reject a non-DDL statement")
	}
}
