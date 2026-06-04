package sql

import (
	"context"
	"testing"
)

func TestParseColumnUnique(t *testing.T) {
	stmt, err := Parse("CREATE TABLE t (id INT PRIMARY KEY, email VARCHAR UNIQUE)")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ct := stmt.(*CreateTableStmt)
	var email *ColumnDef
	for i := range ct.Columns {
		if ct.Columns[i].Name == "email" {
			email = &ct.Columns[i]
		}
	}
	if email == nil {
		t.Fatal("email column not parsed")
	}
	if !email.Unique {
		t.Fatal("email column should be UNIQUE")
	}
}

func TestParseTableUnique(t *testing.T) {
	stmt, err := Parse("CREATE TABLE t (id INT PRIMARY KEY, environment_id INT, email VARCHAR, UNIQUE (environment_id, email))")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ct := stmt.(*CreateTableStmt)
	if len(ct.Indexes) != 1 {
		t.Fatalf("table indexes = %d, want 1", len(ct.Indexes))
	}
	ic := ct.Indexes[0]
	if !ic.Unique {
		t.Fatal("table-level UNIQUE clause should set Unique")
	}
	if len(ic.Columns) != 2 || ic.Columns[0] != "environment_id" || ic.Columns[1] != "email" {
		t.Fatalf("columns = %v", ic.Columns)
	}
}

func TestSchemaFromCreateUniqueIndexes(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR UNIQUE, environment_id INT, email VARCHAR, UNIQUE (environment_id, email))")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	s := schemaFromCreate(stmt.(*CreateTableStmt))

	var singleUnique, compositeUnique *IndexDef
	for i := range s.Indexes {
		idx := &s.Indexes[i]
		if idx.Unique && !idx.composite() {
			singleUnique = idx
		}
		if idx.Unique && idx.composite() {
			compositeUnique = idx
		}
	}
	if singleUnique == nil || singleUnique.Column != "name" {
		t.Fatalf("single-column unique index missing/wrong: %+v", singleUnique)
	}
	if compositeUnique == nil {
		t.Fatal("composite unique index missing")
	}
	if len(compositeUnique.Columns) != 2 {
		t.Fatalf("composite unique columns = %v", compositeUnique.Columns)
	}
}

func TestUniqueConstraintPersists(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, environment_id INT, email VARCHAR, UNIQUE (environment_id, email))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_ = db.Close()

	db2, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	schema, ok := db2.Describe("users")
	if !ok {
		t.Fatal("users not found after reopen")
	}
	var found bool
	for _, idx := range schema.Indexes {
		if idx.Unique && idx.composite() {
			found = true
		}
	}
	if !found {
		t.Fatalf("composite unique index not persisted: %+v", schema.Indexes)
	}
}

func TestErrUniqueViolationIsDistinct(t *testing.T) {
	if ErrUniqueViolation == nil {
		t.Fatal("ErrUniqueViolation must be defined")
	}
	if ErrUniqueViolation == ErrDuplicateColumn {
		t.Fatal("ErrUniqueViolation must be distinct from ErrDuplicateColumn")
	}
}

// Step 1 only adds the UNIQUE surface and backing index; the unique index still
// serves equality lookups exactly like a composite index. Enforcement of the
// constraint lands in a later increment, so this test does not assert that a
// duplicate insert is rejected.
func TestUniqueCompositeServesLookups(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, t.TempDir(), OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, environment_id INT, email VARCHAR, UNIQUE (environment_id, email))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, environment_id, email) VALUES (?, ?, ?)", 1, 7, "a@x.com"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	rs, err := db.Query(ctx, "SELECT id FROM users WHERE environment_id = ? AND email = ?", 7, "a@x.com")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if got := intColumn(t, rs, "id"); len(got) != 1 || got[0] != 1 {
		t.Fatalf("ids = %v, want [1]", got)
	}
}
