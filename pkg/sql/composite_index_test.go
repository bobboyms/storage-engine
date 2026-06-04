package sql

import (
	"context"
	"reflect"
	"testing"
)

func TestParseCreateTableCompositeIndex(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INT PRIMARY KEY, environment_id INT, email VARCHAR, INDEX (environment_id, email))")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ct, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("got %T, want *CreateTableStmt", stmt)
	}
	if len(ct.Indexes) != 1 {
		t.Fatalf("table indexes = %d, want 1", len(ct.Indexes))
	}
	if got := ct.Indexes[0].Columns; !reflect.DeepEqual(got, []string{"environment_id", "email"}) {
		t.Fatalf("index columns = %v, want [environment_id email]", got)
	}
}

func TestSchemaFromCreateBuildsCompositeIndex(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INT PRIMARY KEY, environment_id INT, email VARCHAR, INDEX (environment_id, email))")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	s := schemaFromCreate(stmt.(*CreateTableStmt))

	var found *IndexDef
	for i := range s.Indexes {
		if len(s.Indexes[i].Columns) > 1 {
			found = &s.Indexes[i]
		}
	}
	if found == nil {
		t.Fatal("composite index not found in schema")
	}
	if !reflect.DeepEqual(found.Columns, []string{"environment_id", "email"}) {
		t.Fatalf("composite columns = %v", found.Columns)
	}
	if found.Primary {
		t.Fatal("composite index must not be primary")
	}
}

func compositeUsersSchema(t *testing.T) *TableSchema {
	t.Helper()
	stmt, err := Parse("CREATE TABLE users (id INT PRIMARY KEY, environment_id INT, email VARCHAR, INDEX (environment_id, email))")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	s := schemaFromCreate(stmt.(*CreateTableStmt))
	return &s
}

func TestPlanUsesCompositeIndexForAndEquality(t *testing.T) {
	s := compositeUsersSchema(t)
	sel := parseSelect(t, "SELECT id FROM users WHERE environment_id = 1 AND email = 'a@x.com'")
	plan, err := Plan(sel, s)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	want := compositeIndexName([]string{"environment_id", "email"})
	if plan.IndexName != want {
		t.Fatalf("index = %q, want composite %q", plan.IndexName, want)
	}
	if plan.Lower == nil || plan.Upper == nil {
		t.Fatalf("composite equality should set both bounds, got [%v, %v]", plan.Lower, plan.Upper)
	}
}

func TestPlanSkipsCompositeIndexWhenOnlyFirstColumnGiven(t *testing.T) {
	// Only the leading column is constrained: the composite index needs every
	// column for an exact-match key, so the plan must fall back (not pick it).
	s := compositeUsersSchema(t)
	sel := parseSelect(t, "SELECT id FROM users WHERE environment_id = 1")
	plan, err := Plan(sel, s)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.IndexName == compositeIndexName([]string{"environment_id", "email"}) {
		t.Fatal("composite index must not be chosen without all its columns")
	}
}

func TestCompositeIndexUpdateRefreshesKey(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, t.TempDir(), OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, environment_id INT, email VARCHAR, INDEX (environment_id, email))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, environment_id, email) VALUES (?, ?, ?)", 1, 1, "old@x.com"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := db.Exec(ctx, "UPDATE users SET email = ? WHERE id = ?", "new@x.com", 1); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}

	// Lookup by the new tuple finds the row; the old tuple does not.
	rs, _ := db.Query(ctx, "SELECT id FROM users WHERE environment_id = ? AND email = ?", 1, "new@x.com")
	if got := intColumn(t, rs, "id"); len(got) != 1 || got[0] != 1 {
		t.Fatalf("new tuple ids = %v, want [1]", got)
	}
	rs, _ = db.Query(ctx, "SELECT id FROM users WHERE environment_id = ? AND email = ?", 1, "old@x.com")
	if got := intColumn(t, rs, "id"); len(got) != 0 {
		t.Fatalf("old tuple ids = %v, want none", got)
	}
}

func TestCompositeIndexSyntheticFieldNotProjected(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, t.TempDir(), OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, environment_id INT, email VARCHAR, INDEX (environment_id, email))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, environment_id, email) VALUES (?, ?, ?)", 1, 1, "a@x.com"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	rs, err := db.Query(ctx, "SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT *: %v", err)
	}
	want := []string{"id", "environment_id", "email"}
	if !reflect.DeepEqual(rs.Columns, want) {
		t.Fatalf("SELECT * columns = %v, want %v (no synthetic index field)", rs.Columns, want)
	}
}

func TestCompositeIndexLookupEndToEnd(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, environment_id INT, email VARCHAR, INDEX (environment_id, email))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	seed := []struct {
		id, env int
		email   string
	}{
		{1, 1, "a@x.com"},
		{2, 1, "b@x.com"},
		{3, 2, "a@x.com"},
	}
	for _, r := range seed {
		if _, err := db.Exec(ctx, "INSERT INTO users (id, environment_id, email) VALUES (?, ?, ?)", r.id, r.env, r.email); err != nil {
			t.Fatalf("INSERT %d: %v", r.id, err)
		}
	}

	rs, err := db.Query(ctx, "SELECT id FROM users WHERE environment_id = ? AND email = ?", 1, "a@x.com")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if got := intColumn(t, rs, "id"); len(got) != 1 || got[0] != 1 {
		t.Fatalf("ids = %v, want [1]", got)
	}
	_ = db.Close()

	// The composite index must persist and serve lookups after reopen.
	db2, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	rs2, err := db2.Query(ctx, "SELECT id FROM users WHERE environment_id = ? AND email = ?", 2, "a@x.com")
	if err != nil {
		t.Fatalf("SELECT after reopen: %v", err)
	}
	if got := intColumn(t, rs2, "id"); len(got) != 1 || got[0] != 3 {
		t.Fatalf("after reopen ids = %v, want [3]", got)
	}
}
