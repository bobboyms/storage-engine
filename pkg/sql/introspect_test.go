package sql

import (
	"context"
	"reflect"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/storage"
)

func TestExecutorDescribe(t *testing.T) {
	e := newExecutor(t)

	schema, ok := e.Describe("users")
	if !ok {
		t.Fatal("Describe(users): expected ok")
	}
	wantCols := []Column{
		{Name: "id", Type: storage.TypeInt},
		{Name: "name", Type: storage.TypeVarchar},
		{Name: "age", Type: storage.TypeInt},
	}
	if !reflect.DeepEqual(schema.Columns, wantCols) {
		t.Fatalf("columns = %+v, want %+v", schema.Columns, wantCols)
	}
	wantIdx := []IndexDef{
		{Name: "id", Column: "id", Primary: true},
		{Name: "age", Column: "age"},
	}
	if !reflect.DeepEqual(schema.Indexes, wantIdx) {
		t.Fatalf("indexes = %+v, want %+v", schema.Indexes, wantIdx)
	}

	if _, ok := e.Describe("missing"); ok {
		t.Fatal("Describe(missing): expected not ok")
	}
}

func TestExecutorDescribeReturnsCopy(t *testing.T) {
	e := newExecutor(t)

	schema, ok := e.Describe("users")
	if !ok {
		t.Fatal("Describe(users): expected ok")
	}
	schema.Columns[0].Name = "mutated"
	schema.Indexes[0].Name = "mutated"

	again, _ := e.Describe("users")
	if again.Columns[0].Name != "id" {
		t.Fatalf("internal column mutated: %q", again.Columns[0].Name)
	}
	if again.Indexes[0].Name != "id" {
		t.Fatalf("internal index mutated: %q", again.Indexes[0].Name)
	}
}

func TestExecutorColumns(t *testing.T) {
	e := newExecutor(t)

	cols, ok := e.Columns("users")
	if !ok {
		t.Fatal("Columns(users): expected ok")
	}
	want := []Column{
		{Name: "id", Type: storage.TypeInt},
		{Name: "name", Type: storage.TypeVarchar},
		{Name: "age", Type: storage.TypeInt},
	}
	if !reflect.DeepEqual(cols, want) {
		t.Fatalf("columns = %+v, want %+v", cols, want)
	}

	cols[0].Name = "mutated"
	again, _ := e.Columns("users")
	if again[0].Name != "id" {
		t.Fatalf("internal column mutated: %q", again[0].Name)
	}

	if _, ok := e.Columns("missing"); ok {
		t.Fatal("Columns(missing): expected not ok")
	}
}

func TestExecutorTables(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	for _, ddl := range []string{
		"CREATE TABLE beta (id INT PRIMARY KEY)",
		"CREATE TABLE alpha (id INT PRIMARY KEY)",
		"CREATE TABLE gamma (id INT PRIMARY KEY)",
	} {
		if _, err := db.Exec(ctx, ddl); err != nil {
			t.Fatalf("Exec %q: %v", ddl, err)
		}
	}

	got := db.Tables()
	want := []string{"alpha", "beta", "gamma"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Tables() = %v, want %v (must be sorted)", got, want)
	}
}
