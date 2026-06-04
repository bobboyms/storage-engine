package sql

import (
	"errors"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/storage"
)

func TestParseCreateTable(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT INDEX)")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ct, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("got %T, want *CreateTableStmt", stmt)
	}
	if ct.Table != "users" {
		t.Fatalf("table = %q, want users", ct.Table)
	}
	if len(ct.Columns) != 3 {
		t.Fatalf("columns = %d, want 3", len(ct.Columns))
	}

	id := ct.Columns[0]
	if id.Name != "id" || id.Type != storage.TypeInt || !id.Primary || id.Index {
		t.Fatalf("col[0] = %+v, want id INT PRIMARY KEY", id)
	}
	name := ct.Columns[1]
	if name.Name != "name" || name.Type != storage.TypeVarchar || name.Primary || name.Index {
		t.Fatalf("col[1] = %+v, want name VARCHAR", name)
	}
	age := ct.Columns[2]
	if age.Name != "age" || age.Type != storage.TypeInt || age.Primary || !age.Index {
		t.Fatalf("col[2] = %+v, want age INT INDEX", age)
	}
}

func TestParseCreateTableTypes(t *testing.T) {
	stmt, err := Parse("CREATE TABLE t (a INT PRIMARY KEY, b VARCHAR, c BOOL, d FLOAT, e DATE)")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ct := stmt.(*CreateTableStmt)
	want := []storage.DataType{
		storage.TypeInt, storage.TypeVarchar, storage.TypeBoolean, storage.TypeFloat, storage.TypeDate,
	}
	for i, w := range want {
		if ct.Columns[i].Type != w {
			t.Fatalf("col[%d].Type = %v, want %v", i, ct.Columns[i].Type, w)
		}
	}
}

func TestParseCreateTableErrors(t *testing.T) {
	inputs := []string{
		"CREATE users (id INT PRIMARY KEY)",        // missing TABLE
		"CREATE TABLE users id INT",                // missing (
		"CREATE TABLE users (id INT PRIMARY KEY",   // missing )
		"CREATE TABLE users (id NOPE PRIMARY KEY)", // unknown type
		"CREATE TABLE users ()",                    // no columns
		"CREATE TABLE users (id INT PRIMARY)",      // PRIMARY without KEY
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			if _, err := Parse(in); !errors.Is(err, ErrParse) {
				t.Fatalf("Parse(%q) error = %v, want ErrParse", in, err)
			}
		})
	}
}
