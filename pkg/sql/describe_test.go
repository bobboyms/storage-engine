package sql

import (
	"context"
	"reflect"
	"testing"
)

func TestParseDescribe(t *testing.T) {
	for _, q := range []string{"DESCRIBE users", "DESC users"} {
		stmt, err := Parse(q)
		if err != nil {
			t.Fatalf("Parse(%q): %v", q, err)
		}
		d, ok := stmt.(*DescribeStmt)
		if !ok {
			t.Fatalf("Parse(%q) = %T, want *DescribeStmt", q, stmt)
		}
		if d.Table != "users" {
			t.Fatalf("Parse(%q) table = %q, want users", q, d.Table)
		}
	}
}

func TestParseDescribeMissingTable(t *testing.T) {
	if _, err := Parse("DESCRIBE"); err == nil {
		t.Fatal("expected error for DESCRIBE without a table name")
	}
}

func TestExecDescribe(t *testing.T) {
	e := newExecutor(t)

	rs, err := e.Query(context.Background(), "DESCRIBE users")
	if err != nil {
		t.Fatalf("DESCRIBE users: %v", err)
	}

	wantCols := []string{"column", "type", "key"}
	if !reflect.DeepEqual(rs.Columns, wantCols) {
		t.Fatalf("columns = %v, want %v", rs.Columns, wantCols)
	}

	if got := strColumn(t, rs, "column"); !reflect.DeepEqual(got, []string{"id", "name", "age"}) {
		t.Fatalf("column names = %v", got)
	}
	if got := strColumn(t, rs, "type"); !reflect.DeepEqual(got, []string{"INT", "VARCHAR", "INT"}) {
		t.Fatalf("types = %v", got)
	}
	if got := strColumn(t, rs, "key"); !reflect.DeepEqual(got, []string{"PRI", "", "MUL"}) {
		t.Fatalf("keys = %v", got)
	}
}

func TestExecDescribeUnknownTable(t *testing.T) {
	e := newExecutor(t)
	if _, err := e.Query(context.Background(), "DESCRIBE missing"); err == nil {
		t.Fatal("expected error describing an unknown table")
	}
}
