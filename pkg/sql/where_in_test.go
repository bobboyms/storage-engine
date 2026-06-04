package sql

import (
	"context"
	"testing"
)

func TestParseIn(t *testing.T) {
	sel := parseSelect(t, "SELECT * FROM users WHERE id IN (1, 2, 3)")
	if got := sel.Where.String(); got != "(((id = 1) OR (id = 2)) OR (id = 3))" {
		t.Fatalf("Where = %q", got)
	}
}

func TestParseNotIn(t *testing.T) {
	sel := parseSelect(t, "SELECT * FROM users WHERE id NOT IN (1, 2)")
	if got := sel.Where.String(); got != "((id <> 1) AND (id <> 2))" {
		t.Fatalf("Where = %q", got)
	}
}

func TestParseInEmptyIsError(t *testing.T) {
	if _, err := Parse("SELECT * FROM users WHERE id IN ()"); err == nil {
		t.Fatal("expected error for empty IN list")
	}
}

func TestQueryInIntegration(t *testing.T) {
	e := newExecutor(t)
	ctx := context.Background()
	rs, err := e.Query(ctx, "SELECT id FROM users WHERE id IN (2, 3)")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	ids := sortedInts(intColumn(t, rs, "id"))
	if len(ids) != 2 || ids[0] != 2 || ids[1] != 3 {
		t.Fatalf("ids = %v, want [2 3]", ids)
	}
}

func TestQueryNotInIntegration(t *testing.T) {
	e := newExecutor(t)
	ctx := context.Background()
	rs, err := e.Query(ctx, "SELECT id FROM users WHERE id NOT IN (2, 3)")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	ids := sortedInts(intColumn(t, rs, "id"))
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 4 {
		t.Fatalf("ids = %v, want [1 4]", ids)
	}
}

func TestQueryInStrings(t *testing.T) {
	e := newExecutor(t)
	ctx := context.Background()
	rs, err := e.Query(ctx, "SELECT id FROM users WHERE name IN ('bob', 'carol')")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	ids := sortedInts(intColumn(t, rs, "id"))
	if len(ids) != 2 || ids[0] != 2 || ids[1] != 3 {
		t.Fatalf("ids = %v, want [2 3] (bob=2, carol=3)", ids)
	}
}
