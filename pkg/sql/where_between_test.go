package sql

import (
	"context"
	"sort"
	"testing"
)

func TestParseBetween(t *testing.T) {
	sel := parseSelect(t, "SELECT * FROM users WHERE age BETWEEN 18 AND 30")
	if got := sel.Where.String(); got != "((age >= 18) AND (age <= 30))" {
		t.Fatalf("Where = %q, want ((age >= 18) AND (age <= 30))", got)
	}
}

func TestParseNotBetween(t *testing.T) {
	sel := parseSelect(t, "SELECT * FROM users WHERE age NOT BETWEEN 18 AND 30")
	if got := sel.Where.String(); got != "((age < 18) OR (age > 30))" {
		t.Fatalf("Where = %q, want ((age < 18) OR (age > 30))", got)
	}
}

func sortedInts(in []int64) []int64 {
	out := append([]int64(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func TestQueryBetweenIntegration(t *testing.T) {
	e := newExecutor(t)
	ctx := context.Background()
	// Seeded ages: id1=30, id2=25, id3=40, id4=30.
	rs, err := e.Query(ctx, "SELECT id FROM users WHERE age BETWEEN 26 AND 35")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	ids := sortedInts(intColumn(t, rs, "id"))
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 4 {
		t.Fatalf("ids = %v, want [1 4]", ids)
	}
}

func TestQueryNotBetweenIntegration(t *testing.T) {
	e := newExecutor(t)
	ctx := context.Background()
	rs, err := e.Query(ctx, "SELECT id FROM users WHERE age NOT BETWEEN 26 AND 35")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	ids := sortedInts(intColumn(t, rs, "id"))
	if len(ids) != 2 || ids[0] != 2 || ids[1] != 3 {
		t.Fatalf("ids = %v, want [2 3]", ids)
	}
}
