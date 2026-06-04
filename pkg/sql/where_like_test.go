package sql

import (
	"context"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestParseLike(t *testing.T) {
	sel := parseSelect(t, "SELECT * FROM users WHERE name LIKE 'b%'")
	if got := sel.Where.String(); got != "(name LIKE 'b%')" {
		t.Fatalf("Where = %q, want (name LIKE 'b%%')", got)
	}
}

func TestParseNotLike(t *testing.T) {
	sel := parseSelect(t, "SELECT * FROM users WHERE name NOT LIKE 'b%'")
	if got := sel.Where.String(); got != "(name NOT LIKE 'b%')" {
		t.Fatalf("Where = %q, want (name NOT LIKE 'b%%')", got)
	}
}

func TestEvaluateLikePatterns(t *testing.T) {
	row := map[string]types.Comparable{"name": types.VarcharKey("bob")}
	cases := []struct {
		pattern string
		want    bool
	}{
		{"b%", true},
		{"a%", false},
		{"%b", true},
		{"_ob", true},
		{"b_b", true},
		{"%o%", true},
		{"bob", true},
		{"bo", false},
		{"%", true},
	}
	for _, c := range cases {
		got := evalQuery(t, "name LIKE '"+c.pattern+"'", row)
		if got != c.want {
			t.Fatalf("name LIKE %q = %v, want %v", c.pattern, got, c.want)
		}
	}
	if !evalQuery(t, "name NOT LIKE 'a%'", row) {
		t.Fatal("name NOT LIKE 'a%' should be true for bob")
	}
}

func TestEvaluateLikeTypeMismatch(t *testing.T) {
	row := map[string]types.Comparable{"age": types.IntKey(30)}
	if _, err := Evaluate(mustWhere(t, "SELECT * FROM t WHERE age LIKE '3%'"), row); err == nil {
		t.Fatal("expected error for LIKE on a non-text column")
	}
}

func TestQueryLikeIntegration(t *testing.T) {
	e := newExecutor(t)
	ctx := context.Background()
	// Seeded names: alice, bob, carol, dave (ids 1..4).
	rs, err := e.Query(ctx, "SELECT id FROM users WHERE name LIKE '%a%'")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	ids := sortedInts(intColumn(t, rs, "id"))
	if len(ids) != 3 || ids[0] != 1 || ids[1] != 3 || ids[2] != 4 {
		t.Fatalf("ids = %v, want [1 3 4] (alice, carol, dave contain 'a')", ids)
	}

	rs, err = e.Query(ctx, "SELECT id FROM users WHERE name LIKE 'c%'")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	ids = intColumn(t, rs, "id")
	if len(ids) != 1 || ids[0] != 3 {
		t.Fatalf("ids = %v, want [3] (carol)", ids)
	}
}
