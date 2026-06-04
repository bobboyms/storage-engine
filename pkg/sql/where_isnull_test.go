package sql

import (
	"context"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestParseIsNull(t *testing.T) {
	sel := parseSelect(t, "SELECT * FROM users WHERE name IS NULL")
	if got := sel.Where.String(); got != "(name IS NULL)" {
		t.Fatalf("Where = %q, want (name IS NULL)", got)
	}
}

func TestParseIsNotNull(t *testing.T) {
	sel := parseSelect(t, "SELECT * FROM users WHERE name IS NOT NULL")
	if got := sel.Where.String(); got != "(name IS NOT NULL)" {
		t.Fatalf("Where = %q, want (name IS NOT NULL)", got)
	}
}

func TestEvaluateIsNull(t *testing.T) {
	nullRow := map[string]types.Comparable{"name": types.NullKey{}}
	valRow := map[string]types.Comparable{"name": types.VarcharKey("bob")}

	if !evalQuery(t, "name IS NULL", nullRow) {
		t.Fatal("name IS NULL should be true for NULL value")
	}
	if evalQuery(t, "name IS NULL", valRow) {
		t.Fatal("name IS NULL should be false for non-null value")
	}
	if evalQuery(t, "name IS NOT NULL", valRow) == false {
		t.Fatal("name IS NOT NULL should be true for non-null value")
	}
	if evalQuery(t, "name IS NOT NULL", nullRow) {
		t.Fatal("name IS NOT NULL should be false for NULL value")
	}
}

func TestQueryIsNullIntegration(t *testing.T) {
	e := newExecutor(t)
	ctx := context.Background()
	// Insert a row without the (non-indexed) name column -> name is NULL.
	if _, err := e.Exec(ctx, "INSERT INTO users (id, age) VALUES (5, 22)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	rs, err := e.Query(ctx, "SELECT id FROM users WHERE name IS NULL")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	ids := intColumn(t, rs, "id")
	if len(ids) != 1 || ids[0] != 5 {
		t.Fatalf("ids = %v, want [5]", ids)
	}

	rs, err = e.Query(ctx, "SELECT id FROM users WHERE name IS NOT NULL")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := intColumn(t, rs, "id"); len(got) != 4 {
		t.Fatalf("IS NOT NULL returned %d rows, want 4 seeded rows", len(got))
	}
}
