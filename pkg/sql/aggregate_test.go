package sql

import (
	"context"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestParseAggregateItems(t *testing.T) {
	sel := parseSelect(t, "SELECT COUNT(*), SUM(age) FROM users")
	if len(sel.Items) != 2 {
		t.Fatalf("items = %d, want 2", len(sel.Items))
	}
	if sel.Items[0].Agg == nil || sel.Items[0].Agg.Func != "COUNT" || !sel.Items[0].Agg.Star {
		t.Fatalf("item[0] = %+v, want COUNT(*)", sel.Items[0].Agg)
	}
	if sel.Items[1].Agg == nil || sel.Items[1].Agg.Func != "SUM" || sel.Items[1].Agg.Column.Name != "age" {
		t.Fatalf("item[1] = %+v, want SUM(age)", sel.Items[1].Agg)
	}
}

func TestParseAggregateDistinctAndAlias(t *testing.T) {
	sel := parseSelect(t, "SELECT COUNT(DISTINCT age) AS n FROM users")
	a := sel.Items[0].Agg
	if a == nil || a.Func != "COUNT" || !a.Distinct || a.Column.Name != "age" {
		t.Fatalf("agg = %+v, want COUNT(DISTINCT age)", a)
	}
	if sel.Items[0].Alias != "n" {
		t.Fatalf("alias = %q, want n", sel.Items[0].Alias)
	}
}

func aggResult(t *testing.T, e *Executor, query string) (string, types.Comparable) {
	t.Helper()
	rs, err := e.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("Query(%q): %v", query, err)
	}
	if len(rs.Rows) != 1 || len(rs.Columns) != 1 {
		t.Fatalf("query %q returned %d rows x %d cols, want 1x1", query, len(rs.Rows), len(rs.Columns))
	}
	return rs.Columns[0], rs.Rows[0][0]
}

func TestQueryWholeTableAggregates(t *testing.T) {
	e := newExecutor(t)
	// Seeded ages: 30, 25, 40, 30 over 4 rows.
	name, v := aggResult(t, e, "SELECT COUNT(*) FROM users")
	if name != "count(*)" || v != types.IntKey(4) {
		t.Fatalf("COUNT(*) = %v as %q, want 4 count(*)", v, name)
	}
	if _, v := aggResult(t, e, "SELECT SUM(age) FROM users"); v != types.IntKey(125) {
		t.Fatalf("SUM(age) = %v, want 125", v)
	}
	if _, v := aggResult(t, e, "SELECT MIN(age) FROM users"); v != types.IntKey(25) {
		t.Fatalf("MIN(age) = %v, want 25", v)
	}
	if _, v := aggResult(t, e, "SELECT MAX(age) FROM users"); v != types.IntKey(40) {
		t.Fatalf("MAX(age) = %v, want 40", v)
	}
	if _, v := aggResult(t, e, "SELECT AVG(age) FROM users"); v != types.FloatKey(31.25) {
		t.Fatalf("AVG(age) = %v, want 31.25", v)
	}
	if _, v := aggResult(t, e, "SELECT COUNT(DISTINCT age) FROM users"); v != types.IntKey(3) {
		t.Fatalf("COUNT(DISTINCT age) = %v, want 3 (30,25,40)", v)
	}
}

func TestQueryAggregateAlias(t *testing.T) {
	e := newExecutor(t)
	name, v := aggResult(t, e, "SELECT COUNT(*) AS total FROM users")
	if name != "total" || v != types.IntKey(4) {
		t.Fatalf("got %v as %q, want 4 total", v, name)
	}
}

func TestQueryCountColumnSkipsNull(t *testing.T) {
	e := newExecutor(t)
	ctx := context.Background()
	// Row without the non-indexed name column -> name is NULL.
	if _, err := e.Exec(ctx, "INSERT INTO users (id, age) VALUES (5, 22)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, v := aggResult(t, e, "SELECT COUNT(*) FROM users"); v != types.IntKey(5) {
		t.Fatalf("COUNT(*) = %v, want 5", v)
	}
	if _, v := aggResult(t, e, "SELECT COUNT(name) FROM users"); v != types.IntKey(4) {
		t.Fatalf("COUNT(name) = %v, want 4 (NULL skipped)", v)
	}
}

func TestQueryMixedAggregateAndColumnError(t *testing.T) {
	e := newExecutor(t)
	if _, err := e.Query(context.Background(), "SELECT id, COUNT(*) FROM users"); err == nil {
		t.Fatal("expected error for bare column mixed with aggregate without GROUP BY")
	}
}
