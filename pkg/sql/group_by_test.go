package sql

import (
	"context"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestParseGroupByHaving(t *testing.T) {
	sel := parseSelect(t, "SELECT age, COUNT(*) FROM users GROUP BY age HAVING COUNT(*) > 1")
	if len(sel.GroupBy) != 1 || sel.GroupBy[0] != "age" {
		t.Fatalf("GroupBy = %v, want [age]", sel.GroupBy)
	}
	if sel.Having == nil || sel.Having.String() != "(count(*) > 1)" {
		t.Fatalf("Having = %v, want (count(*) > 1)", sel.Having)
	}
}

// twoCol returns the two-column result as parallel slices for assertions.
func twoCol(t *testing.T, e *Executor, query string) ([]int64, []int64) {
	t.Helper()
	rs, err := e.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("Query(%q): %v", query, err)
	}
	if len(rs.Columns) != 2 {
		t.Fatalf("columns = %v, want 2", rs.Columns)
	}
	a := make([]int64, len(rs.Rows))
	b := make([]int64, len(rs.Rows))
	for i, row := range rs.Rows {
		ka, ok := row[0].(types.IntKey)
		if !ok {
			t.Fatalf("row %d col0 = %T, want IntKey", i, row[0])
		}
		kb, ok := row[1].(types.IntKey)
		if !ok {
			t.Fatalf("row %d col1 = %T, want IntKey", i, row[1])
		}
		a[i] = int64(ka)
		b[i] = int64(kb)
	}
	return a, b
}

func TestQueryGroupByCount(t *testing.T) {
	e := newExecutor(t)
	// ages: 25(x1), 30(x2), 40(x1).
	ages, counts := twoCol(t, e, "SELECT age, COUNT(*) FROM users GROUP BY age ORDER BY age")
	wantAges := []int64{25, 30, 40}
	wantCounts := []int64{1, 2, 1}
	if len(ages) != 3 {
		t.Fatalf("groups = %d, want 3", len(ages))
	}
	for i := range wantAges {
		if ages[i] != wantAges[i] || counts[i] != wantCounts[i] {
			t.Fatalf("got (%v,%v), want (%v,%v)", ages, counts, wantAges, wantCounts)
		}
	}
}

func TestQueryGroupByHaving(t *testing.T) {
	e := newExecutor(t)
	ages, counts := twoCol(t, e, "SELECT age, COUNT(*) FROM users GROUP BY age HAVING COUNT(*) > 1 ORDER BY age")
	if len(ages) != 1 || ages[0] != 30 || counts[0] != 2 {
		t.Fatalf("got ages=%v counts=%v, want [30] [2]", ages, counts)
	}
}

func TestQueryGroupBySum(t *testing.T) {
	e := newExecutor(t)
	// ids per age: 25->{2}, 30->{1,4}, 40->{3}. SUM(id): 2, 5, 3.
	ages, sums := twoCol(t, e, "SELECT age, SUM(id) FROM users GROUP BY age ORDER BY age")
	wantAges := []int64{25, 30, 40}
	wantSums := []int64{2, 5, 3}
	for i := range wantAges {
		if ages[i] != wantAges[i] || sums[i] != wantSums[i] {
			t.Fatalf("got (%v,%v), want (%v,%v)", ages, sums, wantAges, wantSums)
		}
	}
}

func TestQueryGroupByBareColumnError(t *testing.T) {
	e := newExecutor(t)
	// id is not a grouping column.
	if _, err := e.Query(context.Background(), "SELECT id, age FROM users GROUP BY age"); err == nil {
		t.Fatal("expected error: id not in GROUP BY")
	}
}

func TestQueryGroupByEmptyTable(t *testing.T) {
	e := newExecutor(t)
	ctx := context.Background()
	// Delete everything, then GROUP BY should yield zero groups...
	if _, err := e.Exec(ctx, "DELETE FROM users"); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	rs, err := e.Query(ctx, "SELECT age, COUNT(*) FROM users GROUP BY age")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rs.Rows) != 0 {
		t.Fatalf("rows = %d, want 0 groups", len(rs.Rows))
	}
	// ...but a bare aggregate (no GROUP BY) still returns one row.
	rs, err = e.Query(ctx, "SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0][0] != types.IntKey(0) {
		t.Fatalf("COUNT(*) on empty table = %v, want one row with 0", rs.Rows)
	}
}
