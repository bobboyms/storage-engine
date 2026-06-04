package sql

import (
	"context"
	"testing"
)

func TestParseTableAliasAndQualifiedRefs(t *testing.T) {
	sel := parseSelect(t, "SELECT u.id, u.name FROM users AS u WHERE u.age > 18")
	if sel.Table != "users" || sel.Alias != "u" {
		t.Fatalf("table/alias = %q/%q, want users/u", sel.Table, sel.Alias)
	}
	if sel.Items[0].Column == nil || sel.Items[0].Column.Qualifier != "u" || sel.Items[0].Column.Name != "id" {
		t.Fatalf("item[0] = %+v, want u.id", sel.Items[0].Column)
	}
	if got := sel.Where.String(); got != "(u.age > 18)" {
		t.Fatalf("where = %q, want (u.age > 18)", got)
	}
}

func TestParseAliasWithoutAs(t *testing.T) {
	sel := parseSelect(t, "SELECT u.id FROM users u")
	if sel.Alias != "u" {
		t.Fatalf("alias = %q, want u", sel.Alias)
	}
}

func TestQueryQualifiedColumns(t *testing.T) {
	e := newExecutor(t)
	ctx := context.Background()
	// ages >= 30: id1(30), id3(40), id4(30).
	rs, err := e.Query(ctx, "SELECT u.id FROM users u WHERE u.age >= 30 ORDER BY u.id")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	ids := intColumn(t, rs, "id")
	want := []int64{1, 3, 4}
	if len(ids) != len(want) {
		t.Fatalf("ids = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("ids = %v, want %v", ids, want)
		}
	}
}

func TestQueryQualifiedGroupBy(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT u.age, COUNT(*) FROM users u GROUP BY u.age ORDER BY u.age")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rs.Rows) != 3 {
		t.Fatalf("groups = %d, want 3", len(rs.Rows))
	}
	if rs.Columns[0] != "age" {
		t.Fatalf("col0 = %q, want age", rs.Columns[0])
	}
}

func TestQueryUnknownQualifierError(t *testing.T) {
	e := newExecutor(t)
	if _, err := e.Query(context.Background(), "SELECT x.id FROM users u"); err == nil {
		t.Fatal("expected error for unknown qualifier x")
	}
}
