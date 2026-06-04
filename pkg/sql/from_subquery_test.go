package sql

import (
	"context"
	"testing"
)

func TestParseFromSubquery(t *testing.T) {
	sel := parseSelect(t, "SELECT t.x FROM (SELECT id AS x FROM users) AS t")
	if sel.Subquery == nil {
		t.Fatal("Subquery = nil, want derived table")
	}
	if sel.Alias != "t" {
		t.Fatalf("alias = %q, want t", sel.Alias)
	}
	if sel.Subquery.Table != "users" {
		t.Fatalf("subquery table = %q, want users", sel.Subquery.Table)
	}
}

func TestQueryFromSubquerySimple(t *testing.T) {
	e := newJoinExecutor(t)
	// users ages: 1->30, 2->25, 3->40. age >= 30 -> ids 1, 3.
	rs, err := e.Query(context.Background(),
		"SELECT t.x FROM (SELECT id AS x FROM users WHERE age >= 30) t ORDER BY t.x")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	ids := intColumn(t, rs, "x")
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 3 {
		t.Fatalf("ids = %v, want [1 3]", ids)
	}
}

func TestQueryFromSubqueryAggregate(t *testing.T) {
	e := newJoinExecutor(t)
	// Inner groups orders by user_id with counts: u1->2, u2->1.
	// Outer keeps only groups with c >= 2.
	rs, err := e.Query(context.Background(),
		"SELECT s.user_id FROM (SELECT user_id, COUNT(*) AS c FROM orders GROUP BY user_id) s WHERE s.c >= 2")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	ids := intColumn(t, rs, "user_id")
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("ids = %v, want [1] (only user 1 has >= 2 orders)", ids)
	}
}

func TestQueryJoinWithDerivedTable(t *testing.T) {
	e := newJoinExecutor(t)
	rs, err := e.Query(context.Background(),
		"SELECT u.name, s.c FROM users u JOIN (SELECT user_id, COUNT(*) AS c FROM orders GROUP BY user_id) s ON u.id = s.user_id ORDER BY u.name")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	names := strColumn(t, rs, "name")
	counts := intColumn(t, rs, "c")
	if len(names) != 2 || names[0] != "alice" || names[1] != "bob" {
		t.Fatalf("names = %v, want [alice bob]", names)
	}
	if counts[0] != 2 || counts[1] != 1 {
		t.Fatalf("counts = %v, want [2 1]", counts)
	}
}
