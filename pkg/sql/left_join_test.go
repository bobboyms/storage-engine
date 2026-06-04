package sql

import (
	"context"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestParseLeftJoin(t *testing.T) {
	sel := parseSelect(t, "SELECT u.name FROM users u LEFT OUTER JOIN orders o ON u.id = o.user_id")
	if len(sel.Joins) != 1 || !sel.Joins[0].Left {
		t.Fatalf("joins = %+v, want one LEFT join", sel.Joins)
	}
}

func TestQueryLeftJoinKeepsUnmatched(t *testing.T) {
	e := newJoinExecutor(t)
	// carol (id 3) has no orders -> should still appear with NULL amount.
	rs, err := e.Query(context.Background(),
		"SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.name")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rs.Rows) != 4 {
		t.Fatalf("rows = %d, want 4 (alice x2, bob, carol)", len(rs.Rows))
	}
	amountIdx := colIndex(rs, "amount")
	nameIdx := colIndex(rs, "name")
	// Find carol's row and assert NULL amount.
	foundCarol := false
	for _, row := range rs.Rows {
		if row[nameIdx] == types.VarcharKey("carol") {
			foundCarol = true
			if _, isNull := row[amountIdx].(types.NullKey); !isNull {
				t.Fatalf("carol amount = %v, want NULL", row[amountIdx])
			}
		}
	}
	if !foundCarol {
		t.Fatal("carol row missing from LEFT JOIN result")
	}
}

func TestQueryLeftJoinNullFilter(t *testing.T) {
	e := newJoinExecutor(t)
	rs, err := e.Query(context.Background(),
		"SELECT u.name FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE o.amount IS NULL")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	names := strColumn(t, rs, "name")
	if len(names) != 1 || names[0] != "carol" {
		t.Fatalf("names = %v, want [carol] (only user without orders)", names)
	}
}
