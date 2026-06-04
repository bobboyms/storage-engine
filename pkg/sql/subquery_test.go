package sql

import (
	"context"
	"testing"
)

func TestQueryInSubquery(t *testing.T) {
	e := newJoinExecutor(t)
	// orders exist for users 1 and 2; user 3 (carol) has none.
	rs, err := e.Query(context.Background(),
		"SELECT u.name FROM users u WHERE u.id IN (SELECT user_id FROM orders) ORDER BY u.name")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	names := strColumn(t, rs, "name")
	if len(names) != 2 || names[0] != "alice" || names[1] != "bob" {
		t.Fatalf("names = %v, want [alice bob]", names)
	}
}

func TestQueryNotInSubquery(t *testing.T) {
	e := newJoinExecutor(t)
	rs, err := e.Query(context.Background(),
		"SELECT u.name FROM users u WHERE u.id NOT IN (SELECT user_id FROM orders)")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	names := strColumn(t, rs, "name")
	if len(names) != 1 || names[0] != "carol" {
		t.Fatalf("names = %v, want [carol]", names)
	}
}

func TestQueryScalarSubquery(t *testing.T) {
	e := newJoinExecutor(t)
	// MAX(age) over users is 40 (carol).
	rs, err := e.Query(context.Background(),
		"SELECT u.name FROM users u WHERE u.age = (SELECT MAX(age) FROM users)")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	names := strColumn(t, rs, "name")
	if len(names) != 1 || names[0] != "carol" {
		t.Fatalf("names = %v, want [carol]", names)
	}
}

func TestQueryCorrelatedExists(t *testing.T) {
	e := newJoinExecutor(t)
	rs, err := e.Query(context.Background(),
		"SELECT u.name FROM users u WHERE EXISTS (SELECT o.id FROM orders o WHERE o.user_id = u.id) ORDER BY u.name")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	names := strColumn(t, rs, "name")
	if len(names) != 2 || names[0] != "alice" || names[1] != "bob" {
		t.Fatalf("names = %v, want [alice bob]", names)
	}
}

func TestQueryCorrelatedNotExists(t *testing.T) {
	e := newJoinExecutor(t)
	rs, err := e.Query(context.Background(),
		"SELECT u.name FROM users u WHERE NOT EXISTS (SELECT o.id FROM orders o WHERE o.user_id = u.id)")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	names := strColumn(t, rs, "name")
	if len(names) != 1 || names[0] != "carol" {
		t.Fatalf("names = %v, want [carol]", names)
	}
}
