package sql

import (
	"context"
	"errors"
	"testing"
)

func TestParseUnion(t *testing.T) {
	stmt, err := Parse("SELECT id FROM users UNION SELECT id FROM admins")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	so, ok := stmt.(*SetOpStmt)
	if !ok {
		t.Fatalf("Parse returned %T, want *SetOpStmt", stmt)
	}
	if so.All {
		t.Fatalf("All = true, want false for UNION")
	}
	if _, ok := so.Left.(*SelectStmt); !ok {
		t.Fatalf("Left = %T, want *SelectStmt", so.Left)
	}
	if so.Right == nil || so.Right.Table != "admins" {
		t.Fatalf("Right = %+v, want select from admins", so.Right)
	}
}

func TestParseUnionAll(t *testing.T) {
	stmt, err := Parse("SELECT id FROM users UNION ALL SELECT id FROM admins")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	so, ok := stmt.(*SetOpStmt)
	if !ok {
		t.Fatalf("Parse returned %T, want *SetOpStmt", stmt)
	}
	if !so.All {
		t.Fatalf("All = false, want true for UNION ALL")
	}
}

func TestParseUnionChain(t *testing.T) {
	// Three-way UNION is left-associative: ((a UNION b) UNION c).
	stmt, err := Parse("SELECT id FROM a UNION SELECT id FROM b UNION SELECT id FROM c")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	so, ok := stmt.(*SetOpStmt)
	if !ok {
		t.Fatalf("Parse returned %T, want *SetOpStmt", stmt)
	}
	if so.Right.Table != "c" {
		t.Fatalf("outer Right table = %q, want c", so.Right.Table)
	}
	inner, ok := so.Left.(*SetOpStmt)
	if !ok {
		t.Fatalf("inner Left = %T, want *SetOpStmt", so.Left)
	}
	if inner.Right.Table != "b" {
		t.Fatalf("inner Right table = %q, want b", inner.Right.Table)
	}
}

func TestQueryUnionDeduplicates(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT id FROM users WHERE id = 1 UNION SELECT id FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %d, want 1 (deduplicated)", len(rs.Rows))
	}
	if got := intColumn(t, rs, "id"); got[0] != 1 {
		t.Fatalf("id = %d, want 1", got[0])
	}
}

func TestQueryUnionAllKeepsDuplicates(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT id FROM users WHERE id = 1 UNION ALL SELECT id FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("rows = %d, want 2 (UNION ALL keeps duplicates)", len(rs.Rows))
	}
}

func TestQueryUnionCombinesDistinctRows(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT id FROM users WHERE id = 1 UNION SELECT id FROM users WHERE id = 2")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rs.Rows))
	}
}

func TestQueryUnionColumnMismatch(t *testing.T) {
	e := newExecutor(t)
	_, err := e.Query(context.Background(), "SELECT id FROM users UNION SELECT id, name FROM users")
	if !errors.Is(err, ErrExec) {
		t.Fatalf("err = %v, want ErrExec for column count mismatch", err)
	}
}
