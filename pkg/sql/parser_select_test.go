package sql

import (
	"errors"
	"testing"
)

func parseSelect(t *testing.T, input string) *SelectStmt {
	t.Helper()
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse(%q) error: %v", input, err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Parse(%q) returned %T, want *SelectStmt", input, stmt)
	}
	return sel
}

func TestParseSelectColumns(t *testing.T) {
	sel := parseSelect(t, "SELECT id, name FROM users")
	if len(sel.Items) != 2 {
		t.Fatalf("items = %d, want 2", len(sel.Items))
	}
	if sel.Items[0].Column == nil || sel.Items[0].Column.Name != "id" {
		t.Fatalf("item[0] = %+v, want column id", sel.Items[0])
	}
	if sel.Items[1].Column == nil || sel.Items[1].Column.Name != "name" {
		t.Fatalf("item[1] = %+v, want column name", sel.Items[1])
	}
	if sel.Table != "users" {
		t.Fatalf("Table = %q, want users", sel.Table)
	}
	if sel.Where != nil {
		t.Fatalf("Where = %v, want nil", sel.Where)
	}
}

func TestParseSelectStar(t *testing.T) {
	sel := parseSelect(t, "SELECT * FROM users")
	if len(sel.Items) != 1 || !sel.Items[0].Star {
		t.Fatalf("items = %+v, want a single star item", sel.Items)
	}
}

func TestParseSelectWhereComparison(t *testing.T) {
	sel := parseSelect(t, "SELECT * FROM users WHERE age >= 18")
	if sel.Where == nil {
		t.Fatal("Where = nil, want comparison")
	}
	if got := sel.Where.String(); got != "(age >= 18)" {
		t.Fatalf("Where = %q, want (age >= 18)", got)
	}
}

func TestParseSelectWherePrecedence(t *testing.T) {
	sel := parseSelect(t, "SELECT * FROM users WHERE age >= 18 OR age < 5 AND name = 'bob'")
	// AND binds tighter than OR.
	want := "((age >= 18) OR ((age < 5) AND (name = 'bob')))"
	if got := sel.Where.String(); got != want {
		t.Fatalf("Where = %q, want %q", got, want)
	}
}

func TestParseSelectWhereParens(t *testing.T) {
	sel := parseSelect(t, "SELECT * FROM users WHERE (age >= 18 OR age < 5) AND active = true")
	want := "(((age >= 18) OR (age < 5)) AND (active = true))"
	if got := sel.Where.String(); got != want {
		t.Fatalf("Where = %q, want %q", got, want)
	}
}

func TestParseSelectLiteralKinds(t *testing.T) {
	sel := parseSelect(t, "SELECT * FROM t WHERE a = 1 AND b = 3.5 AND c = 'x' AND d = false AND e <> NULL")
	want := "(((((a = 1) AND (b = 3.500000)) AND (c = 'x')) AND (d = false)) AND (e <> NULL))"
	if got := sel.Where.String(); got != want {
		t.Fatalf("Where = %q, want %q", got, want)
	}
}

func TestParseSelectOrderLimitOffset(t *testing.T) {
	sel := parseSelect(t, "SELECT * FROM users ORDER BY age DESC LIMIT 10 OFFSET 5")
	if sel.OrderBy == nil || sel.OrderBy.Column != "age" || !sel.OrderBy.Desc {
		t.Fatalf("OrderBy = %+v, want age DESC", sel.OrderBy)
	}
	if sel.Limit == nil || *sel.Limit != 10 {
		t.Fatalf("Limit = %v, want 10", sel.Limit)
	}
	if sel.Offset == nil || *sel.Offset != 5 {
		t.Fatalf("Offset = %v, want 5", sel.Offset)
	}
}

func TestParseSelectOrderByDefaultAsc(t *testing.T) {
	sel := parseSelect(t, "SELECT * FROM users ORDER BY name")
	if sel.OrderBy == nil || sel.OrderBy.Column != "name" || sel.OrderBy.Desc {
		t.Fatalf("OrderBy = %+v, want name ASC", sel.OrderBy)
	}
}

func TestParseSelectErrors(t *testing.T) {
	inputs := []string{
		"SELECT",                        // missing columns
		"SELECT id",                     // missing FROM
		"SELECT id FROM",                // missing table
		"SELECT * FROM users WHERE",     // missing predicate
		"SELECT * FROM users garbage",   // trailing junk
		"SELECT * FROM users LIMIT abc", // non-numeric limit
		"FROM users",                    // unsupported leading keyword
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			if _, err := Parse(in); !errors.Is(err, ErrParse) {
				t.Fatalf("Parse(%q) error = %v, want ErrParse", in, err)
			}
		})
	}
}
