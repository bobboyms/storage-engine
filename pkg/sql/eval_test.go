package sql

import (
	"errors"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func mustWhere(t *testing.T, query string) Expr {
	t.Helper()
	sel := parseSelect(t, query)
	if sel.Where == nil {
		t.Fatalf("query %q has no WHERE", query)
	}
	return sel.Where
}

func evalQuery(t *testing.T, query string, row map[string]types.Comparable) bool {
	t.Helper()
	ok, err := Evaluate(mustWhere(t, "SELECT * FROM t WHERE "+query), row)
	if err != nil {
		t.Fatalf("Evaluate(%q) error: %v", query, err)
	}
	return ok
}

func TestEvaluateIntComparison(t *testing.T) {
	row := map[string]types.Comparable{"age": types.IntKey(20)}
	if !evalQuery(t, "age >= 18", row) {
		t.Fatal("age >= 18 should be true for age=20")
	}
	if evalQuery(t, "age < 18", row) {
		t.Fatal("age < 18 should be false for age=20")
	}
	if !evalQuery(t, "age = 20", row) {
		t.Fatal("age = 20 should be true")
	}
	if !evalQuery(t, "age <> 21", row) {
		t.Fatal("age <> 21 should be true")
	}
}

func TestEvaluateStringAndBool(t *testing.T) {
	row := map[string]types.Comparable{
		"name":   types.VarcharKey("bob"),
		"active": types.BoolKey(true),
	}
	if !evalQuery(t, "name = 'bob'", row) {
		t.Fatal("name = 'bob' should be true")
	}
	if evalQuery(t, "name = 'al'", row) {
		t.Fatal("name = 'al' should be false")
	}
	if !evalQuery(t, "active = true", row) {
		t.Fatal("active = true should be true")
	}
}

func TestEvaluateNumericPromotion(t *testing.T) {
	row := map[string]types.Comparable{"price": types.FloatKey(9.5)}
	if !evalQuery(t, "price > 9", row) {
		t.Fatal("price > 9 should be true for float column compared to int literal")
	}
	if !evalQuery(t, "price < 9.6", row) {
		t.Fatal("price < 9.6 should be true")
	}
}

func TestEvaluateLiteralOnLeft(t *testing.T) {
	row := map[string]types.Comparable{"age": types.IntKey(20)}
	if !evalQuery(t, "18 <= age", row) {
		t.Fatal("18 <= age should be true for age=20")
	}
}

func TestEvaluateAndOr(t *testing.T) {
	row := map[string]types.Comparable{
		"age":  types.IntKey(20),
		"name": types.VarcharKey("bob"),
	}
	if !evalQuery(t, "age >= 18 AND name = 'bob'", row) {
		t.Fatal("AND of two true predicates should be true")
	}
	if evalQuery(t, "age >= 18 AND name = 'al'", row) {
		t.Fatal("AND with one false predicate should be false")
	}
	if !evalQuery(t, "age > 99 OR name = 'bob'", row) {
		t.Fatal("OR with one true predicate should be true")
	}
	if evalQuery(t, "age > 99 OR name = 'al'", row) {
		t.Fatal("OR of two false predicates should be false")
	}
}

func TestEvaluateNullSemantics(t *testing.T) {
	row := map[string]types.Comparable{"x": types.NullKey{}}
	// Any comparison involving NULL is unknown -> filtered out (false).
	if evalQuery(t, "x = 1", row) {
		t.Fatal("NULL = 1 should be false")
	}
	if evalQuery(t, "x <> 1", row) {
		t.Fatal("NULL <> 1 should be false")
	}

	rowVal := map[string]types.Comparable{"x": types.IntKey(1)}
	if evalQuery(t, "x = NULL", rowVal) {
		t.Fatal("x = NULL should be false")
	}
}

func TestEvaluateErrors(t *testing.T) {
	row := map[string]types.Comparable{"age": types.IntKey(20)}

	// Unknown column.
	if _, err := Evaluate(mustWhere(t, "SELECT * FROM t WHERE missing = 1"), row); !errors.Is(err, ErrEval) {
		t.Fatalf("unknown column error = %v, want ErrEval", err)
	}

	// Type mismatch: string literal vs int column.
	if _, err := Evaluate(mustWhere(t, "SELECT * FROM t WHERE age = 'x'"), row); !errors.Is(err, ErrEval) {
		t.Fatalf("type mismatch error = %v, want ErrEval", err)
	}

	// Non-boolean expression at the top level.
	if _, err := Evaluate(&ColumnRef{Name: "age"}, row); !errors.Is(err, ErrEval) {
		t.Fatalf("non-boolean expr error = %v, want ErrEval", err)
	}
}
