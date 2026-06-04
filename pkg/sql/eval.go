package sql

import (
	"errors"
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// ErrEval is the sentinel wrapped by all predicate-evaluation errors.
var ErrEval = errors.New("sql: eval error")

// Row is a decoded row: a mapping from column name to its typed value.
type Row = map[string]types.Comparable

// Evaluate evaluates a boolean WHERE expression against a row. Comparisons
// involving NULL (on either side) yield false, mirroring SQL's three-valued
// logic where UNKNOWN is filtered out. It returns an error wrapping ErrEval
// for unknown columns, incompatible operand types, or a non-boolean
// expression.
func Evaluate(expr Expr, row Row) (bool, error) {
	if isNullExpr, ok := expr.(*IsNullExpr); ok {
		return evalIsNull(isNullExpr, row)
	}
	be, ok := expr.(*BinaryExpr)
	if !ok {
		return false, fmt.Errorf("%w: expression %s is not a boolean predicate", ErrEval, expr.String())
	}
	switch be.Op {
	case "AND", "OR":
		left, err := Evaluate(be.Left, row)
		if err != nil {
			return false, err
		}
		right, err := Evaluate(be.Right, row)
		if err != nil {
			return false, err
		}
		if be.Op == "AND" {
			return left && right, nil
		}
		return left || right, nil
	default:
		return evalComparison(be, row)
	}
}

func evalIsNull(e *IsNullExpr, row Row) (bool, error) {
	val, isCol, err := resolveColumn(e.Operand, row)
	if err != nil {
		return false, err
	}
	if !isCol {
		if val, err = resolveLiteral(e.Operand, nil); err != nil {
			return false, err
		}
	}
	null := isNull(val)
	if e.Negate {
		return !null, nil
	}
	return null, nil
}

func evalComparison(be *BinaryExpr, row Row) (bool, error) {
	lv, lIsCol, err := resolveColumn(be.Left, row)
	if err != nil {
		return false, err
	}
	rv, rIsCol, err := resolveColumn(be.Right, row)
	if err != nil {
		return false, err
	}

	if !lIsCol {
		var hint types.Comparable
		if rIsCol {
			hint = rv
		}
		if lv, err = resolveLiteral(be.Left, hint); err != nil {
			return false, err
		}
	}
	if !rIsCol {
		var hint types.Comparable
		if lIsCol {
			hint = lv
		}
		if rv, err = resolveLiteral(be.Right, hint); err != nil {
			return false, err
		}
	}

	// SQL three-valued logic: any comparison with NULL is UNKNOWN -> false.
	if isNull(lv) || isNull(rv) {
		return false, nil
	}

	cmp, err := lv.Compare(rv)
	if err != nil {
		return false, fmt.Errorf("%w: cannot compare %s %s %s: %v", ErrEval, be.Left.String(), be.Op, be.Right.String(), err)
	}
	switch be.Op {
	case "=":
		return cmp == 0, nil
	case "<>", "!=":
		return cmp != 0, nil
	case "<":
		return cmp < 0, nil
	case "<=":
		return cmp <= 0, nil
	case ">":
		return cmp > 0, nil
	case ">=":
		return cmp >= 0, nil
	default:
		return false, fmt.Errorf("%w: unsupported operator %q", ErrEval, be.Op)
	}
}

// resolveColumn returns the row value for a column operand. The boolean
// reports whether expr was a column reference.
func resolveColumn(expr Expr, row Row) (types.Comparable, bool, error) {
	col, ok := expr.(*ColumnRef)
	if !ok {
		return nil, false, nil
	}
	v, ok := row[col.Name]
	if !ok {
		return nil, true, fmt.Errorf("%w: unknown column %q", ErrEval, col.Name)
	}
	return v, true, nil
}

// resolveLiteral converts a literal operand to a Comparable, coercing it to the
// concrete type of hint when one is provided.
func resolveLiteral(expr Expr, hint types.Comparable) (types.Comparable, error) {
	lit, ok := expr.(*Literal)
	if !ok {
		return nil, fmt.Errorf("%w: expected a literal operand, got %s", ErrEval, expr.String())
	}
	return LiteralValue(lit, hint)
}

// LiteralValue converts a parsed literal into a types.Comparable. When hint is
// a non-NULL Comparable, the literal is coerced to its concrete type so the two
// can be compared; otherwise the literal's natural type is used.
func LiteralValue(lit *Literal, hint types.Comparable) (types.Comparable, error) {
	if lit.Kind == LitNull {
		return types.NullKey{}, nil
	}
	if hint == nil || isNull(hint) {
		return naturalLiteral(lit), nil
	}
	switch hint.(type) {
	case types.IntKey:
		if lit.Kind == LitInt {
			return types.IntKey(lit.Int), nil
		}
	case types.FloatKey:
		switch lit.Kind {
		case LitFloat:
			return types.FloatKey(lit.Float), nil
		case LitInt:
			return types.FloatKey(float64(lit.Int)), nil
		}
	case types.VarcharKey:
		if lit.Kind == LitString {
			return types.VarcharKey(lit.Str), nil
		}
	case types.BoolKey:
		if lit.Kind == LitBool {
			return types.BoolKey(lit.Bool), nil
		}
	}
	return nil, fmt.Errorf("%w: literal %s is not compatible with %T", ErrEval, lit.String(), hint)
}

func naturalLiteral(lit *Literal) types.Comparable {
	switch lit.Kind {
	case LitInt:
		return types.IntKey(lit.Int)
	case LitFloat:
		return types.FloatKey(lit.Float)
	case LitString:
		return types.VarcharKey(lit.Str)
	case LitBool:
		return types.BoolKey(lit.Bool)
	default:
		return types.NullKey{}
	}
}

func isNull(v types.Comparable) bool {
	_, ok := v.(types.NullKey)
	return ok
}
