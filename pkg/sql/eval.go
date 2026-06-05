package sql

import (
	"context"
	"errors"
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// ErrEval is the sentinel wrapped by all predicate-evaluation errors.
var ErrEval = errors.New("sql: eval error")

// Row is a decoded row: a mapping from column name to its typed value.
type Row = map[string]types.Comparable

// evalContext carries what a predicate needs to evaluate subqueries and
// correlated references: the executor and request context to run a subquery,
// and the merged outer row whose columns an inner query may reference. It is
// nil where subqueries are not supported (e.g. DML and transactional reads).
type evalContext struct {
	exec  *Executor
	ctx   context.Context
	outer Row
}

// Evaluate evaluates a boolean WHERE expression against a row without subquery
// support. Comparisons involving NULL yield false (SQL three-valued logic).
func Evaluate(expr Expr, row Row) (bool, error) {
	return evaluate(expr, row, nil)
}

func evaluate(expr Expr, row Row, ec *evalContext) (bool, error) {
	switch e := expr.(type) {
	case *IsNullExpr:
		return evalIsNull(e, row, ec)
	case *ExistsExpr:
		return evalExists(e, row, ec)
	case *InSubqueryExpr:
		return evalInSubquery(e, row, ec)
	case *BinaryExpr:
		switch e.Op {
		case "AND", "OR":
			left, err := evaluate(e.Left, row, ec)
			if err != nil {
				return false, err
			}
			right, err := evaluate(e.Right, row, ec)
			if err != nil {
				return false, err
			}
			if e.Op == "AND" {
				return left && right, nil
			}
			return left || right, nil
		case "LIKE", "NOT LIKE":
			return evalLike(e, row, ec)
		default:
			return evalComparison(e, row, ec)
		}
	default:
		return false, fmt.Errorf("%w: expression %s is not a boolean predicate", ErrEval, expr.String())
	}
}

func evalIsNull(e *IsNullExpr, row Row, ec *evalContext) (bool, error) {
	val, isRef, err := resolveColumn(e.Operand, row, ec)
	if err != nil {
		return false, err
	}
	if !isRef {
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

// resolveOperands resolves both sides of a binary predicate to typed values,
// coercing a literal operand to the concrete type of the reference operand.
func resolveOperands(be *BinaryExpr, row Row, ec *evalContext) (lv, rv types.Comparable, err error) {
	var lIsRef, rIsRef bool
	lv, lIsRef, err = resolveColumn(be.Left, row, ec)
	if err != nil {
		return nil, nil, err
	}
	rv, rIsRef, err = resolveColumn(be.Right, row, ec)
	if err != nil {
		return nil, nil, err
	}

	if !lIsRef {
		var hint types.Comparable
		if rIsRef {
			hint = rv
		}
		if lv, err = resolveLiteral(be.Left, hint); err != nil {
			return nil, nil, err
		}
	}
	if !rIsRef {
		var hint types.Comparable
		if lIsRef {
			hint = lv
		}
		if rv, err = resolveLiteral(be.Right, hint); err != nil {
			return nil, nil, err
		}
	}
	return lv, rv, nil
}

func evalComparison(be *BinaryExpr, row Row, ec *evalContext) (bool, error) {
	lv, rv, err := resolveOperands(be, row, ec)
	if err != nil {
		return false, err
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

// resolveColumn returns the value for a reference operand (column, aggregate, or
// scalar subquery). The boolean reports whether expr was such a reference (vs. a
// literal). Column and aggregate references resolve against the row, falling
// back to the correlated outer row; a scalar subquery is executed.
func resolveColumn(expr Expr, row Row, ec *evalContext) (types.Comparable, bool, error) {
	switch e := expr.(type) {
	case *ColumnRef:
		v, ok := lookupRow(row, ec, e.String())
		if !ok {
			return nil, true, fmt.Errorf("%w: unknown column %q", ErrEval, e.String())
		}
		return v, true, nil
	case *AggregateExpr:
		v, ok := lookupRow(row, ec, e.Call.canonicalName())
		if !ok {
			return nil, true, fmt.Errorf("%w: aggregate %q not available here", ErrEval, e.Call.canonicalName())
		}
		return v, true, nil
	case *ScalarSubquery:
		v, err := runScalarSubquery(e, row, ec)
		return v, true, err
	default:
		return nil, false, nil
	}
}

// lookupRow resolves a key in the current row, then in the correlated outer row.
func lookupRow(row Row, ec *evalContext, key string) (types.Comparable, bool) {
	if v, ok := row[key]; ok {
		return v, true
	}
	if ec != nil && ec.outer != nil {
		if v, ok := ec.outer[key]; ok {
			return v, true
		}
	}
	return nil, false
}

// runSubquery executes a subquery correlated with the current row merged over
// any existing outer row.
func runSubquery(sub *SelectStmt, row Row, ec *evalContext) (*ResultSet, error) {
	if ec == nil || ec.exec == nil {
		return nil, fmt.Errorf("%w: subqueries are not supported in this context", ErrEval)
	}
	outer := mergeRows(ec.outer, row)
	return ec.exec.execSelect(ec.ctx, sub, outer)
}

func runScalarSubquery(e *ScalarSubquery, row Row, ec *evalContext) (types.Comparable, error) {
	rs, err := runSubquery(e.Select, row, ec)
	if err != nil {
		return nil, err
	}
	if len(rs.Columns) != 1 {
		return nil, fmt.Errorf("%w: scalar subquery must return exactly one column", ErrEval)
	}
	switch len(rs.Rows) {
	case 0:
		return types.NullKey{}, nil
	case 1:
		return rs.Rows[0][0], nil
	default:
		return nil, fmt.Errorf("%w: scalar subquery returned %d rows, expected at most one", ErrEval, len(rs.Rows))
	}
}

func evalExists(e *ExistsExpr, row Row, ec *evalContext) (bool, error) {
	rs, err := runSubquery(e.Select, row, ec)
	if err != nil {
		return false, err
	}
	exists := len(rs.Rows) > 0
	if e.Negate {
		return !exists, nil
	}
	return exists, nil
}

func evalInSubquery(e *InSubqueryExpr, row Row, ec *evalContext) (bool, error) {
	left, isRef, err := resolveColumn(e.Operand, row, ec)
	if err != nil {
		return false, err
	}
	if !isRef {
		if left, err = resolveLiteral(e.Operand, nil); err != nil {
			return false, err
		}
	}
	rs, err := runSubquery(e.Select, row, ec)
	if err != nil {
		return false, err
	}
	if len(rs.Columns) != 1 {
		return false, fmt.Errorf("%w: IN subquery must return exactly one column", ErrEval)
	}
	if isNull(left) {
		return false, nil
	}
	found := false
	for _, r := range rs.Rows {
		v := r[0]
		if isNull(v) {
			continue
		}
		cmp, err := left.Compare(v)
		if err != nil {
			return false, fmt.Errorf("%w: IN comparison failed: %v", ErrEval, err)
		}
		if cmp == 0 {
			found = true
			break
		}
	}
	if e.Negate {
		return !found, nil
	}
	return found, nil
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
	case types.UUIDKey:
		switch lit.Kind {
		case LitUUID:
			return lit.UUID, nil
		case LitString:
			k, err := types.ParseUUID(lit.Str)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrEval, err)
			}
			return k, nil
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
	case LitUUID:
		return lit.UUID
	default:
		return types.NullKey{}
	}
}

func isNull(v types.Comparable) bool {
	_, ok := v.(types.NullKey)
	return ok
}

// evalLike evaluates a LIKE / NOT LIKE predicate. Both operands must be text;
// a NULL on either side yields false (UNKNOWN). The pattern uses SQL wildcards:
// % matches any run of characters and _ matches exactly one.
func evalLike(be *BinaryExpr, row Row, ec *evalContext) (bool, error) {
	lv, rv, err := resolveOperands(be, row, ec)
	if err != nil {
		return false, err
	}
	if isNull(lv) || isNull(rv) {
		return false, nil
	}
	value, ok := lv.(types.VarcharKey)
	if !ok {
		return false, fmt.Errorf("%w: LIKE requires a text column, got %T", ErrEval, lv)
	}
	pattern, ok := rv.(types.VarcharKey)
	if !ok {
		return false, fmt.Errorf("%w: LIKE pattern must be text, got %T", ErrEval, rv)
	}
	matched := likeMatch(string(pattern), string(value))
	if be.Op == "NOT LIKE" {
		return !matched, nil
	}
	return matched, nil
}

// likeMatch reports whether s matches a SQL LIKE pattern, where % matches any
// sequence of characters (including none) and _ matches exactly one character.
// It uses linear-time backtracking over runes.
func likeMatch(pattern, s string) bool {
	pr := []rune(pattern)
	sr := []rune(s)
	var pi, si int
	star := -1
	mark := 0
	for si < len(sr) {
		switch {
		case pi < len(pr) && (pr[pi] == '_' || pr[pi] == sr[si]):
			pi++
			si++
		case pi < len(pr) && pr[pi] == '%':
			star = pi
			mark = si
			pi++
		case star != -1:
			pi = star + 1
			mark++
			si = mark
		default:
			return false
		}
	}
	for pi < len(pr) && pr[pi] == '%' {
		pi++
	}
	return pi == len(pr)
}
