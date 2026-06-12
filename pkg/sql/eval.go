package sql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

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

// resolveColumn returns the value for a reference operand (column, aggregate,
// scalar subquery, or computed expression). The boolean reports whether expr
// was such a reference (vs. a literal). Column and aggregate references resolve
// against the row, falling back to the correlated outer row; a scalar subquery
// is executed; arithmetic, function, and CASE expressions are evaluated.
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
	case *ArithExpr, *FuncCall, *CaseExpr:
		v, err := evalValue(expr, row, ec)
		return v, true, err
	default:
		return nil, false, nil
	}
}

// evalValue evaluates a value expression (column, literal, arithmetic,
// function call, CASE, aggregate reference, or scalar subquery) to a typed
// value against the given row.
func evalValue(expr Expr, row Row, ec *evalContext) (types.Comparable, error) {
	switch e := expr.(type) {
	case *Literal:
		return naturalLiteral(e), nil
	case *ArithExpr:
		return evalArith(e, row, ec)
	case *FuncCall:
		return evalFunc(e, row, ec)
	case *CaseExpr:
		return evalCase(e, row, ec)
	default:
		v, isRef, err := resolveColumn(expr, row, ec)
		if err != nil {
			return nil, err
		}
		if !isRef {
			return nil, fmt.Errorf("%w: expression %s cannot be used as a value", ErrEval, expr.String())
		}
		return v, nil
	}
}

// evalArith evaluates an arithmetic expression. A NULL operand yields NULL.
// Two integer operands keep integer semantics (truncating division); any float
// operand promotes the computation to float.
func evalArith(e *ArithExpr, row Row, ec *evalContext) (types.Comparable, error) {
	lv, err := evalValue(e.Left, row, ec)
	if err != nil {
		return nil, err
	}
	rv, err := evalValue(e.Right, row, ec)
	if err != nil {
		return nil, err
	}
	if isNull(lv) || isNull(rv) {
		return types.NullKey{}, nil
	}
	li, lIsInt := lv.(types.IntKey)
	ri, rIsInt := rv.(types.IntKey)
	if lIsInt && rIsInt {
		return intArith(e.Op, int64(li), int64(ri))
	}
	lf, lOK := numericFloat(lv)
	rf, rOK := numericFloat(rv)
	if !lOK || !rOK {
		return nil, fmt.Errorf("%w: operator %q requires numeric operands, got %T and %T", ErrEval, e.Op, lv, rv)
	}
	return floatArith(e.Op, lf, rf)
}

func intArith(op string, a, b int64) (types.Comparable, error) {
	switch op {
	case "+":
		return types.IntKey(a + b), nil
	case "-":
		return types.IntKey(a - b), nil
	case "*":
		return types.IntKey(a * b), nil
	case "/":
		if b == 0 {
			return nil, fmt.Errorf("%w: division by zero", ErrEval)
		}
		return types.IntKey(a / b), nil
	case "%":
		if b == 0 {
			return nil, fmt.Errorf("%w: division by zero", ErrEval)
		}
		return types.IntKey(a % b), nil
	default:
		return nil, fmt.Errorf("%w: unsupported arithmetic operator %q", ErrEval, op)
	}
}

func floatArith(op string, a, b float64) (types.Comparable, error) {
	switch op {
	case "+":
		return types.FloatKey(a + b), nil
	case "-":
		return types.FloatKey(a - b), nil
	case "*":
		return types.FloatKey(a * b), nil
	case "/":
		if b == 0 {
			return nil, fmt.Errorf("%w: division by zero", ErrEval)
		}
		return types.FloatKey(a / b), nil
	case "%":
		if b == 0 {
			return nil, fmt.Errorf("%w: division by zero", ErrEval)
		}
		return types.FloatKey(math.Mod(a, b)), nil
	default:
		return nil, fmt.Errorf("%w: unsupported arithmetic operator %q", ErrEval, op)
	}
}

// evalFunc evaluates a scalar function call. Single-argument functions
// propagate NULL; COALESCE returns its first non-NULL argument.
func evalFunc(e *FuncCall, row Row, ec *evalContext) (types.Comparable, error) {
	if e.Name == "COALESCE" {
		if len(e.Args) == 0 {
			return nil, fmt.Errorf("%w: COALESCE requires at least one argument", ErrEval)
		}
		for _, arg := range e.Args {
			v, err := evalValue(arg, row, ec)
			if err != nil {
				return nil, err
			}
			if !isNull(v) {
				return v, nil
			}
		}
		return types.NullKey{}, nil
	}

	if len(e.Args) != 1 {
		return nil, fmt.Errorf("%w: %s expects exactly one argument, got %d", ErrEval, e.Name, len(e.Args))
	}
	v, err := evalValue(e.Args[0], row, ec)
	if err != nil {
		return nil, err
	}
	if isNull(v) {
		return types.NullKey{}, nil
	}
	switch e.Name {
	case "UPPER", "LOWER", "LENGTH":
		s, ok := v.(types.VarcharKey)
		if !ok {
			return nil, fmt.Errorf("%w: %s requires a text argument, got %T", ErrEval, e.Name, v)
		}
		switch e.Name {
		case "UPPER":
			return types.VarcharKey(strings.ToUpper(string(s))), nil
		case "LOWER":
			return types.VarcharKey(strings.ToLower(string(s))), nil
		default:
			return types.IntKey(int64(len([]rune(string(s))))), nil
		}
	case "ABS":
		switch n := v.(type) {
		case types.IntKey:
			if n < 0 {
				return -n, nil
			}
			return n, nil
		case types.FloatKey:
			return types.FloatKey(math.Abs(float64(n))), nil
		default:
			return nil, fmt.Errorf("%w: ABS requires a numeric argument, got %T", ErrEval, v)
		}
	default:
		return nil, fmt.Errorf("%w: unknown function %q", ErrEval, e.Name)
	}
}

// evalCase evaluates a searched CASE expression: the first WHEN whose condition
// holds yields its THEN value; otherwise the ELSE value, or NULL without one.
func evalCase(e *CaseExpr, row Row, ec *evalContext) (types.Comparable, error) {
	for _, w := range e.Whens {
		ok, err := evaluate(w.Cond, row, ec)
		if err != nil {
			return nil, err
		}
		if ok {
			return evalValue(w.Then, row, ec)
		}
	}
	if e.Else != nil {
		return evalValue(e.Else, row, ec)
	}
	return types.NullKey{}, nil
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
	case types.DateOnlyKey:
		switch lit.Kind {
		case LitDate:
			return lit.Date, nil
		case LitString:
			k, err := types.ParseDateOnly(lit.Str)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrEval, err)
			}
			return k, nil
		}
	case types.DecimalKey:
		switch lit.Kind {
		case LitDecimal:
			return lit.Dec, nil
		case LitString:
			k, err := types.ParseDecimal(lit.Str)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrEval, err)
			}
			return k, nil
		case LitInt:
			return types.NewDecimal(lit.Int, 0), nil
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
	case LitDate:
		return lit.Date
	case LitDecimal:
		return lit.Dec
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
