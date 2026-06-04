package sql

import (
	"errors"
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// ErrPlan is the sentinel wrapped by all planning errors.
var ErrPlan = errors.New("sql: plan error")

// QueryPlan is the physical access path chosen for a SELECT. The executor
// scans IndexName between the inclusive bounds [Lower, Upper] (nil means
// unbounded), evaluates Residual against every decoded row, and sorts the
// result when NeedsSort is set.
type QueryPlan struct {
	TableName string
	IndexName string
	Column    string // column served by IndexName
	Lower     types.Comparable
	Upper     types.Comparable
	Residual  Expr     // full WHERE expression, or nil
	NeedsSort bool     // executor must sort rows in memory
	Sort      *OrderBy // ordering to apply when NeedsSort
}

// Plan chooses an access path for a SELECT over the given schema. Bounds are a
// conservative narrowing of the index scan; correctness is always guaranteed
// by re-evaluating the full WHERE expression (Residual) per row.
func Plan(stmt *SelectStmt, schema *TableSchema) (*QueryPlan, error) {
	if err := validateColumns(stmt, schema); err != nil {
		return nil, err
	}

	plan := &QueryPlan{TableName: schema.Name, Residual: stmt.Where}

	conjuncts := topLevelConjuncts(stmt.Where)
	chosen, ok := chooseIndex(conjuncts, stmt.OrderBy, schema)
	if !ok {
		pk, hasPK := schema.PrimaryIndex()
		if !hasPK {
			return nil, fmt.Errorf("%w: table %q has no primary index", ErrPlan, schema.Name)
		}
		chosen = pk
	}
	plan.IndexName = chosen.Name
	plan.Column = chosen.Column

	col, _ := schema.Column(chosen.Column)
	plan.Lower, plan.Upper = deriveBounds(conjuncts, chosen.Column, col.Type)

	if stmt.OrderBy != nil {
		orderedByScan := stmt.OrderBy.Column == chosen.Column && !stmt.OrderBy.Desc
		if !orderedByScan {
			plan.NeedsSort = true
			plan.Sort = stmt.OrderBy
		}
	}
	return plan, nil
}

// chooseIndex selects an index by priority: an equality predicate's index, then
// a range predicate's index, then an index that satisfies the ORDER BY column.
func chooseIndex(conjuncts []Expr, orderBy *OrderBy, schema *TableSchema) (IndexDef, bool) {
	var rangeIdx IndexDef
	haveRange := false
	for _, c := range conjuncts {
		col, op, _, ok := simpleComparison(c)
		if !ok {
			continue
		}
		idx, hasIdx := schema.IndexForColumn(col)
		if !hasIdx {
			continue
		}
		if op == "=" {
			return idx, true
		}
		if isRangeOp(op) && !haveRange {
			rangeIdx, haveRange = idx, true
		}
	}
	if haveRange {
		return rangeIdx, true
	}
	if orderBy != nil {
		if idx, ok := schema.IndexForColumn(orderBy.Column); ok {
			return idx, true
		}
	}
	return IndexDef{}, false
}

// deriveBounds computes inclusive scan bounds from the simple comparisons on
// the chosen column. Exclusive operators (< >) use an inclusive bound; the
// residual predicate removes the boundary row.
func deriveBounds(conjuncts []Expr, column string, dt storage.DataType) (types.Comparable, types.Comparable) {
	var lower, upper types.Comparable
	for _, c := range conjuncts {
		col, op, lit, ok := simpleComparison(c)
		if !ok || col != column {
			continue
		}
		val, err := ColumnValue(lit, dt)
		if err != nil {
			// Not convertible to this column's key type: skip the bound. The
			// residual predicate still enforces correctness.
			continue
		}
		if isNull(val) {
			continue
		}
		switch op {
		case "=":
			lower = tighterLower(lower, val)
			upper = tighterUpper(upper, val)
		case ">", ">=":
			lower = tighterLower(lower, val)
		case "<", "<=":
			upper = tighterUpper(upper, val)
		}
	}
	return lower, upper
}

func tighterLower(current, candidate types.Comparable) types.Comparable {
	if current == nil {
		return candidate
	}
	if cmp, err := candidate.Compare(current); err == nil && cmp > 0 {
		return candidate
	}
	return current
}

func tighterUpper(current, candidate types.Comparable) types.Comparable {
	if current == nil {
		return candidate
	}
	if cmp, err := candidate.Compare(current); err == nil && cmp < 0 {
		return candidate
	}
	return current
}

// topLevelConjuncts flattens the top-level AND spine of expr into its
// conjuncts. A top-level OR (or a nil expression) yields no usable conjuncts
// for bound derivation, but a single conjunct list is still returned so simple
// comparisons can be inspected.
func topLevelConjuncts(expr Expr) []Expr {
	if expr == nil {
		return nil
	}
	be, ok := expr.(*BinaryExpr)
	if ok && be.Op == "AND" {
		return append(topLevelConjuncts(be.Left), topLevelConjuncts(be.Right)...)
	}
	return []Expr{expr}
}

// simpleComparison reports whether expr is "column OP literal" or
// "literal OP column", normalizing to (column, op, literal). The operator is
// flipped when the column is on the right so the bound applies to the column.
func simpleComparison(expr Expr) (column string, op string, lit *Literal, ok bool) {
	be, isBin := expr.(*BinaryExpr)
	if !isBin || !isComparisonOp(be.Op) {
		return "", "", nil, false
	}
	if col, isCol := be.Left.(*ColumnRef); isCol {
		if l, isLit := be.Right.(*Literal); isLit {
			return col.Name, be.Op, l, true
		}
	}
	if col, isCol := be.Right.(*ColumnRef); isCol {
		if l, isLit := be.Left.(*Literal); isLit {
			return col.Name, flipOp(be.Op), l, true
		}
	}
	return "", "", nil, false
}

func flipOp(op string) string {
	switch op {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	default: // = <> != are symmetric
		return op
	}
}

func isComparisonOp(op string) bool {
	switch op {
	case "=", "<>", "!=", "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
}

func isRangeOp(op string) bool {
	switch op {
	case "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
}

// validateColumns ensures every column referenced by the statement exists in
// the schema.
func validateColumns(stmt *SelectStmt, schema *TableSchema) error {
	if !stmt.Star {
		for _, c := range stmt.Columns {
			if _, ok := schema.Column(c); !ok {
				return fmt.Errorf("%w: unknown column %q in projection", ErrPlan, c)
			}
		}
	}
	if stmt.OrderBy != nil {
		if _, ok := schema.Column(stmt.OrderBy.Column); !ok {
			return fmt.Errorf("%w: unknown column %q in ORDER BY", ErrPlan, stmt.OrderBy.Column)
		}
	}
	return validateExprColumns(stmt.Where, schema)
}

func validateExprColumns(expr Expr, schema *TableSchema) error {
	switch e := expr.(type) {
	case nil:
		return nil
	case *ColumnRef:
		if _, ok := schema.Column(e.Name); !ok {
			return fmt.Errorf("%w: unknown column %q in WHERE", ErrPlan, e.Name)
		}
	case *IsNullExpr:
		return validateExprColumns(e.Operand, schema)
	case *BinaryExpr:
		if err := validateExprColumns(e.Left, schema); err != nil {
			return err
		}
		return validateExprColumns(e.Right, schema)
	}
	return nil
}
