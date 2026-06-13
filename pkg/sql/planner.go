package sql

import (
	"errors"
	"fmt"
	"strings"

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

	if chosen.composite() {
		// A composite index is an exact-match index on the encoded column tuple;
		// the bounds are the single encoded key (Residual still re-checks rows).
		if key, ok := compositeEqualityKey(conjuncts, chosen, schema); ok {
			plan.Lower, plan.Upper = key, key
		}
	} else {
		plan.Column = chosen.Column
		col, _ := schema.Column(chosen.Column)
		plan.Lower, plan.Upper = deriveBounds(conjuncts, chosen.Column, col.Type)
	}

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
	// Prefer a composite index whose every column is pinned by an equality
	// predicate: it turns "a = .. AND b = .." from a partial scan into an
	// exact-match index lookup.
	for _, idx := range schema.Indexes {
		if idx.composite() && compositeFullyConstrained(conjuncts, idx) {
			return idx, true
		}
	}

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

// compositeFullyConstrained reports whether every column of a composite index
// has an equality predicate among the conjuncts (the precondition for an
// exact-match composite lookup).
func compositeFullyConstrained(conjuncts []Expr, idx IndexDef) bool {
	for _, name := range idx.Columns {
		if _, ok := equalityLiteral(conjuncts, name); !ok {
			return false
		}
	}
	return true
}

// compositeEqualityKey builds the encoded lookup key for a composite index from
// the equality literals on its columns. It returns false if any column is not
// pinned by an equality with a value convertible to the column's type.
func compositeEqualityKey(conjuncts []Expr, idx IndexDef, schema *TableSchema) (types.VarcharKey, bool) {
	parts := make([]types.Comparable, len(idx.Columns))
	for i, name := range idx.Columns {
		lit, ok := equalityLiteral(conjuncts, name)
		if !ok {
			return "", false
		}
		col, _ := schema.Column(name)
		val, err := ColumnValue(lit, col.Type)
		if err != nil {
			return "", false
		}
		parts[i] = val
	}
	return encodeCompositeKey(parts), true
}

// equalityLiteral returns the literal of an "column = literal" conjunct on the
// named column, if present.
func equalityLiteral(conjuncts []Expr, column string) (*Literal, bool) {
	for _, c := range conjuncts {
		if col, op, lit, ok := simpleComparison(c); ok && op == "=" && col == column {
			return lit, true
		}
	}
	return nil, false
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
	alias := stmt.Alias
	for _, item := range stmt.Items {
		switch {
		case item.Column != nil:
			if err := validateRef(item.Column, schema, alias); err != nil {
				return err
			}
		case item.Agg != nil && !item.Agg.Star:
			if err := validateRef(item.Agg.Column, schema, alias); err != nil {
				return err
			}
		case item.Expr != nil:
			if err := validateExprColumns(item.Expr, schema, alias); err != nil {
				return err
			}
		}
	}
	for _, g := range stmt.GroupBy {
		if err := validateQualifiedName(g, schema, alias); err != nil {
			return err
		}
	}
	// In a grouped query ORDER BY may reference an aggregate or alias rather
	// than a base column, so only validate ORDER BY for non-grouped queries.
	if stmt.OrderBy != nil && !isGrouped(stmt) {
		if err := validateQualifiedName(stmt.OrderBy.Column, schema, alias); err != nil {
			return err
		}
	}
	if err := validateExprColumns(stmt.Having, schema, alias); err != nil {
		return err
	}
	return validateExprColumns(stmt.Where, schema, alias)
}

// validateRef checks a column reference: its qualifier (if any) must match the
// table alias, and its column must exist in the schema.
func validateRef(ref *ColumnRef, schema *TableSchema, alias string) error {
	if ref.Qualifier != "" && ref.Qualifier != alias {
		return fmt.Errorf("%w: unknown table qualifier %q", ErrPlan, ref.Qualifier)
	}
	if _, ok := schema.Column(ref.Name); !ok {
		return fmt.Errorf("%w: unknown column %q", ErrPlan, ref.Name)
	}
	return nil
}

// validateQualifiedName validates a column reference stored in textual form
// ("col" or "alias.col"), as used by GROUP BY and ORDER BY.
func validateQualifiedName(s string, schema *TableSchema, alias string) error {
	qualifier, name := "", s
	if i := strings.IndexByte(s, '.'); i >= 0 {
		qualifier, name = s[:i], s[i+1:]
	}
	return validateRef(&ColumnRef{Qualifier: qualifier, Name: name}, schema, alias)
}

func validateExprColumns(expr Expr, schema *TableSchema, alias string) error {
	switch e := expr.(type) {
	case nil:
		return nil
	case *ColumnRef:
		return validateRef(e, schema, alias)
	case *AggregateExpr:
		if !e.Call.Star {
			return validateRef(e.Call.Column, schema, alias)
		}
	case *IsNullExpr:
		return validateExprColumns(e.Operand, schema, alias)
	case *BinaryExpr:
		if err := validateExprColumns(e.Left, schema, alias); err != nil {
			return err
		}
		return validateExprColumns(e.Right, schema, alias)
	case *ArithExpr:
		if err := validateExprColumns(e.Left, schema, alias); err != nil {
			return err
		}
		return validateExprColumns(e.Right, schema, alias)
	case *FuncCall:
		for _, arg := range e.Args {
			if err := validateExprColumns(arg, schema, alias); err != nil {
				return err
			}
		}
	case *CaseExpr:
		for _, w := range e.Whens {
			if err := validateExprColumns(w.Cond, schema, alias); err != nil {
				return err
			}
			if err := validateExprColumns(w.Then, schema, alias); err != nil {
				return err
			}
		}
		return validateExprColumns(e.Else, schema, alias)
	}
	return nil
}
