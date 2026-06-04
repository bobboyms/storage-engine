package sql

import (
	"context"
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// binding associates a table alias with its schema for the duration of a query.
type binding struct {
	alias  string
	schema *TableSchema
}

// queryJoin executes a SELECT whose FROM has one or more JOINs. Every table is
// fully scanned and combined with a nested-loop join honoring each ON
// predicate; the WHERE clause, grouping, ordering, and projection then run over
// the joined rows. Rows carry qualified keys ("alias.col") plus bare keys for
// column names unique across all joined tables.
func (e *Executor) queryJoin(ctx context.Context, sel *SelectStmt) (*ResultSet, error) {
	bindings, err := e.joinBindings(sel)
	if err != nil {
		return nil, err
	}
	if err := validateJoinColumns(sel, bindings); err != nil {
		return nil, err
	}

	unique := uniqueColumns(bindings)

	rows, err := e.scanAll(ctx, bindings[0], unique)
	if err != nil {
		return nil, err
	}
	for i, jc := range sel.Joins {
		right, err := e.scanAll(ctx, bindings[i+1], unique)
		if err != nil {
			return nil, err
		}
		rows, err = nestedLoopJoin(rows, right, jc.On, jc.Left, nullRow(bindings[i+1], unique))
		if err != nil {
			return nil, err
		}
	}

	rows, err = filterRows(rows, sel.Where)
	if err != nil {
		return nil, err
	}

	if isGrouped(sel) {
		return groupedResultSet(sel, rows)
	}
	if sel.OrderBy != nil {
		sortRows(rows, sel.OrderBy)
	}
	rows = applyOffsetLimit(rows, sel.Offset, sel.Limit)
	return projectRows(rows, expandProjectionBindings(sel.Items, bindings)), nil
}

func (e *Executor) joinBindings(sel *SelectStmt) ([]binding, error) {
	bindings := make([]binding, 0, len(sel.Joins)+1)
	base, ok := e.catalog.Table(sel.Table)
	if !ok {
		return nil, fmt.Errorf("%w: unknown table %q", ErrExec, sel.Table)
	}
	bindings = append(bindings, binding{alias: sel.Alias, schema: base})
	seen := map[string]struct{}{sel.Alias: {}}
	for _, jc := range sel.Joins {
		schema, ok := e.catalog.Table(jc.Table)
		if !ok {
			return nil, fmt.Errorf("%w: unknown table %q", ErrExec, jc.Table)
		}
		if _, dup := seen[jc.Alias]; dup {
			return nil, fmt.Errorf("%w: duplicate table alias %q", ErrExec, jc.Alias)
		}
		seen[jc.Alias] = struct{}{}
		bindings = append(bindings, binding{alias: jc.Alias, schema: schema})
	}
	return bindings, nil
}

// uniqueColumns returns the set of column names that appear in exactly one
// binding, so they can be referenced without a qualifier.
func uniqueColumns(bindings []binding) map[string]bool {
	counts := make(map[string]int)
	for _, b := range bindings {
		for _, c := range b.schema.Columns {
			counts[c.Name]++
		}
	}
	unique := make(map[string]bool, len(counts))
	for name, n := range counts {
		if n == 1 {
			unique[name] = true
		}
	}
	return unique
}

// scanAll fully scans a table's primary index, decoding each row with qualified
// keys ("alias.col") and bare keys for columns unique across the join.
func (e *Executor) scanAll(ctx context.Context, b binding, unique map[string]bool) ([]Row, error) {
	pk, ok := b.schema.PrimaryIndex()
	if !ok {
		return nil, fmt.Errorf("%w: table %q has no primary index", ErrExec, b.schema.Name)
	}
	it, err := e.engine.NewIterator(ctx, b.schema.Name, pk.Name, storage.IterOptions{})
	if err != nil {
		return nil, fmt.Errorf("%w: open iterator: %v", ErrExec, err)
	}
	defer func() { _ = it.Close() }()

	var rows []Row
	for it.Next() {
		decoded, err := decodeRow(e.codec, b.schema, b.alias, it.Value())
		if err != nil {
			return nil, err
		}
		row := make(Row, len(b.schema.Columns)*2)
		for _, col := range b.schema.Columns {
			qualified := b.alias + "." + col.Name
			row[qualified] = decoded[qualified]
			if unique[col.Name] {
				row[col.Name] = decoded[qualified]
			}
		}
		rows = append(rows, row)
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("%w: scan: %v", ErrExec, err)
	}
	return rows, nil
}

// nestedLoopJoin combines left and right rows whose ON predicate holds. For a
// LEFT join, a left row with no matching right row is emitted once with the
// right side's columns set to NULL (via rightNull) so WHERE and projection see
// explicit NULLs rather than missing keys.
func nestedLoopJoin(left, right []Row, on Expr, leftJoin bool, rightNull Row) ([]Row, error) {
	var out []Row
	for _, l := range left {
		matched := false
		for _, r := range right {
			merged := mergeRows(l, r)
			ok, err := Evaluate(on, merged)
			if err != nil {
				return nil, err
			}
			if ok {
				out = append(out, merged)
				matched = true
			}
		}
		if leftJoin && !matched {
			out = append(out, mergeRows(l, rightNull))
		}
	}
	return out, nil
}

// nullRow builds a row that sets every column of a binding to NULL, used for the
// unmatched side of a LEFT join.
func nullRow(b binding, unique map[string]bool) Row {
	row := make(Row, len(b.schema.Columns)*2)
	for _, col := range b.schema.Columns {
		row[b.alias+"."+col.Name] = types.NullKey{}
		if unique[col.Name] {
			row[col.Name] = types.NullKey{}
		}
	}
	return row
}

func mergeRows(a, b Row) Row {
	merged := make(Row, len(a)+len(b))
	for k, v := range a {
		merged[k] = v
	}
	for k, v := range b {
		merged[k] = v
	}
	return merged
}

func filterRows(rows []Row, where Expr) ([]Row, error) {
	if where == nil {
		return rows, nil
	}
	out := rows[:0:0]
	for _, row := range rows {
		keep, err := Evaluate(where, row)
		if err != nil {
			return nil, err
		}
		if keep {
			out = append(out, row)
		}
	}
	return out, nil
}

// expandProjectionBindings expands a projection list over multiple table
// bindings. "*" expands to every column of every binding, sourced by its
// qualified key.
func expandProjectionBindings(items []SelectItem, bindings []binding) []projSpec {
	var specs []projSpec
	for _, it := range items {
		switch {
		case it.Star:
			for _, b := range bindings {
				for _, c := range b.schema.Columns {
					specs = append(specs, projSpec{name: c.Name, source: b.alias + "." + c.Name})
				}
			}
		case it.Column != nil:
			specs = append(specs, projSpec{name: it.OutputName(), source: it.Column.String()})
		}
	}
	return specs
}

// validateJoinColumns resolves every column reference in the statement against
// the join bindings: a qualified reference must name a known alias and an
// existing column; an unqualified reference must be unambiguous.
func validateJoinColumns(sel *SelectStmt, bindings []binding) error {
	check := func(ref *ColumnRef) error { return resolveBinding(ref, bindings) }

	for _, it := range sel.Items {
		switch {
		case it.Column != nil:
			if err := check(it.Column); err != nil {
				return err
			}
		case it.Agg != nil && !it.Agg.Star:
			if err := check(it.Agg.Column); err != nil {
				return err
			}
		}
	}
	for _, jc := range sel.Joins {
		if err := validateExprBindings(jc.On, bindings); err != nil {
			return err
		}
	}
	for _, g := range sel.GroupBy {
		if err := check(refFromString(g)); err != nil {
			return err
		}
	}
	if sel.OrderBy != nil && !isGrouped(sel) {
		if err := check(refFromString(sel.OrderBy.Column)); err != nil {
			return err
		}
	}
	if err := validateExprBindings(sel.Having, bindings); err != nil {
		return err
	}
	return validateExprBindings(sel.Where, bindings)
}

func validateExprBindings(expr Expr, bindings []binding) error {
	switch e := expr.(type) {
	case nil:
		return nil
	case *ColumnRef:
		return resolveBinding(e, bindings)
	case *AggregateExpr:
		if !e.Call.Star {
			return resolveBinding(e.Call.Column, bindings)
		}
	case *IsNullExpr:
		return validateExprBindings(e.Operand, bindings)
	case *BinaryExpr:
		if err := validateExprBindings(e.Left, bindings); err != nil {
			return err
		}
		return validateExprBindings(e.Right, bindings)
	}
	return nil
}

func resolveBinding(ref *ColumnRef, bindings []binding) error {
	if ref.Qualifier != "" {
		for _, b := range bindings {
			if b.alias != ref.Qualifier {
				continue
			}
			if _, ok := b.schema.Column(ref.Name); !ok {
				return fmt.Errorf("%w: unknown column %q in table %q", ErrPlan, ref.Name, ref.Qualifier)
			}
			return nil
		}
		return fmt.Errorf("%w: unknown table qualifier %q", ErrPlan, ref.Qualifier)
	}
	matches := 0
	for _, b := range bindings {
		if _, ok := b.schema.Column(ref.Name); ok {
			matches++
		}
	}
	switch matches {
	case 0:
		return fmt.Errorf("%w: unknown column %q", ErrPlan, ref.Name)
	case 1:
		return nil
	default:
		return fmt.Errorf("%w: ambiguous column %q; qualify it with a table alias", ErrPlan, ref.Name)
	}
}

func refFromString(s string) *ColumnRef {
	qualifier, name := "", s
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			qualifier, name = s[:i], s[i+1:]
			break
		}
	}
	return &ColumnRef{Qualifier: qualifier, Name: name}
}
