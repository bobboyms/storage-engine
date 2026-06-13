package sql

import (
	"context"
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/codec"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// table resolves a table name against the executor's catalog.
func (e *Executor) table(name string) (*TableSchema, bool) { return e.catalog.Table(name) }

// scanQualifiedRows fully scans a table's primary index from the engine's
// committed snapshot, keyed by "alias.col".
func (e *Executor) scanQualifiedRows(ctx context.Context, schema *TableSchema, alias string) ([]Row, error) {
	pk, ok := schema.PrimaryIndex()
	if !ok {
		return nil, fmt.Errorf("%w: table %q has no primary index", ErrExec, schema.Name)
	}
	it, err := e.engine.NewIterator(ctx, schema.Name, pk.Name, storage.IterOptions{})
	if err != nil {
		return nil, fmt.Errorf("%w: open iterator: %v", ErrExec, err)
	}
	defer func() { _ = it.Close() }()
	return scanQualifiedIterator(it, e.codec, schema, alias)
}

// derivedRows executes a FROM subquery against the committed snapshot.
func (e *Executor) derivedRows(ctx context.Context, sub *SelectStmt) (*ResultSet, error) {
	return e.execSelect(ctx, sub, nil)
}

// source is a materialized FROM/JOIN input: an alias, the column names it
// exposes, and its rows keyed by the qualified name "alias.col".
type source struct {
	alias   string
	columns []string
	rows    []Row
}

// fromSource materializes FROM/JOIN inputs for the generalized select
// pipeline. The Executor reads committed engine snapshots; a Tx reads through
// its write transaction so joins and derived tables see read-your-writes.
type fromSource interface {
	// table resolves a table name in the catalog.
	table(name string) (*TableSchema, bool)
	// scanQualifiedRows fully scans a table, returning rows keyed by the
	// qualified name "alias.col".
	scanQualifiedRows(ctx context.Context, schema *TableSchema, alias string) ([]Row, error)
	// derivedRows executes a FROM subquery and returns its result set.
	derivedRows(ctx context.Context, sub *SelectStmt) (*ResultSet, error)
}

// queryFrom executes a SELECT whose FROM contains joins and/or derived
// subqueries. Each source (base table, derived table, or joined source) is
// materialized into rows, combined with nested-loop joins honoring each ON
// predicate, then filtered, grouped, ordered, and projected.
func queryFrom(ctx context.Context, sel *SelectStmt, ec *evalContext, src fromSource) (*ResultSet, error) {
	sources, err := buildSources(ctx, sel, src)
	if err != nil {
		return nil, err
	}
	// Skip strict column validation when correlated: outer references are
	// resolved at evaluation time.
	if ec == nil || ec.outer == nil {
		if err := validateFromColumns(sel, sources); err != nil {
			return nil, err
		}
	}

	unique := uniqueSourceColumns(sources)
	for i := range sources {
		addBareKeys(sources[i], unique)
	}

	rows := sources[0].rows
	for i, jc := range sel.Joins {
		right := sources[i+1]
		rows, err = nestedLoopJoin(rows, right.rows, jc.On, jc.Left, nullSourceRow(right, unique), ec)
		if err != nil {
			return nil, err
		}
	}

	rows, err = filterRows(rows, sel.Where, ec)
	if err != nil {
		return nil, err
	}

	if isGrouped(sel) {
		return groupedResultSet(sel, rows, ec)
	}
	if sel.OrderBy != nil {
		sortRows(rows, sel.OrderBy)
	}
	rows = applyOffsetLimit(rows, sel.Offset, sel.Limit)
	return projectRows(rows, expandProjectionSources(sel.Items, sources), ec)
}

func buildSources(ctx context.Context, sel *SelectStmt, src fromSource) ([]source, error) {
	sources := make([]source, 0, len(sel.Joins)+1)
	seen := map[string]struct{}{}

	add := func(table string, sub *SelectStmt, alias string) error {
		if _, dup := seen[alias]; dup {
			return fmt.Errorf("%w: duplicate table alias %q", ErrExec, alias)
		}
		seen[alias] = struct{}{}
		s, err := sourceFor(ctx, table, sub, alias, src)
		if err != nil {
			return err
		}
		sources = append(sources, s)
		return nil
	}

	if err := add(sel.Table, sel.Subquery, sel.Alias); err != nil {
		return nil, err
	}
	for _, jc := range sel.Joins {
		if err := add(jc.Table, jc.Subquery, jc.Alias); err != nil {
			return nil, err
		}
	}
	return sources, nil
}

// sourceFor materializes a single FROM/JOIN source: a derived subquery is
// executed and its result rows are re-keyed by the alias; a table is fully
// scanned with qualified keys.
func sourceFor(ctx context.Context, table string, sub *SelectStmt, alias string, src fromSource) (source, error) {
	if sub != nil {
		rs, err := src.derivedRows(ctx, sub)
		if err != nil {
			return source{}, err
		}
		rows := make([]Row, len(rs.Rows))
		for i, vals := range rs.Rows {
			row := make(Row, len(rs.Columns))
			for j, c := range rs.Columns {
				row[alias+"."+c] = vals[j]
			}
			rows[i] = row
		}
		return source{alias: alias, columns: rs.Columns, rows: rows}, nil
	}

	schema, ok := src.table(table)
	if !ok {
		return source{}, fmt.Errorf("%w: unknown table %q", ErrExec, table)
	}
	rows, err := src.scanQualifiedRows(ctx, schema, alias)
	if err != nil {
		return source{}, err
	}
	cols := make([]string, len(schema.Columns))
	for i, c := range schema.Columns {
		cols[i] = c.Name
	}
	return source{alias: alias, columns: cols, rows: rows}, nil
}

// scanQualifiedIterator drains a primary-index iterator, decoding each row
// keyed only by the qualified name "alias.col". Shared by the executor's
// snapshot scan and the transaction's read-your-writes scan.
func scanQualifiedIterator(it storage.Iterator, cdc codec.Codec, schema *TableSchema, alias string) ([]Row, error) {
	var rows []Row
	for it.Next() {
		decoded, err := decodeRow(cdc, schema, alias, it.Value())
		if err != nil {
			return nil, err
		}
		row := make(Row, len(schema.Columns))
		for _, col := range schema.Columns {
			qualified := alias + "." + col.Name
			row[qualified] = decoded[qualified]
		}
		rows = append(rows, row)
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("%w: scan: %v", ErrExec, err)
	}
	return rows, nil
}

// uniqueSourceColumns returns the set of column names that appear in exactly one
// source, so they can be referenced without a qualifier.
func uniqueSourceColumns(sources []source) map[string]bool {
	counts := make(map[string]int)
	for _, s := range sources {
		for _, c := range s.columns {
			counts[c]++
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

// addBareKeys adds an unqualified key for each of the source's columns that is
// unique across all sources.
func addBareKeys(s source, unique map[string]bool) {
	for _, row := range s.rows {
		for _, c := range s.columns {
			if unique[c] {
				row[c] = row[s.alias+"."+c]
			}
		}
	}
}

// nestedLoopJoin combines left and right rows whose ON predicate holds. For a
// LEFT join, a left row with no matching right row is emitted once with the
// right side's columns set to NULL (via rightNull) so WHERE and projection see
// explicit NULLs rather than missing keys.
func nestedLoopJoin(left, right []Row, on Expr, leftJoin bool, rightNull Row, ec *evalContext) ([]Row, error) {
	var out []Row
	for _, l := range left {
		matched := false
		for _, r := range right {
			merged := mergeRows(l, r)
			ok, err := evaluate(on, merged, ec)
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

// nullSourceRow builds a row setting every column of a source to NULL, used for
// the unmatched side of a LEFT join.
func nullSourceRow(s source, unique map[string]bool) Row {
	row := make(Row, len(s.columns)*2)
	for _, c := range s.columns {
		row[s.alias+"."+c] = types.NullKey{}
		if unique[c] {
			row[c] = types.NullKey{}
		}
	}
	return row
}

func filterRows(rows []Row, where Expr, ec *evalContext) ([]Row, error) {
	if where == nil {
		return rows, nil
	}
	out := rows[:0:0]
	for _, row := range rows {
		keep, err := evaluate(where, row, ec)
		if err != nil {
			return nil, err
		}
		if keep {
			out = append(out, row)
		}
	}
	return out, nil
}

// expandProjectionSources expands a projection list over the query's sources.
// "*" expands to every column of every source, sourced by its qualified key.
func expandProjectionSources(items []SelectItem, sources []source) []projSpec {
	var specs []projSpec
	for _, it := range items {
		switch {
		case it.Star:
			for _, s := range sources {
				for _, c := range s.columns {
					specs = append(specs, projSpec{name: c, source: s.alias + "." + c})
				}
			}
		case it.Column != nil:
			specs = append(specs, projSpec{name: it.OutputName(), source: it.Column.String()})
		case it.Expr != nil:
			specs = append(specs, projSpec{name: it.OutputName(), expr: it.Expr})
		}
	}
	return specs
}

// validateFromColumns resolves every column reference in the statement against
// the query's sources.
func validateFromColumns(sel *SelectStmt, sources []source) error {
	for _, it := range sel.Items {
		switch {
		case it.Column != nil:
			if err := resolveSource(it.Column, sources); err != nil {
				return err
			}
		case it.Agg != nil && !it.Agg.Star:
			if err := resolveSource(it.Agg.Column, sources); err != nil {
				return err
			}
		}
	}
	for _, jc := range sel.Joins {
		if err := validateExprSources(jc.On, sources); err != nil {
			return err
		}
	}
	for _, g := range sel.GroupBy {
		if err := resolveSource(refFromString(g), sources); err != nil {
			return err
		}
	}
	if sel.OrderBy != nil && !isGrouped(sel) {
		if err := resolveSource(refFromString(sel.OrderBy.Column), sources); err != nil {
			return err
		}
	}
	if err := validateExprSources(sel.Having, sources); err != nil {
		return err
	}
	return validateExprSources(sel.Where, sources)
}

func validateExprSources(expr Expr, sources []source) error {
	switch e := expr.(type) {
	case nil:
		return nil
	case *ColumnRef:
		return resolveSource(e, sources)
	case *AggregateExpr:
		if !e.Call.Star {
			return resolveSource(e.Call.Column, sources)
		}
	case *IsNullExpr:
		return validateExprSources(e.Operand, sources)
	case *BinaryExpr:
		if err := validateExprSources(e.Left, sources); err != nil {
			return err
		}
		return validateExprSources(e.Right, sources)
	}
	return nil
}

func resolveSource(ref *ColumnRef, sources []source) error {
	if ref.Qualifier != "" {
		for _, s := range sources {
			if s.alias != ref.Qualifier {
				continue
			}
			if !containsString(s.columns, ref.Name) {
				return fmt.Errorf("%w: unknown column %q in %q", ErrPlan, ref.Name, ref.Qualifier)
			}
			return nil
		}
		return fmt.Errorf("%w: unknown table qualifier %q", ErrPlan, ref.Qualifier)
	}
	matches := 0
	for _, s := range sources {
		if containsString(s.columns, ref.Name) {
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

func containsString(ss []string, target string) bool {
	for _, s := range ss {
		if s == target {
			return true
		}
	}
	return false
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
