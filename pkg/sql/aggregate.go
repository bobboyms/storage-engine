package sql

import (
	"fmt"
	"strings"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// hasAggregates reports whether any projection item is an aggregate call.
func hasAggregates(items []SelectItem) bool {
	for _, it := range items {
		if it.Agg != nil {
			return true
		}
	}
	return false
}

// projSpec maps an output column name to the source row column it reads, or to
// a value expression evaluated against each row when expr is non-nil.
type projSpec struct {
	name   string
	source string
	expr   Expr
}

// expandProjection turns the projection list into concrete output specs,
// expanding "*" to every schema column. It is used by the non-aggregate path.
func expandProjection(items []SelectItem, schema *TableSchema) []projSpec {
	var specs []projSpec
	for _, it := range items {
		switch {
		case it.Star:
			for _, c := range schema.Columns {
				specs = append(specs, projSpec{name: c.Name, source: c.Name})
			}
		case it.Column != nil:
			specs = append(specs, projSpec{name: it.OutputName(), source: it.Column.String()})
		case it.Expr != nil:
			specs = append(specs, projSpec{name: it.OutputName(), expr: it.Expr})
		}
	}
	return specs
}

// projectRows builds a result set from rows using the projection specs,
// evaluating expression specs against each row.
func projectRows(rows []Row, specs []projSpec, ec *evalContext) (*ResultSet, error) {
	cols := make([]string, len(specs))
	for i, s := range specs {
		cols[i] = s.name
	}
	rs := &ResultSet{Columns: cols, Rows: make([][]types.Comparable, len(rows))}
	for i, row := range rows {
		vals := make([]types.Comparable, len(specs))
		for j, s := range specs {
			if s.expr != nil {
				v, err := evalValue(s.expr, row, ec)
				if err != nil {
					return nil, err
				}
				vals[j] = v
				continue
			}
			v, ok := row[s.source]
			if !ok {
				v = types.NullKey{}
			}
			vals[j] = v
		}
		rs.Rows[i] = vals
	}
	return rs, nil
}

// isGrouped reports whether the query requires the grouping/aggregation path.
func isGrouped(sel *SelectStmt) bool {
	return len(sel.GroupBy) > 0 || hasAggregates(sel.Items)
}

// groupedResultSet executes the GROUP BY / aggregate path. With no GROUP BY it
// produces a single group over all rows (the whole-table aggregate case). It
// validates that every bare projection column is a grouping column, computes
// the aggregates appearing in the projection and HAVING per group, filters by
// HAVING, then orders, limits, and projects.
func groupedResultSet(sel *SelectStmt, rows []Row, ec *evalContext) (*ResultSet, error) {
	groupSet := make(map[string]struct{}, len(sel.GroupBy))
	for _, g := range sel.GroupBy {
		groupSet[g] = struct{}{}
	}
	for _, it := range sel.Items {
		switch {
		case it.Star:
			return nil, fmt.Errorf("%w: SELECT * is not allowed with GROUP BY or aggregates", ErrExec)
		case it.Column != nil:
			if _, ok := groupSet[it.Column.String()]; !ok {
				return nil, fmt.Errorf("%w: column %q must appear in GROUP BY or an aggregate", ErrExec, it.Column.String())
			}
		case it.Expr != nil:
			return nil, fmt.Errorf("%w: expression projections are not supported with GROUP BY or aggregates", ErrExec)
		}
	}

	aggs := collectAggregates(sel)
	groups, order := groupRows(rows, sel.GroupBy)

	resultRows := make([]Row, 0, len(groups))
	for _, key := range order {
		members := groups[key]
		gRow := Row{}
		for _, gc := range sel.GroupBy {
			gRow[gc] = members[0][gc]
		}
		for _, a := range aggs {
			v, err := computeAggregate(a, members)
			if err != nil {
				return nil, err
			}
			gRow[a.canonicalName()] = v
		}
		if sel.Having != nil {
			keep, err := evaluate(sel.Having, gRow, ec)
			if err != nil {
				return nil, err
			}
			if !keep {
				continue
			}
		}
		// Expose each item's value under its output name so ORDER BY can
		// reference a column, an aggregate's canonical name, or an alias.
		for _, it := range sel.Items {
			gRow[it.OutputName()] = itemValue(it, gRow)
		}
		resultRows = append(resultRows, gRow)
	}

	if sel.OrderBy != nil {
		sortRows(resultRows, sel.OrderBy)
	}
	resultRows = applyOffsetLimit(resultRows, sel.Offset, sel.Limit)

	specs := make([]projSpec, len(sel.Items))
	for i, it := range sel.Items {
		specs[i] = projSpec{name: it.OutputName(), source: it.OutputName()}
	}
	return projectRows(resultRows, specs, nil)
}

func itemValue(it SelectItem, gRow Row) types.Comparable {
	switch {
	case it.Column != nil:
		return gRow[it.Column.String()]
	case it.Agg != nil:
		return gRow[it.Agg.canonicalName()]
	default:
		return types.NullKey{}
	}
}

// groupRows partitions rows by their grouping-column values. With no grouping
// columns it returns a single group containing all rows (always present, even
// when rows is empty). The returned slice preserves first-appearance order.
func groupRows(rows []Row, groupBy []string) (map[string][]Row, []string) {
	groups := make(map[string][]Row)
	var order []string
	if len(groupBy) == 0 {
		groups[""] = rows
		return groups, []string{""}
	}
	for _, row := range rows {
		key := groupKey(row, groupBy)
		if _, seen := groups[key]; !seen {
			order = append(order, key)
		}
		groups[key] = append(groups[key], row)
	}
	return groups, order
}

func groupKey(row Row, groupBy []string) string {
	var b strings.Builder
	for _, c := range groupBy {
		fmt.Fprintf(&b, "%T:%v|", row[c], row[c])
	}
	return b.String()
}

// collectAggregates gathers the distinct aggregate calls referenced by the
// projection and the HAVING clause.
func collectAggregates(sel *SelectStmt) []*AggregateCall {
	var aggs []*AggregateCall
	seen := make(map[string]struct{})
	add := func(a *AggregateCall) {
		name := a.canonicalName()
		if _, dup := seen[name]; dup {
			return
		}
		seen[name] = struct{}{}
		aggs = append(aggs, a)
	}
	for _, it := range sel.Items {
		if it.Agg != nil {
			add(it.Agg)
		}
	}
	walkAggregates(sel.Having, add)
	return aggs
}

func walkAggregates(expr Expr, add func(*AggregateCall)) {
	switch e := expr.(type) {
	case *AggregateExpr:
		add(e.Call)
	case *IsNullExpr:
		walkAggregates(e.Operand, add)
	case *BinaryExpr:
		walkAggregates(e.Left, add)
		walkAggregates(e.Right, add)
	}
}

func computeAggregate(call *AggregateCall, rows []Row) (types.Comparable, error) {
	if call.Func == "COUNT" && call.Star {
		return types.IntKey(len(rows)), nil
	}

	vals := collectNonNull(rows, call.Column.String())
	if call.Distinct {
		vals = distinctValues(vals)
	}

	switch call.Func {
	case "COUNT":
		return types.IntKey(len(vals)), nil
	case "SUM":
		return sumAggregate(vals)
	case "AVG":
		return avgAggregate(vals)
	case "MIN":
		return minMaxAggregate(vals, true)
	case "MAX":
		return minMaxAggregate(vals, false)
	default:
		return nil, fmt.Errorf("%w: unknown aggregate %q", ErrExec, call.Func)
	}
}

func collectNonNull(rows []Row, column string) []types.Comparable {
	var out []types.Comparable
	for _, row := range rows {
		v, ok := row[column]
		if !ok || isNull(v) {
			continue
		}
		out = append(out, v)
	}
	return out
}

func distinctValues(in []types.Comparable) []types.Comparable {
	seen := make(map[string]struct{}, len(in))
	var out []types.Comparable
	for _, v := range in {
		k := fmt.Sprintf("%T:%v", v, v)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, v)
	}
	return out
}

func sumAggregate(vals []types.Comparable) (types.Comparable, error) {
	if len(vals) == 0 {
		return types.NullKey{}, nil
	}
	allInt := true
	var sumInt int64
	var sumFloat float64
	for _, v := range vals {
		f, ok := numericFloat(v)
		if !ok {
			return nil, fmt.Errorf("%w: SUM requires a numeric column", ErrExec)
		}
		if iv, isInt := v.(types.IntKey); isInt {
			sumInt += int64(iv)
		} else {
			allInt = false
		}
		sumFloat += f
	}
	if allInt {
		return types.IntKey(sumInt), nil
	}
	return types.FloatKey(sumFloat), nil
}

func avgAggregate(vals []types.Comparable) (types.Comparable, error) {
	if len(vals) == 0 {
		return types.NullKey{}, nil
	}
	var sum float64
	for _, v := range vals {
		f, ok := numericFloat(v)
		if !ok {
			return nil, fmt.Errorf("%w: AVG requires a numeric column", ErrExec)
		}
		sum += f
	}
	return types.FloatKey(sum / float64(len(vals))), nil
}

func minMaxAggregate(vals []types.Comparable, min bool) (types.Comparable, error) {
	if len(vals) == 0 {
		return types.NullKey{}, nil
	}
	best := vals[0]
	for _, v := range vals[1:] {
		cmp, err := v.Compare(best)
		if err != nil {
			return nil, fmt.Errorf("%w: %s comparison failed: %v", ErrExec, map[bool]string{true: "MIN", false: "MAX"}[min], err)
		}
		if (min && cmp < 0) || (!min && cmp > 0) {
			best = v
		}
	}
	return best, nil
}

func numericFloat(v types.Comparable) (float64, bool) {
	switch n := v.(type) {
	case types.IntKey:
		return float64(n), true
	case types.FloatKey:
		return float64(n), true
	default:
		return 0, false
	}
}
