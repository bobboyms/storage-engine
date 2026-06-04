package sql

import (
	"fmt"

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

// projSpec maps an output column name to the source row column it reads.
type projSpec struct {
	name   string
	source string
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
			specs = append(specs, projSpec{name: it.OutputName(), source: it.Column.Name})
		}
	}
	return specs
}

// projectRows builds a result set from rows using the projection specs.
func projectRows(rows []Row, specs []projSpec) *ResultSet {
	cols := make([]string, len(specs))
	for i, s := range specs {
		cols[i] = s.name
	}
	rs := &ResultSet{Columns: cols, Rows: make([][]types.Comparable, len(rows))}
	for i, row := range rows {
		vals := make([]types.Comparable, len(specs))
		for j, s := range specs {
			v, ok := row[s.source]
			if !ok {
				v = types.NullKey{}
			}
			vals[j] = v
		}
		rs.Rows[i] = vals
	}
	return rs
}

// aggregateRow computes one aggregate value per item over the given rows.
func aggregateRow(items []SelectItem, rows []Row) ([]types.Comparable, error) {
	out := make([]types.Comparable, len(items))
	for i, it := range items {
		if it.Agg == nil {
			return nil, fmt.Errorf("%w: column %q must appear in GROUP BY or an aggregate", ErrExec, it.OutputName())
		}
		v, err := computeAggregate(it.Agg, rows)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func computeAggregate(call *AggregateCall, rows []Row) (types.Comparable, error) {
	if call.Func == "COUNT" && call.Star {
		return types.IntKey(len(rows)), nil
	}

	vals := collectNonNull(rows, call.Column.Name)
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
