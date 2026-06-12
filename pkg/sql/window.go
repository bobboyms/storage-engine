package sql

import (
	"github.com/bobboyms/storage-engine/pkg/types"
)

// hasWindows reports whether any projection item is a window function.
func hasWindows(items []SelectItem) bool {
	for _, it := range items {
		if it.Window != nil {
			return true
		}
	}
	return false
}

// windowResultSet evaluates a SELECT whose projection contains window
// functions. Window values are computed over all matched rows (after WHERE,
// before LIMIT), each window column is materialized into every row under its
// output name, and the rows are then ordered, trimmed, and projected.
func windowResultSet(sel *SelectStmt, rows []Row, schema *TableSchema) (*ResultSet, error) {
	for _, it := range sel.Items {
		if it.Window == nil {
			continue
		}
		if err := computeWindow(it.Window, rows, it.OutputName()); err != nil {
			return nil, err
		}
	}

	if sel.OrderBy != nil {
		sortRows(rows, sel.OrderBy)
	}
	rows = applyOffsetLimit(rows, sel.Offset, sel.Limit)

	return projectRows(rows, windowProjection(sel.Items, schema), nil)
}

// windowProjection builds the projection specs for a window query, expanding
// "*", passing bare columns through, and reading each window column from the
// output name under which computeWindow stored it.
func windowProjection(items []SelectItem, schema *TableSchema) []projSpec {
	var specs []projSpec
	for _, it := range items {
		switch {
		case it.Star:
			for _, c := range schema.Columns {
				specs = append(specs, projSpec{name: c.Name, source: c.Name})
			}
		case it.Column != nil:
			specs = append(specs, projSpec{name: it.OutputName(), source: it.Column.String()})
		case it.Window != nil:
			specs = append(specs, projSpec{name: it.OutputName(), source: it.OutputName()})
		}
	}
	return specs
}

// computeWindow evaluates one window function across rows, partitioning by its
// PARTITION BY columns and ordering each partition by its ORDER BY clause, then
// writes each row's result under outName.
func computeWindow(w *WindowCall, rows []Row, outName string) error {
	byKey, partitions := groupRows(rows, w.Partition)
	for _, key := range partitions {
		members := byKey[key]
		if w.Order != nil {
			sortRows(members, w.Order)
		}
		if err := computeWindowPartition(w, members, outName); err != nil {
			return err
		}
	}
	return nil
}

// computeWindowPartition assigns the window result for one ordered partition.
// Ranking functions number rows directly; aggregate windows produce a
// whole-partition total when there is no ORDER BY, or a running total (frame =
// unbounded preceding through the current row) when ordered.
func computeWindowPartition(w *WindowCall, members []Row, outName string) error {
	switch w.Func {
	case "ROW_NUMBER", "RANK", "DENSE_RANK":
		assignRanking(w, members, outName)
		return nil
	default:
		return assignAggregateWindow(w, members, outName)
	}
}

// assignRanking computes ROW_NUMBER, RANK, and DENSE_RANK over an ordered
// partition. RANK and DENSE_RANK treat rows with equal ORDER BY keys as peers;
// with no ORDER BY all rows are peers.
func assignRanking(w *WindowCall, members []Row, outName string) {
	rank, dense := 0, 0
	for i, m := range members {
		newPeer := i == 0 || !sameOrderKey(members[i-1], m, w.Order)
		if newPeer {
			rank = i + 1
			dense++
		}
		switch w.Func {
		case "ROW_NUMBER":
			m[outName] = types.IntKey(i + 1)
		case "RANK":
			m[outName] = types.IntKey(rank)
		case "DENSE_RANK":
			m[outName] = types.IntKey(dense)
		}
	}
}

// sameOrderKey reports whether two rows share the same value for the window's
// ORDER BY column. With no ORDER BY every row is a peer.
func sameOrderKey(a, b Row, order *OrderBy) bool {
	if order == nil {
		return true
	}
	cmp, err := a[order.Column].Compare(b[order.Column])
	return err == nil && cmp == 0
}

// assignAggregateWindow computes an aggregate window (SUM/COUNT/AVG/MIN/MAX).
func assignAggregateWindow(w *WindowCall, members []Row, outName string) error {
	call := &AggregateCall{Func: w.Func, Star: w.Star, Column: w.Arg}
	if w.Order == nil {
		val, err := computeAggregate(call, members)
		if err != nil {
			return err
		}
		for _, m := range members {
			m[outName] = val
		}
		return nil
	}
	for i := range members {
		val, err := computeAggregate(call, members[:i+1])
		if err != nil {
			return err
		}
		members[i][outName] = val
	}
	return nil
}
