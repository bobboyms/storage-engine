package sql

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/bobboyms/storage-engine/pkg/codec"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// ErrExec is the sentinel wrapped by all execution errors.
var ErrExec = errors.New("sql: exec error")

// Executor runs SQL statements against a storage engine, using a catalog to
// resolve schemas and a codec to decode stored rows.
type Executor struct {
	engine  *storage.StorageEngine
	catalog *Catalog
	codec   codec.Codec
}

// NewExecutor builds an Executor. The codec must match the one the engine uses
// to encode documents (the engine's default is bsoncodec).
func NewExecutor(engine *storage.StorageEngine, catalog *Catalog, c codec.Codec) *Executor {
	return &Executor{engine: engine, catalog: catalog, codec: c}
}

// ResultSet is the output of a SELECT: column names and rows whose values are
// positionally aligned to Columns.
type ResultSet struct {
	Columns []string
	Rows    [][]types.Comparable
}

// Query parses and executes a SELECT statement, returning the matching rows.
func (e *Executor) Query(ctx context.Context, query string) (*ResultSet, error) {
	stmt, err := Parse(query)
	if err != nil {
		return nil, err
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		return nil, fmt.Errorf("%w: Query expects a SELECT statement", ErrExec)
	}

	schema, ok := e.catalog.Table(sel.Table)
	if !ok {
		return nil, fmt.Errorf("%w: unknown table %q", ErrExec, sel.Table)
	}

	plan, err := Plan(sel, schema)
	if err != nil {
		return nil, err
	}

	projection := projectionColumns(sel, schema)

	rows, err := e.scanRows(ctx, schema, plan)
	if err != nil {
		return nil, err
	}

	if plan.NeedsSort {
		sortRows(rows, plan.Sort)
	}
	rows = applyOffsetLimit(rows, sel.Offset, sel.Limit)

	return project(rows, projection), nil
}

func projectionColumns(sel *SelectStmt, schema *TableSchema) []string {
	if !sel.Star {
		return sel.Columns
	}
	cols := make([]string, len(schema.Columns))
	for i, c := range schema.Columns {
		cols[i] = c.Name
	}
	return cols
}

// scanRows opens the planned index scan, decodes each visible row, and keeps
// the rows whose residual predicate evaluates to true.
func (e *Executor) scanRows(ctx context.Context, schema *TableSchema, plan *QueryPlan) ([]Row, error) {
	// Reverse scans are not supported by the engine, so descending order is
	// always handled by the in-memory sort (plan.NeedsSort).
	it, err := e.engine.NewIterator(ctx, plan.TableName, plan.IndexName, storage.IterOptions{
		Lower: plan.Lower,
		Upper: plan.Upper,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: open iterator: %v", ErrExec, err)
	}
	defer func() { _ = it.Close() }()

	var rows []Row
	for it.Next() {
		row, err := e.decodeRow(schema, it.Value())
		if err != nil {
			return nil, err
		}
		if plan.Residual != nil {
			keep, err := Evaluate(plan.Residual, row)
			if err != nil {
				return nil, err
			}
			if !keep {
				continue
			}
		}
		rows = append(rows, row)
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("%w: scan: %v", ErrExec, err)
	}
	return rows, nil
}

// decodeRow decodes raw heap bytes into a Row keyed by every schema column,
// normalizing each value to the column's declared type. Absent fields become
// NULL.
func (e *Executor) decodeRow(schema *TableSchema, raw []byte) (Row, error) {
	doc, err := e.codec.Open(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: decode row: %v", ErrExec, err)
	}
	row := make(Row, len(schema.Columns))
	for _, col := range schema.Columns {
		val, present, err := doc.Key(col.Name)
		if err != nil {
			return nil, fmt.Errorf("%w: read column %q: %v", ErrExec, col.Name, err)
		}
		if !present || val == nil {
			row[col.Name] = types.NullKey{}
			continue
		}
		row[col.Name] = NormalizeValue(val, col.Type)
	}
	return row, nil
}

func sortRows(rows []Row, order *OrderBy) {
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i][order.Column], rows[j][order.Column]
		cmp, err := a.Compare(b)
		if err != nil {
			return false
		}
		if order.Desc {
			return cmp > 0
		}
		return cmp < 0
	})
}

func applyOffsetLimit(rows []Row, offset, limit *int64) []Row {
	if offset != nil {
		off := int(*offset)
		if off >= len(rows) {
			return nil
		}
		if off > 0 {
			rows = rows[off:]
		}
	}
	if limit != nil {
		lim := int(*limit)
		if lim < 0 {
			lim = 0
		}
		if lim < len(rows) {
			rows = rows[:lim]
		}
	}
	return rows
}

func project(rows []Row, columns []string) *ResultSet {
	rs := &ResultSet{Columns: columns, Rows: make([][]types.Comparable, len(rows))}
	for i, row := range rows {
		vals := make([]types.Comparable, len(columns))
		for j, c := range columns {
			v, ok := row[c]
			if !ok {
				v = types.NullKey{}
			}
			vals[j] = v
		}
		rs.Rows[i] = vals
	}
	return rs
}
