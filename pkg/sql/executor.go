package sql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

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
	ddl     *ddlManager // non-nil only when opened via OpenDatabase

	maintMu sync.Mutex
	maint   *maintenanceRunner // non-nil while scheduled maintenance is running
	// lastCheckpointLSN gates maintenance: a pass only checkpoints when the
	// engine's current LSN has advanced past it (i.e. there were writes).
	lastCheckpointLSN atomic.Uint64
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
	return e.execSelect(ctx, sel, nil)
}

// execSelect executes a parsed SELECT. Queries with joins or a derived FROM
// subquery go through the generalized source pipeline; a plain single-table
// query uses the index-aware fast path. outer is the correlated row from an
// enclosing query (nil at the top level), whose columns inner predicates may
// reference.
func (e *Executor) execSelect(ctx context.Context, sel *SelectStmt, outer Row) (*ResultSet, error) {
	ec := &evalContext{exec: e, ctx: ctx, outer: outer}

	if len(sel.Joins) > 0 || sel.Subquery != nil {
		return e.queryFrom(ctx, sel, ec)
	}

	schema, ok := e.catalog.Table(sel.Table)
	if !ok {
		return nil, fmt.Errorf("%w: unknown table %q", ErrExec, sel.Table)
	}

	plan, err := planSelect(sel, schema, outer != nil)
	if err != nil {
		return nil, err
	}

	rows, err := e.scanRows(ctx, schema, sel.Alias, plan, ec)
	if err != nil {
		return nil, err
	}

	if isGrouped(sel) {
		return groupedResultSet(sel, rows, ec)
	}

	if plan.NeedsSort {
		sortRows(rows, plan.Sort)
	}
	rows = applyOffsetLimit(rows, sel.Offset, sel.Limit)

	return projectRows(rows, expandProjection(sel.Items, schema)), nil
}

// planSelect plans a single-table SELECT. When correlated (a column may refer
// to an outer query), column validation is skipped and a full scan is used,
// since outer references are resolved at evaluation time.
func planSelect(sel *SelectStmt, schema *TableSchema, correlated bool) (*QueryPlan, error) {
	if !correlated {
		return Plan(sel, schema)
	}
	pk, ok := schema.PrimaryIndex()
	if !ok {
		return nil, fmt.Errorf("%w: table %q has no primary index", ErrPlan, schema.Name)
	}
	plan := &QueryPlan{TableName: schema.Name, IndexName: pk.Name, Column: pk.Column, Residual: sel.Where}
	if sel.OrderBy != nil && (sel.OrderBy.Column != pk.Column || sel.OrderBy.Desc) {
		plan.NeedsSort = true
		plan.Sort = sel.OrderBy
	}
	return plan, nil
}

// scanRows opens the planned index scan, decodes each visible row, and keeps
// the rows whose residual predicate evaluates to true.
func (e *Executor) scanRows(ctx context.Context, schema *TableSchema, alias string, plan *QueryPlan, ec *evalContext) ([]Row, error) {
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
		row, err := decodeRow(e.codec, schema, alias, it.Value())
		if err != nil {
			return nil, err
		}
		if plan.Residual != nil {
			keep, err := evaluate(plan.Residual, row, ec)
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
// NULL. When alias is non-empty, each column is also stored under its
// qualified name "alias.col" so qualified references resolve.
func decodeRow(c codec.Codec, schema *TableSchema, alias string, raw []byte) (Row, error) {
	doc, err := c.Open(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: decode row: %v", ErrExec, err)
	}
	row := make(Row, len(schema.Columns))
	for _, col := range schema.Columns {
		val, present, err := doc.Key(col.Name)
		v := types.Comparable(types.NullKey{})
		if err != nil {
			return nil, fmt.Errorf("%w: read column %q: %v", ErrExec, col.Name, err)
		}
		if present && val != nil {
			v = NormalizeValue(val, col.Type)
		}
		row[col.Name] = v
		if alias != "" {
			row[alias+"."+col.Name] = v
		}
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
