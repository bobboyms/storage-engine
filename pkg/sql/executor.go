package sql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
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
//
// Concurrency: Exec and Query may be called from multiple goroutines on the
// same Executor; the underlying engine serializes writes per table and latches
// index/heap pages in the buffer pool, and background maintenance is safe
// against foreground traffic. This has been exercised under the race detector
// (see TestExecutorConcurrentUsageRace and tests/stress with -race). The B+
// tree read descent uses latch crabbing (it pins the child page before
// releasing the parent), so point reads and scan starts never follow a stale
// child pointer past a concurrent split or merge (see TestCrabbingReadDescent
// in pkg/btree/v2). One remaining narrow window: a scan's leaf-to-leaf
// sibling walk releases the current leaf before pinning the next (crabbing
// there would deadlock against the delete paths' right-to-left merge lock
// order), so a scan running exactly while a merge restructures that sibling
// can momentarily observe it mid-change. Callers needing a strict
// point-in-time view across multiple reads should use a single engine
// transaction (BeginRead) rather than separate Query calls.
type Executor struct {
	engine  *storage.StorageEngine
	catalog *Catalog
	codec   codec.Codec
	ddl     *ddlManager // non-nil only when opened via OpenDatabase
	// dirLock holds the exclusive directory lock taken by OpenDatabase; it is
	// released by Close. Nil for executors built via NewExecutor.
	dirLock *dirLock

	maintMu sync.Mutex
	maint   *maintenanceRunner // non-nil while scheduled maintenance is running
	// lastCheckpointLSN gates maintenance: a pass only checkpoints when the
	// engine's current LSN has advanced past it (i.e. there were writes).
	lastCheckpointLSN atomic.Uint64

	// gcMu guards dirtyTables, the set of tables that have accumulated dead
	// heap space (from DELETE/UPDATE) since the last vacuum. Maintenance
	// vacuums only these tables, so insert-only/read workloads never vacuum.
	gcMu        sync.Mutex
	dirtyTables map[string]struct{}

	// decodeHook, when non-nil, is called once per row decoded during a scan.
	// It is test-only instrumentation for asserting that LIMIT push-down stops
	// the scan early; it is nil in production.
	decodeHook func()

	// aiMu guards the per-table AUTO_INCREMENT counters. Each counter holds the
	// last value handed out for that table; it is lazily seeded from the current
	// maximum primary key on first use and advanced in memory thereafter (so
	// allocations need no per-insert disk scan, and rolled-back values are not
	// reused, matching conventional auto-increment semantics).
	aiMu       sync.Mutex
	aiCounters map[string]int64
	aiSeeded   map[string]struct{}
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

// Query parses and executes a SELECT (or DESCRIBE) statement, returning the
// matching rows. Positional "?" placeholders in query are bound, in order, to
// args; passing a different number of args than placeholders is an error
// wrapping ErrBind.
func (e *Executor) Query(ctx context.Context, query string, args ...any) (*ResultSet, error) {
	stmt, err := parseBound(query, args)
	if err != nil {
		return nil, err
	}
	switch s := stmt.(type) {
	case *SelectStmt:
		return e.execSelect(ctx, s, nil)
	case *SetOpStmt:
		return e.execSetOp(ctx, s)
	case *DescribeStmt:
		return e.execDescribe(s)
	default:
		return nil, fmt.Errorf("%w: Query expects a SELECT or DESCRIBE statement", ErrExec)
	}
}

// execSetOp executes a UNION [ALL] set operation. It runs both sides, requires
// them to project the same number of columns, concatenates the rows, and for a
// plain UNION removes duplicate result rows. The output column names come from
// the left query.
func (e *Executor) execSetOp(ctx context.Context, s *SetOpStmt) (*ResultSet, error) {
	var left *ResultSet
	var err error
	switch l := s.Left.(type) {
	case *SelectStmt:
		left, err = e.execSelect(ctx, l, nil)
	case *SetOpStmt:
		left, err = e.execSetOp(ctx, l)
	default:
		return nil, fmt.Errorf("%w: unsupported set-operation operand %T", ErrExec, s.Left)
	}
	if err != nil {
		return nil, err
	}
	right, err := e.execSelect(ctx, s.Right, nil)
	if err != nil {
		return nil, err
	}
	if len(left.Columns) != len(right.Columns) {
		return nil, fmt.Errorf("%w: each UNION query must return the same number of columns (%d vs %d)", ErrExec, len(left.Columns), len(right.Columns))
	}

	out := &ResultSet{Columns: left.Columns}
	if s.All {
		out.Rows = append(append(make([][]types.Comparable, 0, len(left.Rows)+len(right.Rows)), left.Rows...), right.Rows...)
		return out, nil
	}
	seen := make(map[string]struct{}, len(left.Rows)+len(right.Rows))
	for _, src := range [][][]types.Comparable{left.Rows, right.Rows} {
		for _, row := range src {
			key := rowKey(row)
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			out.Rows = append(out.Rows, row)
		}
	}
	return out, nil
}

// rowKey builds a stable identity key for a result row, used to deduplicate
// UNION output. Each value is rendered with its concrete type so values that
// print alike but differ in type are not conflated.
func rowKey(row []types.Comparable) string {
	var b strings.Builder
	for _, v := range row {
		fmt.Fprintf(&b, "%T:%v|", v, v)
	}
	return b.String()
}

// execSelect executes a parsed SELECT. Queries with joins or a derived FROM
// subquery go through the generalized source pipeline; a plain single-table
// query uses the index-aware fast path. outer is the correlated row from an
// enclosing query (nil at the top level), whose columns inner predicates may
// reference.
func (e *Executor) execSelect(ctx context.Context, sel *SelectStmt, outer Row) (*ResultSet, error) {
	ec := &evalContext{exec: e, ctx: ctx, outer: outer}

	if len(sel.Joins) > 0 || sel.Subquery != nil {
		if hasWindows(sel.Items) {
			return nil, fmt.Errorf("%w: window functions are only supported on single-table queries", ErrExec)
		}
		return queryFrom(ctx, sel, ec, e)
	}

	schema, ok := e.catalog.Table(sel.Table)
	if !ok {
		return nil, fmt.Errorf("%w: unknown table %q", ErrExec, sel.Table)
	}

	plan, err := planSelect(sel, schema, outer != nil)
	if err != nil {
		return nil, err
	}

	rows, err := e.scanRows(ctx, schema, sel.Alias, plan, ec, scanLimit(sel, plan))
	if err != nil {
		return nil, err
	}

	if hasWindows(sel.Items) {
		if len(sel.GroupBy) > 0 || hasAggregates(sel.Items) {
			return nil, fmt.Errorf("%w: window functions cannot be combined with GROUP BY or aggregates", ErrExec)
		}
		return windowResultSet(sel, rows, schema)
	}

	if isGrouped(sel) {
		return groupedResultSet(sel, rows, ec)
	}

	if plan.NeedsSort {
		sortRows(rows, plan.Sort)
	}
	rows = applyOffsetLimit(rows, sel.Offset, sel.Limit)

	return projectRows(rows, expandProjection(sel.Items, schema), ec)
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
// the rows whose residual predicate evaluates to true. When limit >= 0 the scan
// stops once that many matching rows have been collected (LIMIT push-down); a
// negative limit collects every matching row.
func (e *Executor) scanRows(ctx context.Context, schema *TableSchema, alias string, plan *QueryPlan, ec *evalContext, limit int) ([]Row, error) {
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
		if limit >= 0 && len(rows) >= limit {
			break // enough matching rows collected; stop scanning early.
		}
		if e.decodeHook != nil {
			e.decodeHook()
		}
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

// scanLimit returns the number of matching rows scanRows may stop after, or -1
// for an unbounded scan. Push-down is only valid when the rows leave the scan
// in their final order: not for GROUP BY / aggregates (every row is needed) nor
// when an in-memory sort reorders them, and only when a LIMIT is present. The
// cap includes OFFSET, which applyOffsetLimit then trims.
func scanLimit(sel *SelectStmt, plan *QueryPlan) int {
	// Window functions are computed over every matched row before LIMIT, so the
	// scan must not stop early.
	if isGrouped(sel) || hasWindows(sel.Items) || plan.NeedsSort || sel.Limit == nil {
		return -1
	}
	lim := int(*sel.Limit)
	if lim < 0 {
		lim = 0
	}
	off := 0
	if sel.Offset != nil && *sel.Offset > 0 {
		off = int(*sel.Offset)
	}
	return off + lim
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
