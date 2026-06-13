package sql

import (
	"context"
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// autoIncrementColumn returns the table's AUTO_INCREMENT column, if any. At
// most one column may carry the flag (it must be the integer primary key).
func autoIncrementColumn(schema *TableSchema) (Column, bool) {
	for _, c := range schema.Columns {
		if c.AutoIncrement {
			return c, true
		}
	}
	return Column{}, false
}

// resolveAutoIncrement fills the AUTO_INCREMENT column of an INSERT before the
// rows are encoded. A row that omits the column, or sets it to NULL, receives a
// freshly allocated value; a row with an explicit value advances the counter
// past it so later auto-allocations do not collide. It returns the column list
// and value rows to encode; when the table has no AUTO_INCREMENT column the
// inputs are returned unchanged.
func (e *Executor) resolveAutoIncrement(ctx context.Context, schema *TableSchema, columns []string, rows [][]Expr) ([]string, [][]Expr, error) {
	aiCol, ok := autoIncrementColumn(schema)
	if !ok {
		return columns, rows, nil
	}

	pos := indexOfString(columns, aiCol.Name)
	outCols := columns
	if pos < 0 {
		outCols = append(append(make([]string, 0, len(columns)+1), columns...), aiCol.Name)
	}

	outRows := make([][]Expr, len(rows))
	for i, row := range rows {
		vals := append(make([]Expr, 0, len(row)+1), row...)
		switch {
		case pos < 0:
			n, err := e.autoIncNext(ctx, schema)
			if err != nil {
				return nil, nil, err
			}
			vals = append(vals, &Literal{Kind: LitInt, Int: n})
		default:
			lit, isLit := vals[pos].(*Literal)
			if isLit && lit.Kind == LitNull {
				n, err := e.autoIncNext(ctx, schema)
				if err != nil {
					return nil, nil, err
				}
				vals[pos] = &Literal{Kind: LitInt, Int: n}
			} else if isLit && lit.Kind == LitInt {
				if err := e.autoIncObserve(ctx, schema, lit.Int); err != nil {
					return nil, nil, err
				}
			}
		}
		outRows[i] = vals
	}
	return outCols, outRows, nil
}

// autoIncNext allocates and returns the next AUTO_INCREMENT value for a table.
func (e *Executor) autoIncNext(ctx context.Context, schema *TableSchema) (int64, error) {
	e.aiMu.Lock()
	defer e.aiMu.Unlock()
	if err := e.seedAutoIncLocked(ctx, schema); err != nil {
		return 0, err
	}
	e.aiCounters[schema.Name]++
	return e.aiCounters[schema.Name], nil
}

// autoIncObserve advances the counter so the next allocated value is greater
// than an explicitly inserted value.
func (e *Executor) autoIncObserve(ctx context.Context, schema *TableSchema, value int64) error {
	e.aiMu.Lock()
	defer e.aiMu.Unlock()
	if err := e.seedAutoIncLocked(ctx, schema); err != nil {
		return err
	}
	if value > e.aiCounters[schema.Name] {
		e.aiCounters[schema.Name] = value
	}
	return nil
}

// seedAutoIncLocked initializes a table's counter from the current maximum
// primary key the first time it is needed, so values continue past the rows
// already present (e.g. after reopening the database). Callers hold e.aiMu.
func (e *Executor) seedAutoIncLocked(ctx context.Context, schema *TableSchema) error {
	if e.aiCounters == nil {
		e.aiCounters = make(map[string]int64)
		e.aiSeeded = make(map[string]struct{})
	}
	if _, done := e.aiSeeded[schema.Name]; done {
		return nil
	}
	max, err := e.maxPrimaryInt(ctx, schema)
	if err != nil {
		return err
	}
	e.aiCounters[schema.Name] = max
	e.aiSeeded[schema.Name] = struct{}{}
	return nil
}

// maxPrimaryInt scans the table's primary index and returns the largest
// integer key, or 0 when the table is empty.
func (e *Executor) maxPrimaryInt(ctx context.Context, schema *TableSchema) (int64, error) {
	pk, ok := schema.PrimaryIndex()
	if !ok {
		return 0, fmt.Errorf("%w: table %q has no primary index", ErrExec, schema.Name)
	}
	it, err := e.engine.NewIterator(ctx, schema.Name, pk.Name, storage.IterOptions{})
	if err != nil {
		return 0, fmt.Errorf("%w: open iterator: %v", ErrExec, err)
	}
	defer func() { _ = it.Close() }()

	var max int64
	for it.Next() {
		if n, ok := it.Key().(types.IntKey); ok && int64(n) > max {
			max = int64(n)
		}
	}
	if err := it.Err(); err != nil {
		return 0, fmt.Errorf("%w: scan: %v", ErrExec, err)
	}
	return max, nil
}

// indexOfString returns the position of name in cols, or -1 when absent.
func indexOfString(cols []string, name string) int {
	for i, c := range cols {
		if c == name {
			return i
		}
	}
	return -1
}
