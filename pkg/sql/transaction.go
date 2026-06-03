package sql

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/codec"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// Tx is an explicit SQL transaction. It runs queries and data-modifying
// statements against a single storage write transaction, providing
// read-your-writes, atomic commit, rollback, and savepoints.
//
// Reads inside a transaction scan the primary index and re-evaluate the full
// WHERE predicate per row, which keeps read-your-writes exact (see
// storage.WriteTransaction.NewIterator). Writes are staged until Commit.
type Tx struct {
	wtx     *storage.WriteTransaction
	catalog *Catalog
	codec   codec.Codec
}

// Begin starts an explicit transaction with the engine's default isolation
// level (RepeatableRead).
func (e *Executor) Begin() *Tx {
	return &Tx{wtx: e.engine.BeginWriteTransaction(), catalog: e.catalog, codec: e.codec}
}

// Commit makes all staged changes durable and visible.
func (t *Tx) Commit(ctx context.Context) error { return t.wtx.Commit(ctx) }

// Rollback discards all staged changes.
func (t *Tx) Rollback(ctx context.Context) error { return t.wtx.Rollback(ctx) }

// Savepoint establishes a named savepoint.
func (t *Tx) Savepoint(name string) error { return t.wtx.Savepoint(name) }

// RollbackToSavepoint discards changes staged after the named savepoint.
func (t *Tx) RollbackToSavepoint(name string) error { return t.wtx.RollbackToSavepoint(name) }

// Query parses and executes a SELECT against the transaction's snapshot
// (with read-your-writes). A trailing FOR UPDATE locks the matched rows.
func (t *Tx) Query(ctx context.Context, query string) (*ResultSet, error) {
	stmt, err := Parse(query)
	if err != nil {
		return nil, err
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		return nil, fmt.Errorf("%w: Query expects a SELECT statement", ErrExec)
	}
	schema, ok := t.catalog.Table(sel.Table)
	if !ok {
		return nil, fmt.Errorf("%w: unknown table %q", ErrExec, sel.Table)
	}
	// Plan validates column references; the access path is forced to the
	// primary index for transactional reads, so only its residual is reused.
	if _, err := Plan(sel, schema); err != nil {
		return nil, err
	}

	rows, err := t.scanRows(ctx, schema, sel.Where)
	if err != nil {
		return nil, err
	}
	if sel.ForUpdate {
		if err := t.lockRows(ctx, schema, rows); err != nil {
			return nil, err
		}
	}

	if needsSort, order := sortDecision(sel, schema); needsSort {
		sortRows(rows, order)
	}
	rows = applyOffsetLimit(rows, sel.Offset, sel.Limit)
	return project(rows, projectionColumns(sel, schema)), nil
}

// sortDecision reports whether the transactional primary-index scan must be
// sorted in memory to satisfy ORDER BY, and the ordering to apply.
func sortDecision(sel *SelectStmt, schema *TableSchema) (bool, *OrderBy) {
	if sel.OrderBy == nil {
		return false, nil
	}
	pk, ok := schema.PrimaryIndex()
	if ok && sel.OrderBy.Column == pk.Column && !sel.OrderBy.Desc {
		return false, nil
	}
	return true, sel.OrderBy
}

func (t *Tx) scanRows(ctx context.Context, schema *TableSchema, residual Expr) ([]Row, error) {
	pk, ok := schema.PrimaryIndex()
	if !ok {
		return nil, fmt.Errorf("%w: table %q has no primary index", ErrExec, schema.Name)
	}
	it, err := t.wtx.NewIterator(ctx, schema.Name, pk.Name, storage.IterOptions{})
	if err != nil {
		return nil, fmt.Errorf("%w: open iterator: %v", ErrExec, err)
	}
	defer func() { _ = it.Close() }()

	var rows []Row
	for it.Next() {
		row, err := decodeRow(t.codec, schema, it.Value())
		if err != nil {
			return nil, err
		}
		if residual != nil {
			keep, err := Evaluate(residual, row)
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

func (t *Tx) lockRows(ctx context.Context, schema *TableSchema, rows []Row) error {
	pk, _ := schema.PrimaryIndex()
	pkCol, _ := schema.Column(pk.Column)
	for _, row := range rows {
		key := NormalizeValue(row[pk.Column], pkCol.Type)
		if _, _, err := t.wtx.GetForUpdate(ctx, schema.Name, pk.Name, key); err != nil {
			return fmt.Errorf("%w: lock row: %v", ErrExec, err)
		}
	}
	return nil
}

// Exec parses and executes an INSERT, UPDATE, or DELETE within the
// transaction, returning the number of affected rows.
func (t *Tx) Exec(ctx context.Context, query string) (int64, error) {
	stmt, err := Parse(query)
	if err != nil {
		return 0, err
	}
	switch s := stmt.(type) {
	case *InsertStmt:
		return t.execInsert(ctx, s)
	case *UpdateStmt:
		return t.execUpdate(ctx, s)
	case *DeleteStmt:
		return t.execDelete(ctx, s)
	default:
		return 0, fmt.Errorf("%w: Exec expects INSERT, UPDATE, or DELETE", ErrExec)
	}
}

func (t *Tx) execInsert(ctx context.Context, stmt *InsertStmt) (int64, error) {
	schema, ok := t.catalog.Table(stmt.Table)
	if !ok {
		return 0, fmt.Errorf("%w: unknown table %q", ErrExec, stmt.Table)
	}
	doc, keys, err := buildInsertDoc(schema, stmt)
	if err != nil {
		return 0, err
	}
	jsonDoc, err := json.Marshal(doc)
	if err != nil {
		return 0, fmt.Errorf("%w: encode document: %v", ErrExec, err)
	}
	if err := t.wtx.WriteRow(ctx, stmt.Table, string(jsonDoc), keys, true); err != nil {
		return 0, fmt.Errorf("%w: insert row: %v", ErrExec, err)
	}
	return 1, nil
}

func (t *Tx) execUpdate(ctx context.Context, stmt *UpdateStmt) (int64, error) {
	schema, ok := t.catalog.Table(stmt.Table)
	if !ok {
		return 0, fmt.Errorf("%w: unknown table %q", ErrExec, stmt.Table)
	}
	pk, ok := schema.PrimaryIndex()
	if !ok {
		return 0, fmt.Errorf("%w: table %q has no primary index", ErrExec, stmt.Table)
	}
	if err := validateAssignments(stmt, schema, pk); err != nil {
		return 0, err
	}
	if err := validateExprColumns(stmt.Where, schema); err != nil {
		return 0, err
	}

	matches, err := t.matchingRaw(ctx, schema, stmt.Where)
	if err != nil {
		return 0, err
	}
	var affected int64
	for _, raw := range matches {
		doc, err := rawToMap(t.codec, raw)
		if err != nil {
			return 0, err
		}
		for _, a := range stmt.Assignments {
			doc[a.Column] = literalToGo(a.Value.(*Literal))
		}
		keys, err := keysFromMap(schema, doc)
		if err != nil {
			return 0, err
		}
		jsonDoc, err := json.Marshal(doc)
		if err != nil {
			return 0, fmt.Errorf("%w: encode document: %v", ErrExec, err)
		}
		if err := t.wtx.WriteRow(ctx, stmt.Table, string(jsonDoc), keys, false); err != nil {
			return 0, fmt.Errorf("%w: upsert row: %v", ErrExec, err)
		}
		affected++
	}
	return affected, nil
}

func (t *Tx) execDelete(ctx context.Context, stmt *DeleteStmt) (int64, error) {
	schema, ok := t.catalog.Table(stmt.Table)
	if !ok {
		return 0, fmt.Errorf("%w: unknown table %q", ErrExec, stmt.Table)
	}
	pk, ok := schema.PrimaryIndex()
	if !ok {
		return 0, fmt.Errorf("%w: table %q has no primary index", ErrExec, stmt.Table)
	}
	if err := validateExprColumns(stmt.Where, schema); err != nil {
		return 0, err
	}

	pkCol, _ := schema.Column(pk.Column)
	keys, err := t.matchingPrimaryKeys(ctx, schema, stmt.Where, pk.Column, pkCol.Type)
	if err != nil {
		return 0, err
	}
	var affected int64
	for _, key := range keys {
		if err := t.wtx.Del(ctx, stmt.Table, pk.Name, key); err != nil {
			return 0, fmt.Errorf("%w: delete row: %v", ErrExec, err)
		}
		affected++
	}
	return affected, nil
}

func (t *Tx) matchingRaw(ctx context.Context, schema *TableSchema, where Expr) ([][]byte, error) {
	pk, _ := schema.PrimaryIndex()
	it, err := t.wtx.NewIterator(ctx, schema.Name, pk.Name, storage.IterOptions{})
	if err != nil {
		return nil, fmt.Errorf("%w: open iterator: %v", ErrExec, err)
	}
	defer func() { _ = it.Close() }()

	var out [][]byte
	for it.Next() {
		raw := append([]byte(nil), it.Value()...)
		keep := true
		if where != nil {
			row, err := decodeRow(t.codec, schema, raw)
			if err != nil {
				return nil, err
			}
			if keep, err = Evaluate(where, row); err != nil {
				return nil, err
			}
		}
		if keep {
			out = append(out, raw)
		}
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("%w: scan: %v", ErrExec, err)
	}
	return out, nil
}

func (t *Tx) matchingPrimaryKeys(ctx context.Context, schema *TableSchema, where Expr, pkColumn string, pkType storage.DataType) ([]types.Comparable, error) {
	pk, _ := schema.PrimaryIndex()
	it, err := t.wtx.NewIterator(ctx, schema.Name, pk.Name, storage.IterOptions{})
	if err != nil {
		return nil, fmt.Errorf("%w: open iterator: %v", ErrExec, err)
	}
	defer func() { _ = it.Close() }()

	var keys []types.Comparable
	for it.Next() {
		row, err := decodeRow(t.codec, schema, it.Value())
		if err != nil {
			return nil, err
		}
		keep := true
		if where != nil {
			if keep, err = Evaluate(where, row); err != nil {
				return nil, err
			}
		}
		if keep {
			keys = append(keys, NormalizeValue(row[pkColumn], pkType))
		}
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("%w: scan: %v", ErrExec, err)
	}
	return keys, nil
}

// buildInsertDoc validates an INSERT and returns its document map and the
// index key map required to write it.
func buildInsertDoc(schema *TableSchema, stmt *InsertStmt) (map[string]any, map[string]types.Comparable, error) {
	if len(stmt.Columns) != len(stmt.Values) {
		return nil, nil, fmt.Errorf("%w: %d columns but %d values", ErrExec, len(stmt.Columns), len(stmt.Values))
	}
	doc := make(map[string]any, len(stmt.Columns))
	values := make(map[string]*Literal, len(stmt.Columns))
	for i, name := range stmt.Columns {
		col, ok := schema.Column(name)
		if !ok {
			return nil, nil, fmt.Errorf("%w: unknown column %q", ErrExec, name)
		}
		lit, ok := stmt.Values[i].(*Literal)
		if !ok {
			return nil, nil, fmt.Errorf("%w: value for %q is not a literal", ErrExec, name)
		}
		if _, err := ColumnValue(lit, col.Type); err != nil {
			return nil, nil, fmt.Errorf("%w: %v", ErrExec, err)
		}
		doc[name] = literalToGo(lit)
		values[name] = lit
	}
	keys, err := keysForInsert(schema, values)
	if err != nil {
		return nil, nil, err
	}
	return doc, keys, nil
}
