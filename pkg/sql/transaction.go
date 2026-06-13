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
	exec    *Executor
	wtx     *storage.WriteTransaction
	catalog *Catalog
	codec   codec.Codec
	// garbage collects tables whose committed changes will leave dead heap
	// space (from UPDATE/DELETE); they are marked for vacuum on Commit.
	garbage map[string]struct{}
}

// Begin starts an explicit transaction with the engine's default isolation
// level (RepeatableRead).
func (e *Executor) Begin() *Tx {
	return &Tx{exec: e, wtx: e.engine.BeginWriteTransaction(), catalog: e.catalog, codec: e.codec}
}

// Commit makes all staged changes durable and visible.
func (t *Tx) Commit(ctx context.Context) error {
	if err := t.wtx.Commit(ctx); err != nil {
		return err
	}
	// The committed UPDATE/DELETEs left dead versions; schedule a vacuum.
	for table := range t.garbage {
		t.exec.markGarbage(table)
	}
	t.garbage = nil
	return nil
}

// Rollback discards all staged changes.
func (t *Tx) Rollback(ctx context.Context) error { return t.wtx.Rollback(ctx) }

// Savepoint establishes a named savepoint.
func (t *Tx) Savepoint(name string) error { return t.wtx.Savepoint(name) }

// RollbackToSavepoint discards changes staged after the named savepoint.
func (t *Tx) RollbackToSavepoint(name string) error { return t.wtx.RollbackToSavepoint(name) }

// Query parses and executes a SELECT against the transaction's snapshot
// (with read-your-writes). A trailing FOR UPDATE locks the matched rows.
func (t *Tx) Query(ctx context.Context, query string, args ...any) (*ResultSet, error) {
	stmt, err := parseBound(query, args)
	if err != nil {
		return nil, err
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		return nil, fmt.Errorf("%w: Query expects a SELECT statement", ErrExec)
	}
	return t.execSelect(ctx, sel)
}

// execSelect executes a parsed SELECT inside the transaction. Queries with
// joins or a derived FROM subquery go through the generalized source pipeline,
// materializing every source through the write transaction so they see staged
// writes; a plain single-table query keeps the primary-scan path.
func (t *Tx) execSelect(ctx context.Context, sel *SelectStmt) (*ResultSet, error) {
	if len(sel.Joins) > 0 || sel.Subquery != nil {
		if sel.ForUpdate {
			return nil, fmt.Errorf("%w: FOR UPDATE is not supported with JOIN or FROM subqueries", ErrExec)
		}
		if hasWindows(sel.Items) {
			return nil, fmt.Errorf("%w: window functions are only supported on single-table queries", ErrExec)
		}
		return queryFrom(ctx, sel, nil, t)
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

	rows, err := t.scanRows(ctx, schema, sel.Alias, sel.Where)
	if err != nil {
		return nil, err
	}
	if sel.ForUpdate {
		// FOR UPDATE must observe the latest committed version of each locked
		// row, not this transaction's (possibly stale) snapshot. Returning the
		// snapshot value here silently loses concurrent updates in any
		// read-modify-write built on FOR UPDATE (e.g. a balance transfer).
		rows, err = t.lockAndRefreshRows(ctx, schema, sel.Alias, rows, sel.Where)
		if err != nil {
			return nil, err
		}
	}

	if isGrouped(sel) {
		return groupedResultSet(sel, rows, nil)
	}

	if needsSort, order := sortDecision(sel, schema); needsSort {
		sortRows(rows, order)
	}
	rows = applyOffsetLimit(rows, sel.Offset, sel.Limit)
	return projectRows(rows, expandProjection(sel.Items, schema), nil)
}

// table resolves a table name against the transaction's catalog.
func (t *Tx) table(name string) (*TableSchema, bool) { return t.catalog.Table(name) }

// scanQualifiedRows fully scans a table's primary index through the write
// transaction (read-your-writes), keyed by "alias.col".
func (t *Tx) scanQualifiedRows(ctx context.Context, schema *TableSchema, alias string) ([]Row, error) {
	pk, ok := schema.PrimaryIndex()
	if !ok {
		return nil, fmt.Errorf("%w: table %q has no primary index", ErrExec, schema.Name)
	}
	it, err := t.wtx.NewIterator(ctx, schema.Name, pk.Name, storage.IterOptions{})
	if err != nil {
		return nil, fmt.Errorf("%w: open iterator: %v", ErrExec, err)
	}
	defer func() { _ = it.Close() }()
	return scanQualifiedIterator(it, t.codec, schema, alias)
}

// derivedRows executes a FROM subquery within the transaction, so the derived
// table also sees staged writes.
func (t *Tx) derivedRows(ctx context.Context, sub *SelectStmt) (*ResultSet, error) {
	return t.execSelect(ctx, sub)
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

func (t *Tx) scanRows(ctx context.Context, schema *TableSchema, alias string, residual Expr) ([]Row, error) {
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
		row, err := decodeRow(t.codec, schema, alias, it.Value())
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

// lockAndRefreshRows takes the row lock on each candidate row (via
// GetForUpdate) and rebuilds the result from the latest committed version the
// lock exposes — so once a concurrent holder commits, this transaction sees
// its change instead of the stale snapshot. Rows deleted by a committed
// concurrent transaction drop out, and the residual predicate is re-evaluated
// against the fresh value so a row that no longer matches is excluded. This is
// the engine's READ COMMITTED-style FOR UPDATE: it does not re-scan for new
// phantom rows, only refreshes the rows the snapshot already matched.
func (t *Tx) lockAndRefreshRows(ctx context.Context, schema *TableSchema, alias string, rows []Row, residual Expr) ([]Row, error) {
	pk, _ := schema.PrimaryIndex()
	pkCol, _ := schema.Column(pk.Column)
	refreshed := make([]Row, 0, len(rows))
	for _, row := range rows {
		key := NormalizeValue(row[pk.Column], pkCol.Type)
		raw, found, err := t.wtx.GetForUpdate(ctx, schema.Name, pk.Name, key)
		if err != nil {
			return nil, fmt.Errorf("%w: lock row: %w", ErrExec, err)
		}
		if !found {
			continue // deleted by a concurrent committed transaction
		}
		fresh, err := decodeRow(t.codec, schema, alias, raw)
		if err != nil {
			return nil, err
		}
		if residual != nil {
			keep, err := Evaluate(residual, fresh)
			if err != nil {
				return nil, err
			}
			if !keep {
				continue
			}
		}
		refreshed = append(refreshed, fresh)
	}
	return refreshed, nil
}

// Exec parses and executes an INSERT, UPDATE, or DELETE within the
// transaction, returning the number of affected rows.
func (t *Tx) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	stmt, err := parseBound(query, args)
	if err != nil {
		return 0, err
	}
	return t.execStmt(ctx, stmt)
}

// execStmt dispatches a parsed data-modifying statement within the
// transaction. DDL and transaction-control statements are not transactional
// here and are rejected.
func (t *Tx) execStmt(ctx context.Context, stmt Statement) (int64, error) {
	switch s := stmt.(type) {
	case *InsertStmt:
		return t.execInsert(ctx, s)
	case *UpdateStmt:
		return t.execUpdate(ctx, s)
	case *DeleteStmt:
		return t.execDelete(ctx, s)
	default:
		return 0, fmt.Errorf("%w: only INSERT, UPDATE, and DELETE are allowed inside a transaction", ErrExec)
	}
}

func (t *Tx) execInsert(ctx context.Context, stmt *InsertStmt) (int64, error) {
	schema, ok := t.catalog.Table(stmt.Table)
	if !ok {
		return 0, fmt.Errorf("%w: unknown table %q", ErrExec, stmt.Table)
	}
	// Assign AUTO_INCREMENT values, then validate every row before staging any
	// write, so a malformed later row does not leave earlier rows staged in the
	// transaction.
	columns, rows, err := t.exec.resolveAutoIncrement(ctx, schema, stmt.Columns, stmt.Rows)
	if err != nil {
		return 0, err
	}
	docs, err := encodeInsertRows(schema, columns, rows)
	if err != nil {
		return 0, err
	}
	for _, d := range docs {
		if err := enforceRowIntegrity(ctx, t.catalog, schema, d.row, t.fkLookup()); err != nil {
			return 0, err
		}
	}
	for _, d := range docs {
		if err := t.wtx.WriteRow(ctx, stmt.Table, d.json, d.keys, true); err != nil {
			if ue := asUniqueViolation(err); ue != nil {
				return 0, ue
			}
			return 0, fmt.Errorf("%w: insert row: %w", ErrExec, err)
		}
	}
	return int64(len(docs)), nil
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
	if err := validateExprColumns(stmt.Where, schema, ""); err != nil {
		return 0, err
	}

	matches, err := t.matchingRaw(ctx, schema, stmt.Where)
	if err != nil {
		return 0, err
	}
	// Prepare and validate every updated row before staging any write, so a
	// constraint violation on a later row does not leave earlier rows staged.
	updates := make([]preparedUpdate, 0, len(matches))
	for _, raw := range matches {
		doc, err := rawToMap(t.codec, raw)
		if err != nil {
			return 0, err
		}
		typedRow, err := decodeRow(t.codec, schema, "", raw)
		if err != nil {
			return 0, err
		}
		if err := applyAssignments(schema, stmt.Assignments, doc, typedRow); err != nil {
			return 0, err
		}
		if err := enforceRowIntegrity(ctx, t.catalog, schema, typedRow, t.fkLookup()); err != nil {
			return 0, err
		}
		keys, err := keysFromMap(schema, doc)
		if err != nil {
			return 0, err
		}
		jsonDoc, err := json.Marshal(doc)
		if err != nil {
			return 0, fmt.Errorf("%w: encode document: %v", ErrExec, err)
		}
		updates = append(updates, preparedUpdate{json: string(jsonDoc), keys: keys})
	}

	var affected int64
	for _, u := range updates {
		if err := t.wtx.WriteRow(ctx, stmt.Table, u.json, u.keys, false); err != nil {
			if ue := asUniqueViolation(err); ue != nil {
				return 0, ue
			}
			return 0, fmt.Errorf("%w: upsert row: %w", ErrExec, err)
		}
		affected++
	}
	if affected > 0 {
		t.markGarbage(stmt.Table)
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
	if err := validateExprColumns(stmt.Where, schema, ""); err != nil {
		return 0, err
	}

	pkCol, _ := schema.Column(pk.Column)
	keys, err := t.matchingPrimaryKeys(ctx, schema, stmt.Where, pk.Column, pkCol.Type)
	if err != nil {
		return 0, err
	}
	if err := enforceDeleteRestrict(ctx, t.catalog, schema, keys, t.integrityScan()); err != nil {
		return 0, err
	}
	var affected int64
	for _, key := range keys {
		if err := t.wtx.Del(ctx, stmt.Table, pk.Name, key); err != nil {
			return 0, fmt.Errorf("%w: delete row: %v", ErrExec, err)
		}
		affected++
	}
	if affected > 0 {
		t.markGarbage(stmt.Table)
	}
	return affected, nil
}

// markGarbage records a table whose committed UPDATE/DELETE will leave dead
// heap space, to be vacuumed after Commit.
func (t *Tx) markGarbage(table string) {
	if t.garbage == nil {
		t.garbage = make(map[string]struct{})
	}
	t.garbage[table] = struct{}{}
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
			row, err := decodeRow(t.codec, schema, "", raw)
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
		row, err := decodeRow(t.codec, schema, "", it.Value())
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

// buildInsertDoc validates one INSERT row (a VALUES tuple positionally aligned
// with columns) and returns its document map, the index key map required to
// write it, and the typed row used for constraint validation.
func buildInsertDoc(schema *TableSchema, columns []string, values []Expr) (map[string]any, map[string]types.Comparable, Row, error) {
	if len(columns) != len(values) {
		return nil, nil, nil, fmt.Errorf("%w: %d columns but %d values", ErrExec, len(columns), len(values))
	}
	doc := make(map[string]any, len(columns))
	literals := make(map[string]*Literal, len(columns))
	for i, name := range columns {
		col, ok := schema.Column(name)
		if !ok {
			return nil, nil, nil, fmt.Errorf("%w: unknown column %q", ErrExec, name)
		}
		lit, ok := values[i].(*Literal)
		if !ok {
			return nil, nil, nil, fmt.Errorf("%w: value for %q is not a literal", ErrExec, name)
		}
		if lit.Kind == LitNull && col.NotNull {
			return nil, nil, nil, fmt.Errorf("%w: column %q is NOT NULL", ErrExec, name)
		}
		if _, err := ColumnValue(lit, col.Type); err != nil {
			return nil, nil, nil, fmt.Errorf("%w: %v", ErrExec, err)
		}
		doc[name] = literalToDocValue(lit, col.Type)
		literals[name] = lit
	}
	// Fill omitted columns from their defaults, and reject omitted NOT NULL
	// columns that have no default to fall back on.
	for _, col := range schema.Columns {
		if _, provided := literals[col.Name]; provided {
			continue
		}
		switch {
		case col.Default != nil && col.Default.Kind != LitNull:
			doc[col.Name] = literalToDocValue(col.Default, col.Type)
			literals[col.Name] = col.Default
		case col.NotNull:
			return nil, nil, nil, fmt.Errorf("%w: column %q is NOT NULL and has no default", ErrExec, col.Name)
		}
	}
	keys, err := keysForInsert(schema, literals)
	if err != nil {
		return nil, nil, nil, err
	}
	addCompositeKeyFields(doc, schema, keys)

	typed := make(Row, len(schema.Columns))
	for _, col := range schema.Columns {
		lit, ok := literals[col.Name]
		if !ok {
			typed[col.Name] = types.NullKey{}
			continue
		}
		v, err := ColumnValue(lit, col.Type)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("%w: %v", ErrExec, err)
		}
		typed[col.Name] = v
	}
	return doc, keys, typed, nil
}
