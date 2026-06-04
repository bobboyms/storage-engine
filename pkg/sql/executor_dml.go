package sql

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/codec"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// Exec parses and executes a data-modifying statement (INSERT, UPDATE, or
// DELETE) and returns the number of affected rows. Positional "?" placeholders
// in query are bound, in order, to args; passing a different number of args
// than placeholders is an error wrapping ErrBind.
func (e *Executor) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	stmt, err := parseBound(query, args)
	if err != nil {
		return 0, err
	}
	switch s := stmt.(type) {
	case *InsertStmt:
		return e.execInsert(ctx, s)
	case *UpdateStmt:
		return e.execUpdate(ctx, s)
	case *DeleteStmt:
		return e.execDelete(ctx, s)
	case *CreateTableStmt:
		return e.execCreateTable(s)
	case *AlterTableStmt:
		return e.execAlterTable(s)
	default:
		return 0, fmt.Errorf("%w: Exec expects INSERT, UPDATE, DELETE, CREATE TABLE, or ALTER TABLE", ErrExec)
	}
}

func (e *Executor) execInsert(ctx context.Context, stmt *InsertStmt) (int64, error) {
	schema, ok := e.catalog.Table(stmt.Table)
	if !ok {
		return 0, fmt.Errorf("%w: unknown table %q", ErrExec, stmt.Table)
	}
	if len(stmt.Columns) != len(stmt.Values) {
		return 0, fmt.Errorf("%w: %d columns but %d values", ErrExec, len(stmt.Columns), len(stmt.Values))
	}

	doc := make(map[string]any, len(stmt.Columns))
	values := make(map[string]*Literal, len(stmt.Columns))
	for i, name := range stmt.Columns {
		col, ok := schema.Column(name)
		if !ok {
			return 0, fmt.Errorf("%w: unknown column %q", ErrExec, name)
		}
		lit, ok := stmt.Values[i].(*Literal)
		if !ok {
			return 0, fmt.Errorf("%w: value for %q is not a literal", ErrExec, name)
		}
		// Validate type compatibility up front.
		if _, err := ColumnValue(lit, col.Type); err != nil {
			return 0, fmt.Errorf("%w: %v", ErrExec, err)
		}
		doc[name] = literalToGo(lit)
		values[name] = lit
	}

	keys, err := keysForInsert(schema, values)
	if err != nil {
		return 0, err
	}
	addCompositeKeyFields(doc, schema, keys)

	jsonDoc, err := json.Marshal(doc)
	if err != nil {
		return 0, fmt.Errorf("%w: encode document: %v", ErrExec, err)
	}
	if err := e.engine.InsertRow(ctx, stmt.Table, string(jsonDoc), keys); err != nil {
		return 0, fmt.Errorf("%w: insert row: %v", ErrExec, err)
	}
	return 1, nil
}

// keysForInsert builds the index-name -> key map required by InsertRow. Every
// index column must be provided by the statement so all indexes stay in sync.
func keysForInsert(schema *TableSchema, values map[string]*Literal) (map[string]types.Comparable, error) {
	keys := make(map[string]types.Comparable, len(schema.Indexes))
	for _, idx := range schema.Indexes {
		if idx.composite() {
			key, err := compositeKeyForValues(schema, idx, values)
			if err != nil {
				return nil, err
			}
			keys[idx.Name] = key
			continue
		}
		lit, ok := values[idx.Column]
		if !ok {
			return nil, fmt.Errorf("%w: INSERT must provide indexed column %q", ErrExec, idx.Column)
		}
		col, _ := schema.Column(idx.Column)
		key, err := ColumnValue(lit, col.Type)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrExec, err)
		}
		keys[idx.Name] = key
	}
	return keys, nil
}

func (e *Executor) execUpdate(ctx context.Context, stmt *UpdateStmt) (int64, error) {
	schema, ok := e.catalog.Table(stmt.Table)
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

	matches, err := e.matchingRaw(ctx, schema, stmt.Where)
	if err != nil {
		return 0, err
	}

	var affected int64
	for _, raw := range matches {
		doc, err := rawToMap(e.codec, raw)
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
		addCompositeKeyFields(doc, schema, keys) // refresh synthetic key after assignments
		jsonDoc, err := json.Marshal(doc)
		if err != nil {
			return 0, fmt.Errorf("%w: encode document: %v", ErrExec, err)
		}
		if err := e.engine.UpsertRow(ctx, stmt.Table, string(jsonDoc), keys); err != nil {
			return 0, fmt.Errorf("%w: upsert row: %v", ErrExec, err)
		}
		affected++
	}
	if affected > 0 {
		e.markGarbage(stmt.Table) // UPDATE tombstones the old version
	}
	return affected, nil
}

func validateAssignments(stmt *UpdateStmt, schema *TableSchema, pk IndexDef) error {
	for _, a := range stmt.Assignments {
		col, ok := schema.Column(a.Column)
		if !ok {
			return fmt.Errorf("%w: unknown column %q", ErrExec, a.Column)
		}
		if a.Column == pk.Column {
			return fmt.Errorf("%w: cannot update primary key column %q", ErrExec, a.Column)
		}
		lit, ok := a.Value.(*Literal)
		if !ok {
			return fmt.Errorf("%w: assignment to %q is not a literal", ErrExec, a.Column)
		}
		if _, err := ColumnValue(lit, col.Type); err != nil {
			return fmt.Errorf("%w: %v", ErrExec, err)
		}
	}
	return nil
}

func (e *Executor) execDelete(ctx context.Context, stmt *DeleteStmt) (int64, error) {
	schema, ok := e.catalog.Table(stmt.Table)
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
	keys, err := e.matchingPrimaryKeys(ctx, schema, stmt.Where, pk.Column, pkCol.Type)
	if err != nil {
		return 0, err
	}

	var affected int64
	for _, key := range keys {
		removed, err := e.engine.Del(ctx, stmt.Table, pk.Name, key)
		if err != nil {
			return 0, fmt.Errorf("%w: delete row: %v", ErrExec, err)
		}
		if removed {
			affected++
		}
	}
	if affected > 0 {
		e.markGarbage(stmt.Table) // DELETE tombstones the row's heap version
	}
	return affected, nil
}

// matchingRaw scans the table (full primary scan) and returns the raw bytes of
// every row matching the WHERE predicate.
func (e *Executor) matchingRaw(ctx context.Context, schema *TableSchema, where Expr) ([][]byte, error) {
	pk, _ := schema.PrimaryIndex()
	it, err := e.engine.NewIterator(ctx, schema.Name, pk.Name, storage.IterOptions{})
	if err != nil {
		return nil, fmt.Errorf("%w: open iterator: %v", ErrExec, err)
	}
	defer func() { _ = it.Close() }()

	var out [][]byte
	for it.Next() {
		raw := append([]byte(nil), it.Value()...)
		keep, err := e.rowMatches(schema, raw, where)
		if err != nil {
			return nil, err
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

// matchingPrimaryKeys scans the table and returns the primary-key value of
// every row matching the WHERE predicate.
func (e *Executor) matchingPrimaryKeys(ctx context.Context, schema *TableSchema, where Expr, pkColumn string, pkType storage.DataType) ([]types.Comparable, error) {
	pk, _ := schema.PrimaryIndex()
	it, err := e.engine.NewIterator(ctx, schema.Name, pk.Name, storage.IterOptions{})
	if err != nil {
		return nil, fmt.Errorf("%w: open iterator: %v", ErrExec, err)
	}
	defer func() { _ = it.Close() }()

	var keys []types.Comparable
	for it.Next() {
		raw := it.Value()
		row, err := decodeRow(e.codec, schema, "", raw)
		if err != nil {
			return nil, err
		}
		keep := true
		if where != nil {
			keep, err = Evaluate(where, row)
			if err != nil {
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

func (e *Executor) rowMatches(schema *TableSchema, raw []byte, where Expr) (bool, error) {
	if where == nil {
		return true, nil
	}
	row, err := decodeRow(e.codec, schema, "", raw)
	if err != nil {
		return false, err
	}
	return Evaluate(where, row)
}

func rawToMap(c codec.Codec, raw []byte) (map[string]any, error) {
	text, err := c.DecodeToText(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: decode row: %v", ErrExec, err)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(text), &doc); err != nil {
		return nil, fmt.Errorf("%w: parse row json: %v", ErrExec, err)
	}
	return doc, nil
}

// keysFromMap builds the index key map from a decoded document map for
// UpsertRow, converting each indexed column's value to its declared key type.
func keysFromMap(schema *TableSchema, doc map[string]any) (map[string]types.Comparable, error) {
	keys := make(map[string]types.Comparable, len(schema.Indexes))
	for _, idx := range schema.Indexes {
		if idx.composite() {
			key, err := compositeKeyFromDoc(schema, idx, doc)
			if err != nil {
				return nil, fmt.Errorf("%w: index %q: %v", ErrExec, idx.Name, err)
			}
			keys[idx.Name] = key
			continue
		}
		col, _ := schema.Column(idx.Column)
		key, err := jsonValueToKey(doc[idx.Column], col.Type)
		if err != nil {
			return nil, fmt.Errorf("%w: index %q: %v", ErrExec, idx.Name, err)
		}
		keys[idx.Name] = key
	}
	return keys, nil
}

func literalToGo(lit *Literal) any {
	switch lit.Kind {
	case LitInt:
		return lit.Int
	case LitFloat:
		return lit.Float
	case LitString:
		return lit.Str
	case LitBool:
		return lit.Bool
	default:
		return nil
	}
}

// jsonValueToKey converts a document value to a column's key type. Numbers may
// arrive as float64 (decoded from JSON) or int64 (set by an UPDATE assignment),
// so both are accepted for numeric columns.
func jsonValueToKey(v any, dt storage.DataType) (types.Comparable, error) {
	if v == nil {
		return types.NullKey{}, nil
	}
	switch dt {
	case storage.TypeInt:
		if n, ok := asInt64(v); ok {
			return types.IntKey(n), nil
		}
	case storage.TypeFloat:
		if f, ok := asFloat64(v); ok {
			return types.FloatKey(f), nil
		}
	case storage.TypeVarchar:
		if s, ok := v.(string); ok {
			return types.VarcharKey(s), nil
		}
	case storage.TypeBoolean:
		if b, ok := v.(bool); ok {
			return types.BoolKey(b), nil
		}
	}
	return nil, fmt.Errorf("%w: value %v is not compatible with column type %s", ErrValue, v, dt)
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	default:
		return 0, false
	}
}

func asFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	default:
		return 0, false
	}
}
