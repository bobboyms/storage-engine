package sql

import (
	"fmt"
	"sort"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// Introspection API.
//
// These methods expose the catalog as a first-class contract instead of
// relying on the incidental ResultSet.Columns produced by a SELECT * LIMIT 0.
// They read from the in-memory catalog, which is populated in both execution
// modes (NewExecutor and OpenDatabase), so they work regardless of how the
// Executor was constructed. Returned schemas and slices are copies, so callers
// may mutate them without affecting the catalog.

// Tables returns the names of all registered tables, sorted lexicographically
// for a deterministic contract.
func (e *Executor) Tables() []string {
	names := make([]string, 0, len(e.catalog.tables))
	for name := range e.catalog.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Describe returns a copy of the schema (columns and indexes) registered under
// table, and whether such a table exists.
func (e *Executor) Describe(table string) (TableSchema, bool) {
	schema, ok := e.catalog.Table(table)
	if !ok {
		return TableSchema{}, false
	}
	return cloneSchema(*schema), true
}

// Columns returns a copy of the column list for table, in declaration order,
// and whether the table exists.
func (e *Executor) Columns(table string) ([]Column, bool) {
	schema, ok := e.catalog.Table(table)
	if !ok {
		return nil, false
	}
	cols := make([]Column, len(schema.Columns))
	copy(cols, schema.Columns)
	return cols, true
}

// execDescribe backs the DESCRIBE statement. It returns one row per column with
// the column name, its canonical SQL type, and a key marker ("PRI" for the
// primary key, "MUL" for a secondary-indexed column, empty otherwise).
func (e *Executor) execDescribe(stmt *DescribeStmt) (*ResultSet, error) {
	schema, ok := e.catalog.Table(stmt.Table)
	if !ok {
		return nil, fmt.Errorf("%w: unknown table %q", ErrExec, stmt.Table)
	}
	rs := &ResultSet{
		Columns: []string{"column", "type", "key"},
		Rows:    make([][]types.Comparable, 0, len(schema.Columns)),
	}
	for _, col := range schema.Columns {
		rs.Rows = append(rs.Rows, []types.Comparable{
			types.VarcharKey(col.Name),
			types.VarcharKey(typeNameForData(col.Type)),
			types.VarcharKey(keyMarker(schema, col.Name)),
		})
	}
	return rs, nil
}

// keyMarker classifies how column participates in the table's indexes.
func keyMarker(schema *TableSchema, column string) string {
	if pk, ok := schema.PrimaryIndex(); ok && pk.Column == column {
		return "PRI"
	}
	if _, ok := schema.IndexForColumn(column); ok {
		return "MUL"
	}
	return ""
}
