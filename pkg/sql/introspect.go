package sql

import "sort"

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
