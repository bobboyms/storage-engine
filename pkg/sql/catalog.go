// Package sql implements a thin SQL layer on top of the storage engine. It
// translates a small SQL subset into the engine's native access paths (point
// lookups and ordered index scans) over registered table schemas.
//
// The layer is intentionally single-table: it relies on a Catalog describing
// each table's columns and indexes so that column references in a query can be
// resolved to engine index names and key types.
package sql

import (
	"errors"
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/storage"
)

// ErrInvalidSchema is returned when a TableSchema fails validation.
var ErrInvalidSchema = errors.New("sql: invalid table schema")

// ErrDuplicateTable is returned when a table with the same name is registered
// twice in a Catalog.
var ErrDuplicateTable = errors.New("sql: duplicate table")

// ErrDuplicateColumn is returned by ALTER TABLE ADD COLUMN when the target
// column name already exists in the table schema.
var ErrDuplicateColumn = errors.New("sql: duplicate column")

// ErrUniqueViolation is returned when a write would violate a UNIQUE
// constraint. The constraint surface (parsing, schema, persistence) exists now;
// enforcement that returns this error is added in a later increment.
var ErrUniqueViolation = errors.New("sql: unique constraint violation")

// ErrCheckViolation is returned when an INSERT or UPDATE produces a row whose
// CHECK constraint evaluates to false.
var ErrCheckViolation = errors.New("sql: check constraint violation")

// ErrForeignKeyViolation is returned when a write would break referential
// integrity: a child row referencing a missing parent, or a parent delete/drop
// while child rows still reference it.
var ErrForeignKeyViolation = errors.New("sql: foreign key constraint violation")

// Column describes a single column of a table together with the engine data
// type used to encode its values. NotNull rejects NULL values on INSERT and
// UPDATE; Default, when non-nil, is the literal stored when an INSERT omits
// the column. The literal is immutable after parse, so sharing the pointer
// across schema clones is safe.
type Column struct {
	Name    string
	Type    storage.DataType
	NotNull bool
	Default *Literal
}

// IndexDef describes an index. A single-column index serves Column; a composite
// index serves Columns (len >= 2) and leaves Column empty. Exactly one index per
// table must be marked Primary; its column is the table's primary key. A
// composite index is never primary.
type IndexDef struct {
	Name    string
	Column  string
	Primary bool
	Columns []string
	// Unique marks a UNIQUE constraint. The constraint is recorded and persisted
	// here; enforcement is added in a later increment.
	Unique bool
}

// composite reports whether idx spans more than one column.
func (idx IndexDef) composite() bool { return len(idx.Columns) > 1 }

// TableSchema describes the columns, indexes, and integrity constraints of a
// single table. Checks are boolean expressions every stored row must satisfy
// (NULL operands make a check pass, matching SQL's UNKNOWN semantics).
// ForeignKeys constrain column values to a parent table's primary key.
type TableSchema struct {
	Name        string
	Columns     []Column
	Indexes     []IndexDef
	Checks      []Expr
	ForeignKeys []ForeignKey
}

// Column returns the column with the given name and whether it exists.
func (s TableSchema) Column(name string) (Column, bool) {
	for _, c := range s.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// PrimaryIndex returns the table's primary index and whether one is defined.
func (s TableSchema) PrimaryIndex() (IndexDef, bool) {
	for _, idx := range s.Indexes {
		if idx.Primary {
			return idx, true
		}
	}
	return IndexDef{}, false
}

// IndexForColumn returns an index serving the given column and whether one
// exists. A primary index is preferred over a secondary index on the same
// column.
func (s TableSchema) IndexForColumn(column string) (IndexDef, bool) {
	var found IndexDef
	ok := false
	for _, idx := range s.Indexes {
		if idx.Column != column {
			continue
		}
		if idx.Primary {
			return idx, true
		}
		if !ok {
			found, ok = idx, true
		}
	}
	return found, ok
}

func (s TableSchema) validate() error {
	if s.Name == "" {
		return fmt.Errorf("%w: empty table name", ErrInvalidSchema)
	}
	if len(s.Columns) == 0 {
		return fmt.Errorf("%w: table %q has no columns", ErrInvalidSchema, s.Name)
	}

	seenCols := make(map[string]struct{}, len(s.Columns))
	for _, c := range s.Columns {
		if c.Name == "" {
			return fmt.Errorf("%w: table %q has an empty column name", ErrInvalidSchema, s.Name)
		}
		if _, dup := seenCols[c.Name]; dup {
			return fmt.Errorf("%w: duplicate column %q in table %q", ErrInvalidSchema, c.Name, s.Name)
		}
		seenCols[c.Name] = struct{}{}
		if c.Default != nil {
			if c.Default.Kind == LitNull {
				if c.NotNull {
					return fmt.Errorf("%w: column %q is NOT NULL but declares DEFAULT NULL", ErrInvalidSchema, c.Name)
				}
			} else if _, err := ColumnValue(c.Default, c.Type); err != nil {
				return fmt.Errorf("%w: column %q has a type-incompatible default: %v", ErrInvalidSchema, c.Name, err)
			}
		}
	}

	seenIdx := make(map[string]struct{}, len(s.Indexes))
	primaries := 0
	for _, idx := range s.Indexes {
		if idx.Name == "" {
			return fmt.Errorf("%w: table %q has an empty index name", ErrInvalidSchema, s.Name)
		}
		if _, dup := seenIdx[idx.Name]; dup {
			return fmt.Errorf("%w: duplicate index %q in table %q", ErrInvalidSchema, idx.Name, s.Name)
		}
		seenIdx[idx.Name] = struct{}{}
		if idx.composite() {
			if idx.Primary {
				return fmt.Errorf("%w: composite index %q cannot be primary", ErrInvalidSchema, idx.Name)
			}
			for _, col := range idx.Columns {
				if _, ok := seenCols[col]; !ok {
					return fmt.Errorf("%w: index %q references unknown column %q", ErrInvalidSchema, idx.Name, col)
				}
			}
		} else if _, ok := seenCols[idx.Column]; !ok {
			return fmt.Errorf("%w: index %q references unknown column %q", ErrInvalidSchema, idx.Name, idx.Column)
		}
		if idx.Primary {
			primaries++
		}
	}
	if primaries != 1 {
		return fmt.Errorf("%w: table %q must have exactly one primary index, found %d", ErrInvalidSchema, s.Name, primaries)
	}

	for _, chk := range s.Checks {
		if chk == nil {
			return fmt.Errorf("%w: table %q has a nil CHECK expression", ErrInvalidSchema, s.Name)
		}
		if containsSubquery(chk) {
			return fmt.Errorf("%w: CHECK %s: subqueries are not allowed in CHECK constraints", ErrInvalidSchema, chk.String())
		}
		if err := validateExprColumns(chk, &s, ""); err != nil {
			return fmt.Errorf("%w: CHECK %s: %v", ErrInvalidSchema, chk.String(), err)
		}
	}
	for _, fk := range s.ForeignKeys {
		if _, ok := s.Column(fk.Column); !ok {
			return fmt.Errorf("%w: foreign key on unknown column %q in table %q", ErrInvalidSchema, fk.Column, s.Name)
		}
		if fk.RefTable == "" || fk.RefColumn == "" {
			return fmt.Errorf("%w: foreign key on %q must name a referenced table and column", ErrInvalidSchema, fk.Column)
		}
	}
	return nil
}

// containsSubquery reports whether expr contains any subquery node.
func containsSubquery(expr Expr) bool {
	switch e := expr.(type) {
	case *ScalarSubquery, *InSubqueryExpr, *ExistsExpr:
		return true
	case *IsNullExpr:
		return containsSubquery(e.Operand)
	case *BinaryExpr:
		return containsSubquery(e.Left) || containsSubquery(e.Right)
	case *ArithExpr:
		return containsSubquery(e.Left) || containsSubquery(e.Right)
	case *FuncCall:
		for _, a := range e.Args {
			if containsSubquery(a) {
				return true
			}
		}
	case *CaseExpr:
		for _, w := range e.Whens {
			if containsSubquery(w.Cond) || containsSubquery(w.Then) {
				return true
			}
		}
		if e.Else != nil {
			return containsSubquery(e.Else)
		}
	}
	return false
}

// referencingForeignKeys returns every foreign key in the catalog that
// references the given table, paired with the child schema declaring it.
func (c *Catalog) referencingForeignKeys(table string) []fkReference {
	var refs []fkReference
	for _, ts := range c.tables {
		for _, fk := range ts.ForeignKeys {
			if fk.RefTable == table {
				refs = append(refs, fkReference{child: ts, fk: fk})
			}
		}
	}
	return refs
}

// fkReference pairs a child table schema with one of its foreign keys.
type fkReference struct {
	child *TableSchema
	fk    ForeignKey
}

// Catalog holds the registered table schemas for a SQL session.
type Catalog struct {
	tables map[string]*TableSchema
}

// NewCatalog returns an empty Catalog.
func NewCatalog() *Catalog {
	return &Catalog{tables: make(map[string]*TableSchema)}
}

// AddTable validates and registers a table schema. It returns ErrInvalidSchema
// when the schema is malformed and ErrDuplicateTable when a table with the same
// name is already registered.
func (c *Catalog) AddTable(schema TableSchema) error {
	if err := schema.validate(); err != nil {
		return err
	}
	if _, exists := c.tables[schema.Name]; exists {
		return fmt.Errorf("%w: %q", ErrDuplicateTable, schema.Name)
	}
	stored := schema
	c.tables[schema.Name] = &stored
	return nil
}

// ReplaceTable validates and replaces an already-registered table schema. It
// returns ErrInvalidSchema when the schema is malformed and ErrExec when no
// table with that name exists. Used by ALTER TABLE to swap in an evolved schema.
func (c *Catalog) ReplaceTable(schema TableSchema) error {
	if err := schema.validate(); err != nil {
		return err
	}
	if _, exists := c.tables[schema.Name]; !exists {
		return fmt.Errorf("%w: unknown table %q", ErrExec, schema.Name)
	}
	stored := schema
	c.tables[schema.Name] = &stored
	return nil
}

// RemoveTable unregisters a table schema. Removing an unknown name is a no-op,
// matching DROP TABLE IF EXISTS semantics; callers that need to report unknown
// tables check existence first.
func (c *Catalog) RemoveTable(name string) {
	delete(c.tables, name)
}

// Table returns the schema registered under name and whether it exists.
func (c *Catalog) Table(name string) (*TableSchema, bool) {
	ts, ok := c.tables[name]
	return ts, ok
}

// snapshot returns a deep copy of the registered schemas, used to restore the
// catalog if a multi-statement migration fails partway through.
func (c *Catalog) snapshot() map[string]*TableSchema {
	m := make(map[string]*TableSchema, len(c.tables))
	for name, ts := range c.tables {
		cp := cloneSchema(*ts)
		m[name] = &cp
	}
	return m
}

// restore replaces the catalog's schemas with a previously taken snapshot.
func (c *Catalog) restore(snapshot map[string]*TableSchema) {
	c.tables = snapshot
}
