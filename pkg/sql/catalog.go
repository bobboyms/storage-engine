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

// Column describes a single column of a table together with the engine data
// type used to encode its values.
type Column struct {
	Name string
	Type storage.DataType
}

// IndexDef describes an index serving a column. Exactly one index per table
// must be marked Primary; its column is the table's primary key.
type IndexDef struct {
	Name    string
	Column  string
	Primary bool
}

// TableSchema describes the columns and indexes of a single table.
type TableSchema struct {
	Name    string
	Columns []Column
	Indexes []IndexDef
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
		if _, ok := seenCols[idx.Column]; !ok {
			return fmt.Errorf("%w: index %q references unknown column %q", ErrInvalidSchema, idx.Name, idx.Column)
		}
		if idx.Primary {
			primaries++
		}
	}
	if primaries != 1 {
		return fmt.Errorf("%w: table %q must have exactly one primary index, found %d", ErrInvalidSchema, s.Name, primaries)
	}
	return nil
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
