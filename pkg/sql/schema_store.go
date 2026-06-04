package sql

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// schemaFileName is the on-disk catalog persisted in a database directory.
const schemaFileName = "schema.json"

// persistedColumn / persistedIndex / persistedTable are the JSON-serializable
// forms of a table schema. Types are stored by name so the file stays readable
// and stable against engine enum reordering.
type persistedColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type persistedIndex struct {
	Name    string   `json:"name"`
	Column  string   `json:"column"`
	Primary bool     `json:"primary"`
	Columns []string `json:"columns,omitempty"`
	Unique  bool     `json:"unique,omitempty"`
}

type persistedTable struct {
	Name    string            `json:"name"`
	Columns []persistedColumn `json:"columns"`
	Indexes []persistedIndex  `json:"indexes"`
}

func toPersisted(s TableSchema) persistedTable {
	pt := persistedTable{Name: s.Name}
	for _, c := range s.Columns {
		pt.Columns = append(pt.Columns, persistedColumn{Name: c.Name, Type: typeNameForData(c.Type)})
	}
	for _, idx := range s.Indexes {
		pt.Indexes = append(pt.Indexes, persistedIndex(idx))
	}
	return pt
}

func fromPersisted(pt persistedTable) (TableSchema, error) {
	s := TableSchema{Name: pt.Name}
	for _, c := range pt.Columns {
		dt, ok := dataTypeForName(c.Type)
		if !ok {
			return TableSchema{}, fmt.Errorf("%w: column %q has unknown persisted type %q", ErrInvalidSchema, c.Name, c.Type)
		}
		s.Columns = append(s.Columns, Column{Name: c.Name, Type: dt})
	}
	for _, idx := range pt.Indexes {
		s.Indexes = append(s.Indexes, IndexDef(idx))
	}
	return s, nil
}

// loadSchemas reads the persisted table schemas from dir. A missing file yields
// no schemas (a fresh database).
func loadSchemas(dir string) ([]TableSchema, error) {
	path := filepath.Join(dir, schemaFileName)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sql: read schema file: %w", err)
	}
	var tables []persistedTable
	if err := json.Unmarshal(data, &tables); err != nil {
		return nil, fmt.Errorf("sql: parse schema file: %w", err)
	}
	schemas := make([]TableSchema, 0, len(tables))
	for _, pt := range tables {
		s, err := fromPersisted(pt)
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, s)
	}
	return schemas, nil
}

// saveSchemas writes the table schemas to dir atomically (temp file + rename).
func saveSchemas(dir string, schemas []TableSchema) error {
	tables := make([]persistedTable, 0, len(schemas))
	for _, s := range schemas {
		tables = append(tables, toPersisted(s))
	}
	data, err := json.MarshalIndent(tables, "", "  ")
	if err != nil {
		return fmt.Errorf("sql: encode schema file: %w", err)
	}
	tmp := filepath.Join(dir, schemaFileName+".tmp")
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("sql: write schema file: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, schemaFileName)); err != nil {
		return fmt.Errorf("sql: commit schema file: %w", err)
	}
	return nil
}
