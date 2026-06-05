package sql

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// schemaFileName is the on-disk catalog persisted in a database directory.
const schemaFileName = "schema.json"

// CatalogFormatVersion is the on-disk format version of the catalog
// (schema.json). It is independent of the library's API version and is bumped
// only when the persisted catalog layout or semantics change in a way that
// needs a migration. Additive changes (new optional fields) keep the same
// version because old and new readers stay compatible.
const CatalogFormatVersion = 1

// ErrUnsupportedCatalogVersion is returned when a catalog file declares a
// format version newer than this build supports (e.g. opening data written by
// a future engine). Restoring or opening such data is refused rather than
// risking a misread.
var ErrUnsupportedCatalogVersion = errors.New("sql: unsupported catalog format version")

// persistedCatalog is the versioned envelope wrapping the persisted tables.
type persistedCatalog struct {
	FormatVersion int              `json:"format_version"`
	Tables        []persistedTable `json:"tables"`
}

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
	tables, version, err := decodeCatalog(data)
	if err != nil {
		return nil, err
	}
	// Upgrade an older on-disk catalog to the current format before use. The
	// migrated tables are re-stamped to the current version on the next schema
	// write (any DDL), so a read-only open leaves the file untouched.
	tables, err = migrateCatalog(tables, version, CatalogFormatVersion, catalogMigrations)
	if err != nil {
		return nil, err
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

// decodeCatalog parses the catalog file, returning the tables and the on-disk
// format version. It accepts either the current versioned envelope
// ({"format_version":N,"tables":[...]}) or the legacy bare array of tables
// (treated as format version 1) so pre-envelope databases keep opening. A
// version newer than this build supports is refused.
func decodeCatalog(data []byte) ([]persistedTable, int, error) {
	if trimmed := bytes.TrimLeft(data, " \t\r\n"); len(trimmed) > 0 && trimmed[0] == '[' {
		var tables []persistedTable
		if err := json.Unmarshal(data, &tables); err != nil {
			return nil, 0, fmt.Errorf("sql: parse schema file: %w", err)
		}
		return tables, 1, nil
	}

	var cat persistedCatalog
	if err := json.Unmarshal(data, &cat); err != nil {
		return nil, 0, fmt.Errorf("sql: parse schema file: %w", err)
	}
	if cat.FormatVersion > CatalogFormatVersion {
		return nil, 0, fmt.Errorf("%w: file is v%d, this build supports up to v%d", ErrUnsupportedCatalogVersion, cat.FormatVersion, CatalogFormatVersion)
	}
	version := cat.FormatVersion
	if version == 0 {
		version = 1 // an envelope without the field predates versioning.
	}
	return cat.Tables, version, nil
}

// saveSchemas writes the table schemas to dir atomically (temp file + rename).
func saveSchemas(dir string, schemas []TableSchema) error {
	cat := persistedCatalog{FormatVersion: CatalogFormatVersion}
	for _, s := range schemas {
		cat.Tables = append(cat.Tables, toPersisted(s))
	}
	data, err := json.MarshalIndent(cat, "", "  ")
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
