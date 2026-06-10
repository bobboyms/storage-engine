package sql

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/bobboyms/storage-engine/pkg/types"
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
	Name    string            `json:"name"`
	Type    string            `json:"type"`
	NotNull bool              `json:"not_null,omitempty"`
	Default *persistedDefault `json:"default,omitempty"`
}

// persistedDefault stores a column DEFAULT as a literal kind plus its value
// rendered as plain text (no SQL quoting), so values that contain quotes
// round-trip safely.
type persistedDefault struct {
	Kind  string `json:"kind"`
	Value string `json:"value,omitempty"`
}

// defaultKindNames maps literal kinds to their persisted names. The names are
// part of the on-disk catalog format; do not rename them.
var defaultKindNames = map[LiteralKind]string{
	LitInt: "int", LitFloat: "float", LitString: "string", LitBool: "bool",
	LitNull: "null", LitUUID: "uuid", LitDate: "date", LitDecimal: "decimal",
}

func toPersistedDefault(l *Literal) *persistedDefault {
	if l == nil {
		return nil
	}
	pd := &persistedDefault{Kind: defaultKindNames[l.Kind]}
	switch l.Kind {
	case LitInt:
		pd.Value = strconv.FormatInt(l.Int, 10)
	case LitFloat:
		pd.Value = strconv.FormatFloat(l.Float, 'g', -1, 64)
	case LitString:
		pd.Value = l.Str
	case LitBool:
		pd.Value = strconv.FormatBool(l.Bool)
	case LitNull:
		// no value
	case LitUUID:
		pd.Value = l.UUID.String()
	case LitDate:
		pd.Value = l.Date.String()
	case LitDecimal:
		pd.Value = l.Dec.String()
	}
	return pd
}

func fromPersistedDefault(pd *persistedDefault) (*Literal, error) {
	if pd == nil {
		return nil, nil
	}
	switch pd.Kind {
	case "int":
		n, err := strconv.ParseInt(pd.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid persisted int default %q", ErrInvalidSchema, pd.Value)
		}
		return &Literal{Kind: LitInt, Int: n}, nil
	case "float":
		f, err := strconv.ParseFloat(pd.Value, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid persisted float default %q", ErrInvalidSchema, pd.Value)
		}
		return &Literal{Kind: LitFloat, Float: f}, nil
	case "string":
		return &Literal{Kind: LitString, Str: pd.Value}, nil
	case "bool":
		b, err := strconv.ParseBool(pd.Value)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid persisted bool default %q", ErrInvalidSchema, pd.Value)
		}
		return &Literal{Kind: LitBool, Bool: b}, nil
	case "null":
		return &Literal{Kind: LitNull}, nil
	case "uuid":
		k, err := types.ParseUUID(pd.Value)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid persisted uuid default %q", ErrInvalidSchema, pd.Value)
		}
		return &Literal{Kind: LitUUID, UUID: k}, nil
	case "date":
		k, err := types.ParseDateOnly(pd.Value)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid persisted date default %q", ErrInvalidSchema, pd.Value)
		}
		return &Literal{Kind: LitDate, Date: k}, nil
	case "decimal":
		k, err := types.ParseDecimal(pd.Value)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid persisted decimal default %q", ErrInvalidSchema, pd.Value)
		}
		return &Literal{Kind: LitDecimal, Dec: k}, nil
	default:
		return nil, fmt.Errorf("%w: unknown persisted default kind %q", ErrInvalidSchema, pd.Kind)
	}
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
		pt.Columns = append(pt.Columns, persistedColumn{
			Name:    c.Name,
			Type:    typeNameForData(c.Type),
			NotNull: c.NotNull,
			Default: toPersistedDefault(c.Default),
		})
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
		def, err := fromPersistedDefault(c.Default)
		if err != nil {
			return TableSchema{}, err
		}
		s.Columns = append(s.Columns, Column{Name: c.Name, Type: dt, NotNull: c.NotNull, Default: def})
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
