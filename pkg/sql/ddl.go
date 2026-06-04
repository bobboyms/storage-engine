package sql

import (
	"strings"

	"github.com/bobboyms/storage-engine/pkg/storage"
)

// sqlTypeNames maps SQL type keywords (uppercased) to engine data types.
var sqlTypeNames = map[string]storage.DataType{
	"INT":       storage.TypeInt,
	"INTEGER":   storage.TypeInt,
	"BIGINT":    storage.TypeInt,
	"VARCHAR":   storage.TypeVarchar,
	"TEXT":      storage.TypeVarchar,
	"STRING":    storage.TypeVarchar,
	"BOOL":      storage.TypeBoolean,
	"BOOLEAN":   storage.TypeBoolean,
	"FLOAT":     storage.TypeFloat,
	"DOUBLE":    storage.TypeFloat,
	"REAL":      storage.TypeFloat,
	"DATE":      storage.TypeDate,
	"TIMESTAMP": storage.TypeDate,
	"DATEONLY":  storage.TypeDateOnly,
	"BYTES":     storage.TypeBytes,
	"BLOB":      storage.TypeBytes,
	"UUID":      storage.TypeUUID,
	"DECIMAL":   storage.TypeDecimal,
	"NUMERIC":   storage.TypeDecimal,
}

// dataTypeForName resolves a SQL type name to an engine data type.
func dataTypeForName(name string) (storage.DataType, bool) {
	dt, ok := sqlTypeNames[strings.ToUpper(name)]
	return dt, ok
}
