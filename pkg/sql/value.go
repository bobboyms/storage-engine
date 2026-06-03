package sql

import (
	"errors"
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// ErrValue is the sentinel wrapped when a literal cannot be converted to the
// engine key type of a column.
var ErrValue = errors.New("sql: value conversion error")

// ColumnValue converts a parsed literal into the types.Comparable key matching
// the column's engine data type. A NULL literal maps to types.NullKey for any
// column type. Conversions that do not match the column type (or target an
// unsupported type) return an error wrapping ErrValue.
func ColumnValue(lit *Literal, dt storage.DataType) (types.Comparable, error) {
	if lit.Kind == LitNull {
		return types.NullKey{}, nil
	}
	switch dt {
	case storage.TypeInt:
		if lit.Kind == LitInt {
			return types.IntKey(lit.Int), nil
		}
	case storage.TypeVarchar:
		if lit.Kind == LitString {
			return types.VarcharKey(lit.Str), nil
		}
	case storage.TypeBoolean:
		if lit.Kind == LitBool {
			return types.BoolKey(lit.Bool), nil
		}
	case storage.TypeFloat:
		switch lit.Kind {
		case LitFloat:
			return types.FloatKey(lit.Float), nil
		case LitInt:
			return types.FloatKey(float64(lit.Int)), nil
		}
	}
	return nil, fmt.Errorf("%w: literal %s is not compatible with column type %s", ErrValue, lit.String(), dt)
}
