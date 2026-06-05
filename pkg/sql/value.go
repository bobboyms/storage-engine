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
	case storage.TypeUUID:
		switch lit.Kind {
		case LitUUID:
			return lit.UUID, nil
		case LitString:
			k, err := types.ParseUUID(lit.Str)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrValue, err)
			}
			return k, nil
		}
	case storage.TypeDateOnly:
		switch lit.Kind {
		case LitDate:
			return lit.Date, nil
		case LitString:
			k, err := types.ParseDateOnly(lit.Str)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrValue, err)
			}
			return k, nil
		}
	case storage.TypeDecimal:
		switch lit.Kind {
		case LitDecimal:
			return lit.Dec, nil
		case LitString:
			k, err := types.ParseDecimal(lit.Str)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrValue, err)
			}
			return k, nil
		case LitInt:
			return types.NewDecimal(lit.Int, 0), nil
		}
	}
	return nil, fmt.Errorf("%w: literal %s is not compatible with column type %s", ErrValue, lit.String(), dt)
}

// NormalizeValue coerces a decoded value to the canonical key type for a
// column's declared engine data type. Codecs may decode JSON numbers either as
// integers or floats, so normalizing here keeps comparisons against typed
// literals consistent. Values that already match (or cannot be coerced) are
// returned unchanged; NULL is always preserved.
func NormalizeValue(v types.Comparable, dt storage.DataType) types.Comparable {
	if isNull(v) {
		return v
	}
	switch dt {
	case storage.TypeInt:
		switch n := v.(type) {
		case types.IntKey:
			return n
		case types.FloatKey:
			return types.IntKey(int64(n))
		}
	case storage.TypeFloat:
		switch n := v.(type) {
		case types.FloatKey:
			return n
		case types.IntKey:
			return types.FloatKey(float64(n))
		}
	}
	return v
}
