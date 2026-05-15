package v2

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// VariableKeyCodec is the companion interface for variable-size keys
// (currently VarcharKey). It uses byte slices instead of uint64.
// BTreeV2 selects the page layout through the codec type.
type VariableKeyCodec interface {
	// Encode serializes k into bytes. Size varies.
	Encode(k types.Comparable) ([]byte, error)

	// Decode reverses Encode.
	Decode(b []byte) types.Comparable

	// Compare returns semantic ordering (-1/0/1).
	Compare(a, b []byte) int
}

// VarcharKeyCodec serializes strings as raw UTF-8 bytes. The slot directory
// stores the size, so no length prefix is needed. Ordering is bytewise.
type VarcharKeyCodec struct{}

func (VarcharKeyCodec) Encode(k types.Comparable) ([]byte, error) {
	v, ok := k.(types.VarcharKey)
	if !ok {
		return nil, fmt.Errorf("%w: VarcharKeyCodec expected types.VarcharKey, got %T", types.ErrIncompatibleComparableTypes, k)
	}
	return []byte(string(v)), nil
}

func (VarcharKeyCodec) Decode(b []byte) types.Comparable {
	return types.VarcharKey(string(b))
}

func (VarcharKeyCodec) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// KeyCodec abstracts key encoding, decoding, and comparison for B+ tree v2.
//
// Fixed-size keys are stored in 8 bytes on page (uint64). The codec converts
// between types.Comparable and uint64 and owns semantic comparison. Direct
// uint64 comparison is not correct for:
//   - negative IntKey values, whose sign bit would sort after positives
//   - FloatKey bit patterns, which do not match numeric ordering
//
// Variable-size keys such as VarcharKey use VariableKeyCodec and the variable
// node-page layout.
type KeyCodec interface {
	// Encode converts k to its 8-byte binary representation.
	Encode(k types.Comparable) (uint64, error)

	// Decode reverses Encode.
	Decode(u uint64) types.Comparable

	// Compare returns -1/0/1 for the semantic order of the Comparable values
	// represented by a and b.
	Compare(a, b uint64) int
}

// IntKeyCodec stores IntKey as uint64 while preserving int64 bits.

type IntKeyCodec struct{}

func (IntKeyCodec) Encode(k types.Comparable) (uint64, error) {
	v, ok := k.(types.IntKey)
	if !ok {
		return 0, fmt.Errorf("%w: IntKeyCodec expected types.IntKey, got %T", types.ErrIncompatibleComparableTypes, k)
	}
	return uint64(int64(v)), nil //nolint:gosec // preserve bit pattern int64 -> uint64
}

func (IntKeyCodec) Decode(u uint64) types.Comparable {
	return types.IntKey(int64(u)) //nolint:gosec // preserve bit pattern uint64 -> int64
}

func (IntKeyCodec) Compare(a, b uint64) int {
	ai, bi := int64(a), int64(b) //nolint:gosec // bit-pattern preserving cast for signed comparison
	if ai < bi {
		return -1
	}
	if ai > bi {
		return 1
	}
	return 0
}

// FloatKeyCodec stores FloatKey as IEEE 754 bits and compares as float64.

type FloatKeyCodec struct{}

func (FloatKeyCodec) Encode(k types.Comparable) (uint64, error) {
	v, ok := k.(types.FloatKey)
	if !ok {
		return 0, fmt.Errorf("%w: FloatKeyCodec expected types.FloatKey, got %T", types.ErrIncompatibleComparableTypes, k)
	}
	return math.Float64bits(float64(v)), nil
}

func (FloatKeyCodec) Decode(u uint64) types.Comparable {
	return types.FloatKey(math.Float64frombits(u))
}

func (FloatKeyCodec) Compare(a, b uint64) int {
	af, bf := math.Float64frombits(a), math.Float64frombits(b)
	if af < bf {
		return -1
	}
	if af > bf {
		return 1
	}
	return 0
}

// BoolKeyCodec stores BoolKey as 0/1.

type BoolKeyCodec struct{}

func (BoolKeyCodec) Encode(k types.Comparable) (uint64, error) {
	v, ok := k.(types.BoolKey)
	if !ok {
		return 0, fmt.Errorf("%w: BoolKeyCodec expected types.BoolKey, got %T", types.ErrIncompatibleComparableTypes, k)
	}
	if bool(v) {
		return 1, nil
	}
	return 0, nil
}

func (BoolKeyCodec) Decode(u uint64) types.Comparable {
	return types.BoolKey(u != 0)
}

func (BoolKeyCodec) Compare(a, b uint64) int {
	// false (0) < true (1), so direct uint64 ordering is valid.
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// DateKeyCodec stores DateKey as UnixNano int64 bits.

type DateKeyCodec struct{}

func (DateKeyCodec) Encode(k types.Comparable) (uint64, error) {
	v, ok := k.(types.DateKey)
	if !ok {
		return 0, fmt.Errorf("%w: DateKeyCodec expected types.DateKey, got %T", types.ErrIncompatibleComparableTypes, k)
	}
	return uint64(time.Time(v).UnixNano()), nil
}

func (DateKeyCodec) Decode(u uint64) types.Comparable {
	return types.DateKey(time.Unix(0, int64(u))) //nolint:gosec // UnixNano bit pattern round-trip
}

func (DateKeyCodec) Compare(a, b uint64) int {
	ai, bi := int64(a), int64(b) //nolint:gosec // bit-pattern preserving cast for signed comparison
	if ai < bi {
		return -1
	}
	if ai > bi {
		return 1
	}
	return 0
}
