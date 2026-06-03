package v2

import (
	"bytes"
	"encoding/binary"
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

// CompositeKeyCodec stores secondary index entries as variable-size
// CompositeKey values. Its Compare method decodes and delegates to
// CompositeKey.Compare, so the byte format only needs to be stable and
// reversible; it does not need to be lexicographically sortable.
type CompositeKeyCodec struct{}

const (
	compositeCodecVersion byte = 1
	compositeTypeInt      byte = 1
	compositeTypeVarchar  byte = 2
	compositeTypeBool     byte = 3
	compositeTypeFloat    byte = 4
	compositeTypeDate     byte = 5
)

func (CompositeKeyCodec) Encode(k types.Comparable) ([]byte, error) {
	v, ok := k.(types.CompositeKey)
	if !ok {
		return nil, fmt.Errorf("%w: CompositeKeyCodec expected types.CompositeKey, got %T", types.ErrIncompatibleComparableTypes, k)
	}
	if v.Secondary == nil {
		return nil, fmt.Errorf("%w: CompositeKeyCodec requires secondary key", types.ErrIncompatibleComparableTypes)
	}
	out := make([]byte, 0, 32)
	out = append(out, compositeCodecVersion)
	var err error
	out, err = appendCompositeComponent(out, v.Secondary)
	if err != nil {
		return nil, err
	}
	out = append(out, byte(v.PrimaryBound+1)) //nolint:gosec // PrimaryBound is constrained to {-1,0,1}, so PrimaryBound+1 is in {0,1,2}
	if v.PrimaryBound == types.CompositePrimaryKey {
		if v.Primary == nil {
			return nil, fmt.Errorf("%w: CompositeKeyCodec requires primary key", types.ErrIncompatibleComparableTypes)
		}
		out, err = appendCompositeComponent(out, v.Primary)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (CompositeKeyCodec) Decode(b []byte) types.Comparable {
	if len(b) == 0 || b[0] != compositeCodecVersion {
		return types.CompositeKey{}
	}
	secondary, pos, err := decodeCompositeComponent(b, 1)
	if err != nil || pos >= len(b) {
		return types.CompositeKey{}
	}
	bound := int(b[pos]) - 1
	pos++
	if bound != types.CompositePrimaryKey {
		return types.CompositeKey{Secondary: secondary, PrimaryBound: bound}
	}
	primary, _, err := decodeCompositeComponent(b, pos)
	if err != nil {
		return types.CompositeKey{}
	}
	return types.NewCompositeKey(secondary, primary)
}

func (c CompositeKeyCodec) Compare(a, b []byte) int {
	ak, aok := c.Decode(a).(types.CompositeKey)
	bk, bok := c.Decode(b).(types.CompositeKey)
	if !aok || !bok || ak.Secondary == nil || bk.Secondary == nil {
		return bytes.Compare(a, b)
	}
	cmp, err := ak.Compare(bk)
	if err != nil {
		return bytes.Compare(a, b)
	}
	return cmp
}

func appendCompositeComponent(out []byte, key types.Comparable) ([]byte, error) {
	var payload []byte
	var tag byte
	switch v := key.(type) {
	case types.IntKey:
		tag = compositeTypeInt
		payload = make([]byte, 8)
		binary.LittleEndian.PutUint64(payload, uint64(int64(v))) //nolint:gosec // preserve signed bit pattern
	case types.VarcharKey:
		tag = compositeTypeVarchar
		payload = []byte(string(v))
	case types.BoolKey:
		tag = compositeTypeBool
		if bool(v) {
			payload = []byte{1}
		} else {
			payload = []byte{0}
		}
	case types.FloatKey:
		tag = compositeTypeFloat
		payload = make([]byte, 8)
		binary.LittleEndian.PutUint64(payload, math.Float64bits(float64(v)))
	case types.DateKey:
		tag = compositeTypeDate
		nano, err := v.UnixNanoChecked()
		if err != nil {
			return nil, err
		}
		payload = make([]byte, 8)
		binary.LittleEndian.PutUint64(payload, uint64(nano)) //nolint:gosec // preserve signed bit pattern
	default:
		return nil, fmt.Errorf("%w: unsupported composite key component %T", types.ErrIncompatibleComparableTypes, key)
	}
	out = append(out, tag)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload))) //nolint:gosec // key payload is page bounded
	out = append(out, lenBuf[:]...)
	out = append(out, payload...)
	return out, nil
}

func decodeCompositeComponent(b []byte, pos int) (types.Comparable, int, error) {
	if pos+5 > len(b) {
		return nil, pos, fmt.Errorf("btree/v2: truncated composite key component")
	}
	tag := b[pos]
	pos++
	n := int(binary.LittleEndian.Uint32(b[pos : pos+4]))
	pos += 4
	if n < 0 || pos+n > len(b) {
		return nil, pos, fmt.Errorf("btree/v2: invalid composite key component length")
	}
	payload := b[pos : pos+n]
	pos += n
	switch tag {
	case compositeTypeInt:
		if len(payload) != 8 {
			return nil, pos, fmt.Errorf("btree/v2: invalid int component length")
		}
		return types.IntKey(int64(binary.LittleEndian.Uint64(payload))), pos, nil //nolint:gosec // inverse of encode
	case compositeTypeVarchar:
		return types.VarcharKey(string(payload)), pos, nil
	case compositeTypeBool:
		if len(payload) != 1 {
			return nil, pos, fmt.Errorf("btree/v2: invalid bool component length")
		}
		return types.BoolKey(payload[0] != 0), pos, nil
	case compositeTypeFloat:
		if len(payload) != 8 {
			return nil, pos, fmt.Errorf("btree/v2: invalid float component length")
		}
		return types.FloatKey(math.Float64frombits(binary.LittleEndian.Uint64(payload))), pos, nil
	case compositeTypeDate:
		if len(payload) != 8 {
			return nil, pos, fmt.Errorf("btree/v2: invalid date component length")
		}
		return types.DateKey(time.Unix(0, int64(binary.LittleEndian.Uint64(payload)))), pos, nil //nolint:gosec // inverse of encode
	default:
		return nil, pos, fmt.Errorf("btree/v2: unknown composite key component tag %d", tag)
	}
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
	nano, err := v.UnixNanoChecked()
	if err != nil {
		return 0, err
	}
	return uint64(nano), nil //nolint:gosec // UnixNano bit pattern round-trip
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
