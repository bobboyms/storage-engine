package types

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

// UUIDKey is a native 16-byte UUID key. It is compared bytewise (the
// canonical UUID ordering) and renders in canonical 8-4-4-4-12 form. It
// is stored as raw bytes rather than text, so it is half the size of the
// 36-char string form and sorts correctly.
type UUIDKey [16]byte

// ParseUUID accepts the canonical hyphenated form
// ("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx") or the 32-hex-digit form,
// case-insensitively.
func ParseUUID(s string) (UUIDKey, error) {
	var hexStr string
	switch len(s) {
	case 36:
		if s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
			return UUIDKey{}, fmt.Errorf("types: invalid UUID format %q", s)
		}
		hexStr = s[0:8] + s[9:13] + s[14:18] + s[19:23] + s[24:36]
	case 32:
		hexStr = s
	default:
		return UUIDKey{}, fmt.Errorf("types: invalid UUID length %d in %q", len(s), s)
	}

	var k UUIDKey
	if _, err := hex.Decode(k[:], []byte(hexStr)); err != nil {
		return UUIDKey{}, fmt.Errorf("types: invalid UUID %q: %w", s, err)
	}
	return k, nil
}

// UUIDKeyFromBytes builds a UUIDKey from exactly 16 bytes.
func UUIDKeyFromBytes(b []byte) (UUIDKey, error) {
	if len(b) != 16 {
		return UUIDKey{}, fmt.Errorf("types: UUIDKey requires 16 bytes, got %d", len(b))
	}
	var k UUIDKey
	copy(k[:], b)
	return k, nil
}

// Compare orders two UUIDKey values bytewise. Returns
// ErrIncompatibleComparableTypes for any other type.
func (k UUIDKey) Compare(other Comparable) (int, error) {
	if c, isNull := nullOrderingAgainst(other); isNull {
		return c, nil
	}
	o, ok := other.(UUIDKey)
	if !ok {
		return 0, fmt.Errorf("%w: UUIDKey and %T", ErrIncompatibleComparableTypes, other)
	}
	return bytes.Compare(k[:], o[:]), nil
}

func (k UUIDKey) String() string {
	var buf [36]byte
	hex.Encode(buf[0:8], k[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], k[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], k[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], k[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], k[10:16])
	return string(buf[:])
}
