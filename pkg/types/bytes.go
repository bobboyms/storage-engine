package types

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

// BytesKey is an arbitrary binary key (BLOB). It is ordered bytewise,
// matching the lexicographic ordering used by the storage engine's
// variable-key pages, and renders as hex for debugging.
type BytesKey []byte

// Compare orders two BytesKey values bytewise. Returns
// ErrIncompatibleComparableTypes for any other type.
func (k BytesKey) Compare(other Comparable) (int, error) {
	if c, isNull := nullOrderingAgainst(other); isNull {
		return c, nil
	}
	o, ok := other.(BytesKey)
	if !ok {
		return 0, fmt.Errorf("%w: BytesKey and %T", ErrIncompatibleComparableTypes, other)
	}
	return bytes.Compare(k, o), nil
}

func (k BytesKey) String() string {
	return hex.EncodeToString(k)
}
