package types

import (
	"fmt"
	"math/big"
	"strings"
)

// DecimalKey is an exact-precision decimal usable as a key. Unlike
// FloatKey (IEEE-754, lossy), it stores an arbitrary-precision integer
// coefficient and a base-10 scale, so values like monetary amounts are
// represented and compared without rounding error.
//
// The value is coefficient × 10^(-scale). For example, 123.45 is stored
// as coefficient=12345, scale=2. Comparison is exact and ignores
// trailing-zero scale differences (1.50 == 1.5).
type DecimalKey struct {
	coef  *big.Int
	scale int32
}

// NewDecimal builds a DecimalKey from an unscaled int64 coefficient and a
// scale (number of fractional digits). NewDecimal(12345, 2) == 123.45.
func NewDecimal(unscaled int64, scale int32) DecimalKey {
	return DecimalKey{coef: big.NewInt(unscaled), scale: scale}
}

// ParseDecimal parses a plain decimal string: an optional sign, digits,
// and an optional single fractional part ("-123.45", "0.001", "42").
// Scientific notation and thousands separators are rejected so the
// representation stays exact and unambiguous.
func ParseDecimal(s string) (DecimalKey, error) {
	if s == "" {
		return DecimalKey{}, fmt.Errorf("types: empty decimal string")
	}

	neg := false
	body := s
	switch body[0] {
	case '+':
		body = body[1:]
	case '-':
		neg = true
		body = body[1:]
	}
	if body == "" {
		return DecimalKey{}, fmt.Errorf("types: decimal %q has no digits", s)
	}

	intPart, fracPart := body, ""
	if dot := strings.IndexByte(body, '.'); dot >= 0 {
		intPart = body[:dot]
		fracPart = body[dot+1:]
		if strings.IndexByte(fracPart, '.') >= 0 {
			return DecimalKey{}, fmt.Errorf("types: decimal %q has multiple separators", s)
		}
	}
	// Cap the digit counts well below int32 limits: a key with millions
	// of digits is pathological, and the bound makes the scale cast safe.
	if len(intPart) > maxDecimalDigits || len(fracPart) > maxDecimalDigits {
		return DecimalKey{}, fmt.Errorf("types: decimal %q exceeds %d digits", s, maxDecimalDigits)
	}
	// Require at least one digit overall, and only digits in each part.
	if intPart == "" && fracPart == "" {
		return DecimalKey{}, fmt.Errorf("types: decimal %q has no digits", s)
	}
	if !allDigits(intPart) || !allDigits(fracPart) {
		return DecimalKey{}, fmt.Errorf("types: decimal %q contains non-digit characters", s)
	}

	digits := intPart + fracPart
	if digits == "" {
		return DecimalKey{}, fmt.Errorf("types: decimal %q has no digits", s)
	}
	coef, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return DecimalKey{}, fmt.Errorf("types: decimal %q is not a valid integer coefficient", s)
	}
	if neg {
		coef.Neg(coef)
	}
	return DecimalKey{coef: coef, scale: int32(len(fracPart))}, nil //nolint:gosec // len(fracPart) <= maxDecimalDigits, far below int32 max
}

// maxDecimalDigits bounds the integer and fractional digit counts a
// decimal string may carry, keeping the scale within int32 and rejecting
// pathological input.
const maxDecimalDigits = 1 << 20

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func (k DecimalKey) coefficient() *big.Int {
	if k.coef == nil {
		return big.NewInt(0)
	}
	return k.coef
}

// Compare orders two DecimalKey values exactly. It aligns both to the
// larger scale before comparing coefficients, so 1.50 and 1.5 compare
// equal. Returns ErrIncompatibleComparableTypes for any other type.
func (k DecimalKey) Compare(other Comparable) (int, error) {
	o, ok := other.(DecimalKey)
	if !ok {
		return 0, fmt.Errorf("%w: DecimalKey and %T", ErrIncompatibleComparableTypes, other)
	}

	a := k.coefficient()
	b := o.coefficient()
	if k.scale != o.scale {
		// Scale up the value with the smaller scale so both share the
		// larger scale, keeping the comparison exact.
		if k.scale < o.scale {
			a = scaleUp(a, o.scale-k.scale)
		} else {
			b = scaleUp(b, k.scale-o.scale)
		}
	}
	return a.Cmp(b), nil
}

// scaleUp returns n × 10^power without mutating n.
func scaleUp(n *big.Int, power int32) *big.Int {
	mul := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(power)), nil)
	return new(big.Int).Mul(n, mul)
}

// String renders the decimal in plain notation, preserving the scale
// (so 1.50 stays "1.50"). Negative values carry a leading '-'.
func (k DecimalKey) String() string {
	coef := k.coefficient()
	if k.scale <= 0 {
		return coef.String()
	}

	neg := coef.Sign() < 0
	digits := new(big.Int).Abs(coef).String()
	scale := int(k.scale)
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	intPart := digits[:len(digits)-scale]
	fracPart := digits[len(digits)-scale:]
	out := intPart + "." + fracPart
	if neg {
		out = "-" + out
	}
	return out
}
