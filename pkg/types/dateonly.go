package types

import (
	"fmt"
	"time"
)

// DateOnlyKey is a calendar date (year-month-day) with no time-of-day and
// no timezone — the DATE type, as distinct from DateKey, which is an
// absolute instant (TIMESTAMP). Because it stores the civil components
// directly and compares them in order, it is timezone-naive and has no
// UnixNano range limit (it represents any year an int32 can hold).
type DateOnlyKey struct {
	year  int32
	month uint8 // 1-12
	day   uint8 // 1-31
}

// NewDateOnly builds a validated DateOnlyKey. It rejects impossible dates
// (e.g. February 30) rather than normalizing them.
func NewDateOnly(year int, month time.Month, day int) (DateOnlyKey, error) {
	if month < time.January || month > time.December {
		return DateOnlyKey{}, fmt.Errorf("types: invalid month %d", month)
	}
	if day < 1 || day > 31 {
		return DateOnlyKey{}, fmt.Errorf("types: invalid day %d", day)
	}
	// Validate day-in-month (and leap years) via calendar round-trip in
	// UTC, which involves no UnixNano conversion so the year range is
	// unbounded.
	norm := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	if norm.Year() != year || norm.Month() != month || norm.Day() != day {
		return DateOnlyKey{}, fmt.Errorf("types: %04d-%02d-%02d is not a valid calendar date", year, month, day)
	}
	return DateOnlyKey{year: int32(year), month: uint8(month), day: uint8(day)}, nil //nolint:gosec // year range validated by caller; month/day bounded above
}

// ParseDateOnly parses a strict "YYYY-MM-DD" date (zero-padded), with no
// time or timezone suffix.
func ParseDateOnly(s string) (DateOnlyKey, error) {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return DateOnlyKey{}, fmt.Errorf("types: invalid DATE %q: %w", s, err)
	}
	// time.Parse("2006-01-02") accepts only the date layout, but be
	// explicit that the input had no trailing components.
	if t.Format("2006-01-02") != s {
		return DateOnlyKey{}, fmt.Errorf("types: DATE %q is not canonical YYYY-MM-DD", s)
	}
	return NewDateOnly(t.Year(), t.Month(), t.Day())
}

// Compare orders by year, then month, then day. NULL sorts first.
// Comparing against any non-DateOnlyKey (including DateKey/TIMESTAMP)
// returns ErrIncompatibleComparableTypes.
func (k DateOnlyKey) Compare(other Comparable) (int, error) {
	if c, isNull := nullOrderingAgainst(other); isNull {
		return c, nil
	}
	o, ok := other.(DateOnlyKey)
	if !ok {
		return 0, fmt.Errorf("%w: DateOnlyKey and %T", ErrIncompatibleComparableTypes, other)
	}
	if c := cmpInt32(k.year, o.year); c != 0 {
		return c, nil
	}
	if c := cmpUint8(k.month, o.month); c != 0 {
		return c, nil
	}
	return cmpUint8(k.day, o.day), nil
}

func (k DateOnlyKey) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", k.year, k.month, k.day)
}

func cmpInt32(a, b int32) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpUint8(a, b uint8) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
