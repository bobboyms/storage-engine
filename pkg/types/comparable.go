package types

import (
	"errors"
	"fmt"
	"time"
)

var ErrIncompatibleComparableTypes = errors.New("types: incompatible comparable types")

// Comparable is implemented by all storage keys.
type Comparable interface {
	// Compare returns -1 when less than other, 0 when equal, and 1 when
	// greater than other. It returns ErrIncompatibleComparableTypes when
	// the concrete key types cannot be compared.
	Compare(other Comparable) (int, error)
}

type IntKey int

func (k IntKey) Compare(other Comparable) (int, error) {
	o, ok := other.(IntKey)
	if !ok {
		return 0, fmt.Errorf("%w: IntKey and %T", ErrIncompatibleComparableTypes, other)
	}
	if k < o {
		return -1, nil
	}
	if k > o {
		return 1, nil
	}
	return 0, nil
}

type VarcharKey string

func (k VarcharKey) Compare(other Comparable) (int, error) {
	o, ok := other.(VarcharKey)
	if !ok {
		return 0, fmt.Errorf("%w: VarcharKey and %T", ErrIncompatibleComparableTypes, other)
	}
	if k < o {
		return -1, nil
	}
	if k > o {
		return 1, nil
	}
	return 0, nil
}

type FloatKey float64

func (k FloatKey) Compare(other Comparable) (int, error) {
	o, ok := other.(FloatKey)
	if !ok {
		return 0, fmt.Errorf("%w: FloatKey and %T", ErrIncompatibleComparableTypes, other)
	}
	if k < o {
		return -1, nil
	}
	if k > o {
		return 1, nil
	}
	return 0, nil
}

// BoolKey sorts false before true.
type BoolKey bool

func (k BoolKey) Compare(other Comparable) (int, error) {
	o, ok := other.(BoolKey)
	if !ok {
		return 0, fmt.Errorf("%w: BoolKey and %T", ErrIncompatibleComparableTypes, other)
	}
	if k == o {
		return 0, nil
	}
	if !k && o {
		return -1, nil
	}
	return 1, nil
}

type DateKey time.Time

func (k DateKey) Compare(other Comparable) (int, error) {
	otherDate, ok := other.(DateKey)
	if !ok {
		return 0, fmt.Errorf("%w: DateKey and %T", ErrIncompatibleComparableTypes, other)
	}
	o := time.Time(otherDate)
	t := time.Time(k)
	if t.Before(o) {
		return -1, nil
	}
	if t.After(o) {
		return 1, nil
	}
	return 0, nil
}

func (k DateKey) String() string {
	return time.Time(k).Format("2006-01-02 15:04:05")
}

func (k IntKey) String() string     { return fmt.Sprintf("%d", k) }
func (k VarcharKey) String() string { return string(k) }
func (k FloatKey) String() string   { return fmt.Sprintf("%f", k) }
func (k BoolKey) String() string    { return fmt.Sprintf("%t", bool(k)) }
