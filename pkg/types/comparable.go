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

const (
	CompositePrimaryMin = -1
	CompositePrimaryKey = 0
	CompositePrimaryMax = 1
)

// CompositeKey is the physical key used by non-unique secondary indexes.
// Secondary carries the logical indexed value; Primary makes each physical
// index entry unique, mirroring a B-tree ordered by (secondary_key, row_id).
type CompositeKey struct {
	Secondary    Comparable
	Primary      Comparable
	PrimaryBound int
}

func NewCompositeKey(secondary, primary Comparable) CompositeKey {
	return CompositeKey{Secondary: secondary, Primary: primary, PrimaryBound: CompositePrimaryKey}
}

func CompositeLowerBound(secondary Comparable) CompositeKey {
	return CompositeKey{Secondary: secondary, PrimaryBound: CompositePrimaryMin}
}

func CompositeUpperBound(secondary Comparable) CompositeKey {
	return CompositeKey{Secondary: secondary, PrimaryBound: CompositePrimaryMax}
}

func (k CompositeKey) Compare(other Comparable) (int, error) {
	o, ok := other.(CompositeKey)
	if !ok {
		return 0, fmt.Errorf("%w: CompositeKey and %T", ErrIncompatibleComparableTypes, other)
	}
	if k.Secondary == nil || o.Secondary == nil {
		return 0, fmt.Errorf("%w: CompositeKey with nil secondary", ErrIncompatibleComparableTypes)
	}
	if cmp, err := k.Secondary.Compare(o.Secondary); err != nil || cmp != 0 {
		return cmp, err
	}
	if k.PrimaryBound != CompositePrimaryKey || o.PrimaryBound != CompositePrimaryKey {
		if k.PrimaryBound < o.PrimaryBound {
			return -1, nil
		}
		if k.PrimaryBound > o.PrimaryBound {
			return 1, nil
		}
		return 0, nil
	}
	if k.Primary == nil || o.Primary == nil {
		return 0, fmt.Errorf("%w: CompositeKey with nil primary", ErrIncompatibleComparableTypes)
	}
	return k.Primary.Compare(o.Primary)
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
func (k CompositeKey) String() string {
	return fmt.Sprintf("(%v,%v,%d)", k.Secondary, k.Primary, k.PrimaryBound)
}
