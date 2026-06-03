package types

import (
	"errors"
	"testing"
)

func TestCompositeKey_OrdersBySecondaryThenPrimary(t *testing.T) {
	cases := []struct {
		name     string
		left     CompositeKey
		right    CompositeKey
		expected int
	}{
		{
			name:     "different secondary dominates",
			left:     NewCompositeKey(VarcharKey("a"), IntKey(99)),
			right:    NewCompositeKey(VarcharKey("b"), IntKey(1)),
			expected: -1,
		},
		{
			name:     "same secondary breaks tie on primary",
			left:     NewCompositeKey(VarcharKey("a"), IntKey(1)),
			right:    NewCompositeKey(VarcharKey("a"), IntKey(2)),
			expected: -1,
		},
		{
			name:     "fully equal",
			left:     NewCompositeKey(VarcharKey("a"), IntKey(1)),
			right:    NewCompositeKey(VarcharKey("a"), IntKey(1)),
			expected: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.left.Compare(tc.right)
			if err != nil {
				t.Fatalf("Compare: %v", err)
			}
			if sign(got) != tc.expected {
				t.Fatalf("Compare = %d, want sign %d", got, tc.expected)
			}
		})
	}
}

func TestCompositeKey_BoundsBracketAllPrimaries(t *testing.T) {
	secondary := VarcharKey("shared")
	lower := CompositeLowerBound(secondary)
	upper := CompositeUpperBound(secondary)
	entry := NewCompositeKey(secondary, IntKey(0))

	if got, err := lower.Compare(entry); err != nil || got >= 0 {
		t.Fatalf("lower bound must sort before entry: got=%d err=%v", got, err)
	}
	if got, err := upper.Compare(entry); err != nil || got <= 0 {
		t.Fatalf("upper bound must sort after entry: got=%d err=%v", got, err)
	}
	// Bounds must also bracket the extreme primary values for the same secondary.
	minEntry := NewCompositeKey(secondary, IntKey(-1<<62))
	maxEntry := NewCompositeKey(secondary, IntKey(1<<62))
	if got, err := lower.Compare(minEntry); err != nil || got >= 0 {
		t.Fatalf("lower bound must sort before smallest entry: got=%d err=%v", got, err)
	}
	if got, err := upper.Compare(maxEntry); err != nil || got <= 0 {
		t.Fatalf("upper bound must sort after largest entry: got=%d err=%v", got, err)
	}
}

func TestCompositeKey_CompareRejectsBadInputs(t *testing.T) {
	valid := NewCompositeKey(VarcharKey("a"), IntKey(1))

	if _, err := valid.Compare(IntKey(1)); !errors.Is(err, ErrIncompatibleComparableTypes) {
		t.Fatalf("comparing to non-composite: want ErrIncompatibleComparableTypes, got %v", err)
	}
	nilSecondary := CompositeKey{PrimaryBound: CompositePrimaryKey}
	if _, err := nilSecondary.Compare(valid); !errors.Is(err, ErrIncompatibleComparableTypes) {
		t.Fatalf("nil secondary: want ErrIncompatibleComparableTypes, got %v", err)
	}
	nilPrimary := CompositeKey{Secondary: VarcharKey("a"), PrimaryBound: CompositePrimaryKey}
	if _, err := nilPrimary.Compare(valid); !errors.Is(err, ErrIncompatibleComparableTypes) {
		t.Fatalf("nil primary: want ErrIncompatibleComparableTypes, got %v", err)
	}
}

func TestCompositeKey_String(t *testing.T) {
	k := NewCompositeKey(VarcharKey("a"), IntKey(7))
	if got, want := k.String(), "(a,7,0)"; got != want {
		t.Fatalf("String = %q, want %q", got, want)
	}
}

func sign(v int) int {
	switch {
	case v < 0:
		return -1
	case v > 0:
		return 1
	default:
		return 0
	}
}
