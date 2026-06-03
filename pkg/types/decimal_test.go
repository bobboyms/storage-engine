package types

import (
	"errors"
	"testing"
)

func TestDecimalKey_ParseAndString(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"0", "0"},
		{"123", "123"},
		{"-5", "-5"},
		{"123.45", "123.45"},
		{"-0.01", "-0.01"},
		{"1.50", "1.50"},
		{"000.500", "0.500"},
	}
	for _, tc := range cases {
		d, err := ParseDecimal(tc.in)
		if err != nil {
			t.Fatalf("ParseDecimal(%q): %v", tc.in, err)
		}
		if got := d.String(); got != tc.want {
			t.Errorf("ParseDecimal(%q).String() = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestDecimalKey_ParseRejectsGarbage(t *testing.T) {
	for _, in := range []string{"", "abc", "1.2.3", "1,5", "1e3", "--1", "."} {
		if _, err := ParseDecimal(in); err == nil {
			t.Errorf("ParseDecimal(%q) should have failed", in)
		}
	}
}

func TestDecimalKey_CompareExactOrdering(t *testing.T) {
	mk := func(s string) DecimalKey {
		t.Helper()
		d, err := ParseDecimal(s)
		if err != nil {
			t.Fatalf("ParseDecimal(%q): %v", s, err)
		}
		return d
	}

	// Equality must ignore trailing-zero scale differences: 1.50 == 1.5.
	if c := mustCompare(t, mk("1.50"), mk("1.5")); c != 0 {
		t.Fatalf("1.50 vs 1.5: got %d, want 0", c)
	}

	ordered := []string{"-100", "-1.5", "-0.01", "0", "0.001", "0.01", "1.5", "1.55", "2", "100.0001"}
	for i := 0; i+1 < len(ordered); i++ {
		lo, hi := mk(ordered[i]), mk(ordered[i+1])
		if c := mustCompare(t, lo, hi); c != -1 {
			t.Errorf("%s vs %s: got %d, want -1", ordered[i], ordered[i+1], c)
		}
		if c := mustCompare(t, hi, lo); c != 1 {
			t.Errorf("%s vs %s: got %d, want 1", ordered[i+1], ordered[i], c)
		}
	}
}

func TestDecimalKey_CompareIncompatibleType(t *testing.T) {
	d, _ := ParseDecimal("1.23")
	if _, err := d.Compare(IntKey(1)); !errors.Is(err, ErrIncompatibleComparableTypes) {
		t.Fatalf("expected ErrIncompatibleComparableTypes, got %v", err)
	}
}

func TestNewDecimal_FromUnscaled(t *testing.T) {
	// 12345 with scale 2 == 123.45
	d := NewDecimal(12345, 2)
	parsed, _ := ParseDecimal("123.45")
	if c := mustCompare(t, d, parsed); c != 0 {
		t.Fatalf("NewDecimal(12345,2) != 123.45 (got cmp %d)", c)
	}
}
