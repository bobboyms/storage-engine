package types

import (
	"errors"
	"testing"
	"time"
)

func TestDateOnlyKey_ParseAndString(t *testing.T) {
	for _, s := range []string{"2024-06-01", "1000-01-01", "3000-12-31", "0001-01-01"} {
		d, err := ParseDateOnly(s)
		if err != nil {
			t.Fatalf("ParseDateOnly(%q): %v", s, err)
		}
		if got := d.String(); got != s {
			t.Errorf("ParseDateOnly(%q).String() = %q", s, got)
		}
	}
}

func TestDateOnlyKey_ParseRejectsInvalid(t *testing.T) {
	for _, s := range []string{"", "2024-13-01", "2024-00-01", "2024-02-30", "2024-06-31", "not-a-date", "2024-6-1", "2024/06/01", "2024-06-01T00:00:00"} {
		if _, err := ParseDateOnly(s); err == nil {
			t.Errorf("ParseDateOnly(%q) should have failed", s)
		}
	}
}

func TestDateOnlyKey_Ordering(t *testing.T) {
	mk := func(s string) DateOnlyKey {
		d, err := ParseDateOnly(s)
		if err != nil {
			t.Fatalf("ParseDateOnly(%q): %v", s, err)
		}
		return d
	}
	// Wide range, including years outside the UnixNano window, ordered.
	ordered := []string{"1000-01-01", "1677-01-01", "2024-06-01", "2024-06-02", "2024-07-01", "2025-01-01", "3000-12-31"}
	for i := 0; i+1 < len(ordered); i++ {
		if c := mustCompare(t, mk(ordered[i]), mk(ordered[i+1])); c != -1 {
			t.Errorf("%s vs %s: got %d, want -1", ordered[i], ordered[i+1], c)
		}
	}
	if c := mustCompare(t, mk("2024-06-01"), mk("2024-06-01")); c != 0 {
		t.Fatalf("equal dates: got %d, want 0", c)
	}
}

func TestDateOnlyKey_NewDateOnlyValidation(t *testing.T) {
	if _, err := NewDateOnly(2024, time.February, 30); err == nil {
		t.Fatal("Feb 30 should be invalid")
	}
	if d, err := NewDateOnly(2024, time.February, 29); err != nil { // 2024 is a leap year
		t.Fatalf("Feb 29 2024 should be valid: %v", err)
	} else if d.String() != "2024-02-29" {
		t.Fatalf("got %q", d.String())
	}
}

func TestDateOnlyKey_NullSortsFirst(t *testing.T) {
	d, _ := ParseDateOnly("2024-06-01")
	if c := mustCompare(t, NullKey{}, d); c != -1 {
		t.Fatalf("NULL vs date: got %d, want -1", c)
	}
	if c := mustCompare(t, d, NullKey{}); c != 1 {
		t.Fatalf("date vs NULL: got %d, want 1", c)
	}
}

func TestDateOnlyKey_OrdinalRoundTripAndOrder(t *testing.T) {
	mk := func(y int, m time.Month, d int) DateOnlyKey {
		k, err := NewDateOnly(y, m, d)
		if err != nil {
			t.Fatalf("NewDateOnly(%d,%d,%d): %v", y, m, d, err)
		}
		return k
	}
	cases := []DateOnlyKey{
		mk(-44, time.March, 15), // proleptic / negative year
		mk(1, time.January, 1),
		mk(1969, time.December, 31),
		mk(2024, time.February, 29),
		mk(9999, time.December, 31),
	}
	for _, d := range cases {
		back := DateOnlyFromOrdinal(d.Ordinal())
		if back.String() != d.String() {
			t.Fatalf("ordinal round-trip: %q -> %q", d.String(), back.String())
		}
	}
	// Ordinal must be monotonic with calendar order, including negatives.
	for i := 0; i+1 < len(cases); i++ {
		if cases[i].Ordinal() >= cases[i+1].Ordinal() {
			t.Fatalf("ordinal not monotonic at %d: %d >= %d", i, cases[i].Ordinal(), cases[i+1].Ordinal())
		}
	}
}

func TestDateOnlyKey_IncompatibleWithTimestamp(t *testing.T) {
	d, _ := ParseDateOnly("2024-06-01")
	if _, err := d.Compare(DateKey(time.Now())); !errors.Is(err, ErrIncompatibleComparableTypes) {
		t.Fatalf("DATE vs TIMESTAMP should be incompatible, got %v", err)
	}
}
