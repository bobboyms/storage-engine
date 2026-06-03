package types

import (
	"testing"
	"time"
)

func TestNullKey_EqualToNull(t *testing.T) {
	if c := mustCompare(t, NullKey{}, NullKey{}); c != 0 {
		t.Fatalf("NULL vs NULL: got %d, want 0", c)
	}
}

// TestNullKey_SortsFirst verifies NULL is the minimum (NULLS FIRST)
// against every other key type, in both operand orders, so ordering is
// consistent regardless of which side the NULL is on.
func TestNullKey_SortsFirst(t *testing.T) {
	uuid, _ := ParseUUID("00000000-0000-0000-0000-000000000000")
	dec, _ := ParseDecimal("-999.99")
	others := []Comparable{
		IntKey(-100),
		VarcharKey(""),
		FloatKey(-1e9),
		BoolKey(false),
		DateKey(time.Unix(0, 0)),
		dec,
		BytesKey{},
		uuid,
	}
	null := NullKey{}
	for _, o := range others {
		if c := mustCompare(t, null, o); c != -1 {
			t.Errorf("NULL vs %T: got %d, want -1", o, c)
		}
		if c := mustCompare(t, o, null); c != 1 {
			t.Errorf("%T vs NULL: got %d, want 1", o, c)
		}
	}
}

func TestNullKey_String(t *testing.T) {
	if got := (NullKey{}).String(); got != "NULL" {
		t.Fatalf("NullKey.String() = %q, want %q", got, "NULL")
	}
}
