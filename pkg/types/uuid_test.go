package types

import (
	"errors"
	"testing"
)

func TestUUIDKey_ParseAndStringRoundTrip(t *testing.T) {
	canonical := "550e8400-e29b-41d4-a716-446655440000"
	u, err := ParseUUID(canonical)
	if err != nil {
		t.Fatalf("ParseUUID: %v", err)
	}
	if got := u.String(); got != canonical {
		t.Fatalf("round-trip: got %q, want %q", got, canonical)
	}
	// Hyphen-less and uppercase forms parse to the same value.
	u2, err := ParseUUID("550E8400E29B41D4A716446655440000")
	if err != nil {
		t.Fatalf("ParseUUID hyphenless: %v", err)
	}
	if c := mustCompare(t, u, u2); c != 0 {
		t.Fatalf("hyphenless/uppercase should equal canonical, cmp=%d", c)
	}
}

func TestUUIDKey_ParseRejectsGarbage(t *testing.T) {
	for _, in := range []string{"", "not-a-uuid", "550e8400-e29b-41d4-a716", "zzze8400-e29b-41d4-a716-446655440000", "550e8400e29b41d4a71644665544000"} {
		if _, err := ParseUUID(in); err == nil {
			t.Errorf("ParseUUID(%q) should have failed", in)
		}
	}
}

func TestUUIDKey_CompareBytewise(t *testing.T) {
	lo, _ := ParseUUID("00000000-0000-0000-0000-000000000001")
	mid, _ := ParseUUID("00000000-0000-0000-0000-0000000000ff")
	hi, _ := ParseUUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
	if c := mustCompare(t, lo, mid); c != -1 {
		t.Errorf("lo vs mid: got %d, want -1", c)
	}
	if c := mustCompare(t, mid, hi); c != -1 {
		t.Errorf("mid vs hi: got %d, want -1", c)
	}
	if c := mustCompare(t, hi, lo); c != 1 {
		t.Errorf("hi vs lo: got %d, want 1", c)
	}
}

func TestUUIDKey_FromBytesLengthCheck(t *testing.T) {
	if _, err := UUIDKeyFromBytes(make([]byte, 15)); err == nil {
		t.Fatal("expected error for 15-byte input")
	}
	if _, err := UUIDKeyFromBytes(make([]byte, 16)); err != nil {
		t.Fatalf("16-byte input should be valid: %v", err)
	}
}

func TestUUIDKey_CompareIncompatibleType(t *testing.T) {
	u, _ := ParseUUID("550e8400-e29b-41d4-a716-446655440000")
	if _, err := u.Compare(VarcharKey("x")); !errors.Is(err, ErrIncompatibleComparableTypes) {
		t.Fatalf("expected ErrIncompatibleComparableTypes, got %v", err)
	}
}
