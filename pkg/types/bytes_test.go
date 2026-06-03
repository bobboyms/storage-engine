package types

import (
	"errors"
	"testing"
)

func TestBytesKey_CompareBytewise(t *testing.T) {
	ordered := []BytesKey{
		{},
		{0x00},
		{0x00, 0x00},
		{0x00, 0x01},
		{0x01},
		{0x01, 0xff},
		{0xff},
		{0xff, 0x00},
	}
	for i := 0; i+1 < len(ordered); i++ {
		if c := mustCompare(t, ordered[i], ordered[i+1]); c != -1 {
			t.Errorf("%v vs %v: got %d, want -1", ordered[i], ordered[i+1], c)
		}
		if c := mustCompare(t, ordered[i+1], ordered[i]); c != 1 {
			t.Errorf("%v vs %v: got %d, want 1", ordered[i+1], ordered[i], c)
		}
	}
}

func TestBytesKey_Equal(t *testing.T) {
	if c := mustCompare(t, BytesKey{1, 2, 3}, BytesKey{1, 2, 3}); c != 0 {
		t.Fatalf("equal byte slices: got %d, want 0", c)
	}
}

func TestBytesKey_CompareIncompatibleType(t *testing.T) {
	if _, err := (BytesKey{1}).Compare(VarcharKey("x")); !errors.Is(err, ErrIncompatibleComparableTypes) {
		t.Fatalf("expected ErrIncompatibleComparableTypes, got %v", err)
	}
}

func TestBytesKey_StringIsHex(t *testing.T) {
	if got := (BytesKey{0xde, 0xad, 0xbe, 0xef}).String(); got != "deadbeef" {
		t.Fatalf("BytesKey.String() = %q, want %q", got, "deadbeef")
	}
}
