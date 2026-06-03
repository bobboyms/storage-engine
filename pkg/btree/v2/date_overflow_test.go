package v2

import (
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// TestDateKeyCodec_RejectsOverflow verifies the fixed-key date codec
// surfaces an error for timestamps outside the representable UnixNano
// range instead of silently wrapping to a bogus 8-byte value.
func TestDateKeyCodec_RejectsOverflow(t *testing.T) {
	future := types.DateKey(time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	if _, err := (DateKeyCodec{}).Encode(future); err == nil {
		t.Fatal("DateKeyCodec.Encode should reject an out-of-range date")
	}

	inRange := types.DateKey(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	if _, err := (DateKeyCodec{}).Encode(inRange); err != nil {
		t.Fatalf("DateKeyCodec.Encode in-range: %v", err)
	}
}

// TestCompositeKeyCodec_RejectsDateOverflow verifies the secondary-index
// composite codec also rejects out-of-range dates.
func TestCompositeKeyCodec_RejectsDateOverflow(t *testing.T) {
	future := types.DateKey(time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	ck := types.NewCompositeKey(future, types.IntKey(1))
	if _, err := (CompositeKeyCodec{}).Encode(ck); err == nil {
		t.Fatal("CompositeKeyCodec.Encode should reject an out-of-range date component")
	}
}
