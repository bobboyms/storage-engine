package types

import (
	"testing"
	"time"
)

func TestDateKey_UnixNanoChecked_WithinRange(t *testing.T) {
	when := time.Date(2024, 6, 1, 12, 0, 0, 123, time.UTC)
	got, err := DateKey(when).UnixNanoChecked()
	if err != nil {
		t.Fatalf("UnixNanoChecked in-range: %v", err)
	}
	if got != when.UnixNano() {
		t.Fatalf("UnixNanoChecked = %d, want %d", got, when.UnixNano())
	}
}

// TestDateKey_UnixNanoChecked_RejectsOverflow verifies a timestamp beyond
// the int64-nanosecond range (~year 2262) is reported as an error rather
// than silently wrapping to a bogus value.
func TestDateKey_UnixNanoChecked_RejectsOverflow(t *testing.T) {
	future := time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := DateKey(future).UnixNanoChecked(); err == nil {
		t.Fatalf("expected overflow error for year 3000, got nil")
	}

	past := time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := DateKey(past).UnixNanoChecked(); err == nil {
		t.Fatalf("expected overflow error for year 1000, got nil")
	}
}
