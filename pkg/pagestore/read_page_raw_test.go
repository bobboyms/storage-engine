package pagestore

import (
	"path/filepath"
	"testing"
)

// TestReadPageRaw_ReturnsHoleBytesWithoutValidation pins the recovery
// primitive: a zero-filled hole page reads back as raw zeros (ReadPage would
// reject it with ErrInvalidMagic), while out-of-range reads still fail.
func TestReadPageRaw_ReturnsHoleBytesWithoutValidation(t *testing.T) {
	pf, err := NewPageFile(filepath.Join(t.TempDir(), "raw.pages"), nil)
	if err != nil {
		t.Fatalf("create page file: %v", err)
	}
	defer func() { _ = pf.Close() }()

	var page Page
	if err := pf.WritePage(2, &page); err != nil {
		t.Fatalf("write page 2: %v", err)
	}

	if _, err := pf.ReadPage(1); err == nil {
		t.Fatal("ReadPage on a hole page succeeded, want validation error")
	}

	raw, err := pf.ReadPageRaw(1)
	if err != nil {
		t.Fatalf("ReadPageRaw on hole: %v", err)
	}
	for i, b := range raw {
		if b != 0 {
			t.Fatalf("hole byte %d = %d, want 0", i, b)
		}
	}

	if _, err := pf.ReadPageRaw(99); err == nil {
		t.Fatal("ReadPageRaw out of range succeeded, want error")
	}
}
