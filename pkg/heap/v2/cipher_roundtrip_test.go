package v2

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

// TestVacuumedPageSurvivesCipherRoundTrip reproduces the TDE data-loss bug
// behind the corrupt BSON reads seen in pkg/sql under vacuum: with an
// encrypting PageFile, only body[:UsableBodySize] survives the disk round-trip
// (the AES-GCM nonce+tag occupy the final 28 bytes of the on-disk body).
// InitSlottedPage honors that limit, but OpenSlottedPage opened existing pages
// with the FULL body, so Compact — which repacks surviving records from
// len(body) downward — moved live records into the cipher-reserved tail. The
// next flush silently truncated them on disk, and any reload (eviction,
// reopen, recovery) read the tail back as zeros.
//
// Deterministic repro: write two records, delete the first, vacuum (Compact
// repacks the survivor at the tail), flush via Close, reopen, read the
// survivor and require its bytes intact.
func TestVacuumedPageSurvivesCipherRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "heap.db")
	cipher := makeCipher(t) // stateless; safe to share across both opens

	h, err := NewHeapV2(path, 16, cipher)
	if err != nil {
		t.Fatalf("NewHeapV2: %v", err)
	}

	// Distinctive tails so truncation to zeros cannot pass unnoticed; > 28
	// bytes so the survivor necessarily spans the cipher-reserved zone when
	// Compact packs it at the very end of an unrestricted body.
	doc1 := bytes.Repeat([]byte{0x11}, 96)
	doc2 := bytes.Repeat([]byte{0xAB}, 96)

	rid1, err := h.Write(doc1, 1, -1)
	if err != nil {
		t.Fatalf("Write doc1: %v", err)
	}
	rid2, err := h.Write(doc2, 2, -1)
	if err != nil {
		t.Fatalf("Write doc2: %v", err)
	}

	if err := h.Delete(rid1, 3); err != nil {
		t.Fatalf("Delete rid1: %v", err)
	}
	reclaimed, err := h.Vacuum(context.Background(), 3)
	if err != nil {
		t.Fatalf("Vacuum: %v", err)
	}
	if reclaimed != 1 {
		t.Fatalf("Vacuum reclaimed = %d, want 1 (doc1's slot)", reclaimed)
	}

	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	h2, err := NewHeapV2(path, 16, cipher)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer h2.Close()

	got, _, err := h2.Read(rid2)
	if err != nil {
		t.Fatalf("Read survivor after reopen: %v", err)
	}
	if !bytes.Equal(got, doc2) {
		t.Fatalf("survivor corrupted across cipher round-trip:\n got %x\nwant %x", got, doc2)
	}
}
