package v2

import (
	"math"
	"path/filepath"
	"testing"
)

// TestApplyPageRedo_OverridesPoisonedDiskLSN covers heaps written by a binary
// from before the vacuum horizon clamp: their on-disk pages can carry the
// MaxUint64 PageLSN sentinel. Such a page must not be treated as "newer than
// every redo entry" (which would skip all physical redo for it forever), and
// applying a redo image must leave the page with a real, healed PageLSN.
func TestApplyPageRedo_OverridesPoisonedDiskLSN(t *testing.T) {
	h, err := NewHeapV2(filepath.Join(t.TempDir(), "t.heap"), 16, nil)
	if err != nil {
		t.Fatalf("new heap: %v", err)
	}
	defer func() { _ = h.Close() }()

	rid, err := h.Write([]byte("doc-1"), 3, -1)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	pid, _ := DecodeRecordID(rid)
	if err := h.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	// Stamp the on-disk page with the poisoned sentinel, the way a pre-clamp
	// vacuum did.
	handle, err := h.bp.FetchForWrite(pid)
	if err != nil {
		t.Fatalf("fetch page: %v", err)
	}
	handle.Page().AdvancePageLSN(math.MaxUint64)
	handle.MarkDirty()
	handle.Release()
	if err := h.Sync(); err != nil {
		t.Fatalf("sync poisoned page: %v", err)
	}

	// A redo image captured from that same stamped page: its embedded header
	// is poisoned too, but the WAL entry carries a real (clamped) LSN.
	image, err := h.pf.ReadPage(pid)
	if err != nil {
		t.Fatalf("read page image: %v", err)
	}

	applied, err := h.ApplyPageRedo(pid, image, 7)
	if err != nil {
		t.Fatalf("apply page redo: %v", err)
	}
	if !applied {
		t.Fatal("ApplyPageRedo skipped the entry because the poisoned on-disk PageLSN looked newer")
	}

	onDisk, err := h.pf.ReadPage(pid)
	if err != nil {
		t.Fatalf("read page after redo: %v", err)
	}
	hdr, err := onDisk.GetHeader()
	if err != nil {
		t.Fatalf("page header after redo: %v", err)
	}
	if hdr.PageLSN != 7 {
		t.Fatalf("on-disk PageLSN = %d after redo, want 7 (healed to the entry LSN)", hdr.PageLSN)
	}
}
