package v2

import (
	"context"
	"math"
	"path/filepath"
	"strings"
	"testing"
)

func newCheckedHeap(t *testing.T) *HeapV2 {
	t.Helper()
	h, err := NewHeapV2(filepath.Join(t.TempDir(), "t.heap"), 16, nil)
	if err != nil {
		t.Fatalf("new heap: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func mustCheck(t *testing.T, h *HeapV2) []IntegrityIssue {
	t.Helper()
	issues, err := h.CheckIntegrity(context.Background())
	if err != nil {
		t.Fatalf("CheckIntegrity: %v", err)
	}
	return issues
}

func requireIssueContaining(t *testing.T, issues []IntegrityIssue, fragment string) {
	t.Helper()
	for _, issue := range issues {
		if strings.Contains(issue.Detail, fragment) {
			return
		}
	}
	t.Fatalf("no integrity issue mentions %q; got %v", fragment, issues)
}

func TestCheckIntegrity_HealthyHeapIsClean(t *testing.T) {
	h := newCheckedHeap(t)

	// A realistic mix: live rows, a version chain, a tombstone, and a
	// vacuumed slot.
	first, err := h.Write([]byte("v1"), 2, NoRecordID)
	if err != nil {
		t.Fatalf("write v1: %v", err)
	}
	if _, err := h.Write([]byte("v2"), 3, first); err != nil {
		t.Fatalf("write v2 chained: %v", err)
	}
	dead, err := h.Write([]byte("dead"), 4, NoRecordID)
	if err != nil {
		t.Fatalf("write dead: %v", err)
	}
	if err := h.Delete(dead, 5); err != nil {
		t.Fatalf("delete: %v", err)
	}
	gone, err := h.Write([]byte("gone"), 6, NoRecordID)
	if err != nil {
		t.Fatalf("write gone: %v", err)
	}
	if err := h.Delete(gone, 7); err != nil {
		t.Fatalf("delete gone: %v", err)
	}
	if _, err := h.Vacuum(context.Background(), 8); err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	if issues := mustCheck(t, h); len(issues) != 0 {
		t.Fatalf("healthy heap reported issues: %v", issues)
	}
}

func TestCheckIntegrity_FlagsPoisonedLSNs(t *testing.T) {
	h := newCheckedHeap(t)
	rid, err := h.Write([]byte("doc"), 3, NoRecordID)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	pid, _ := DecodeRecordID(rid)

	handle, err := h.bp.FetchForWrite(pid)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	handle.Page().AdvancePageLSN(math.MaxUint64)
	handle.MarkDirty()
	handle.Release()

	requireIssueContaining(t, mustCheck(t, h), "poisoned PageLSN")
}

func TestCheckIntegrity_FlagsDanglingPrevRecordID(t *testing.T) {
	h := newCheckedHeap(t)
	// A version chain pointing at a page that does not exist.
	bogusPrev := EncodeRecordID(99, 5)
	if _, err := h.Write([]byte("doc"), 3, bogusPrev); err != nil {
		t.Fatalf("write: %v", err)
	}

	requireIssueContaining(t, mustCheck(t, h), "PrevRecordID")
}

func TestCheckIntegrity_FlagsTombstoneWithoutDeleteLSN(t *testing.T) {
	h := newCheckedHeap(t)
	rid, err := h.Write([]byte("doc"), 3, NoRecordID)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	pid, slot := DecodeRecordID(rid)

	// Corrupt the record header in place: invalid without a DeleteLSN.
	handle, err := h.bp.FetchForWrite(pid)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	sp := OpenSlottedPage(handle.Page(), h.maxBodySize)
	offset, _ := sp.readSlot(slot)
	var rh RecordHeader
	decodeRecordHeader(&rh, sp.body[offset:offset+RecordHeaderSize])
	rh.Valid = false
	rh.DeleteLSN = 0
	encodeRecordHeader(&rh, sp.body[offset:offset+RecordHeaderSize])
	handle.MarkDirty()
	handle.Release()

	requireIssueContaining(t, mustCheck(t, h), "tombstone")
}

func TestCheckIntegrity_FlagsSlotOutOfBounds(t *testing.T) {
	h := newCheckedHeap(t)
	rid, err := h.Write([]byte("doc"), 3, NoRecordID)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	pid, slot := DecodeRecordID(rid)

	handle, err := h.bp.FetchForWrite(pid)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	sp := OpenSlottedPage(handle.Page(), h.maxBodySize)
	sp.writeSlot(slot, uint16(h.maxBodySize-2), 500) //nolint:gosec // test value within uint16
	handle.MarkDirty()
	handle.Release()

	requireIssueContaining(t, mustCheck(t, h), "slot")
}
