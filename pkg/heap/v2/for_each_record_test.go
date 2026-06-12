package v2

import (
	"context"
	"testing"
)

// TestForEachRecord_VisitsEveryStoredRecord pins the full-heap scan used by
// index rebuilds: every stored record (live and tombstoned) is visited
// exactly once, vacuumed slots are skipped, and the callback receives the
// stable record ID.
func TestForEachRecord_VisitsEveryStoredRecord(t *testing.T) {
	h := newCheckedHeap(t)
	ctx := context.Background()

	live, err := h.Write([]byte("live"), 2, NoRecordID)
	if err != nil {
		t.Fatalf("write live: %v", err)
	}
	dead, err := h.Write([]byte("dead"), 3, NoRecordID)
	if err != nil {
		t.Fatalf("write dead: %v", err)
	}
	if err := h.Delete(dead, 6); err != nil {
		t.Fatalf("delete dead: %v", err)
	}
	gone, err := h.Write([]byte("gone"), 3, NoRecordID)
	if err != nil {
		t.Fatalf("write gone: %v", err)
	}
	if err := h.Delete(gone, 4); err != nil {
		t.Fatalf("delete gone: %v", err)
	}
	// Horizon 5 reclaims only "gone" (DeleteLSN 4); "dead" (DeleteLSN 6)
	// survives as a tombstone.
	if _, err := h.Vacuum(ctx, 5); err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	got := map[int64]string{}
	err = h.ForEachRecord(ctx, func(rid int64, rh RecordHeader, doc []byte) error {
		got[rid] = string(doc)
		if rid == live && !rh.Valid {
			t.Errorf("live record %d reported as not valid", rid)
		}
		if rid == dead && rh.Valid {
			t.Errorf("tombstoned record %d reported as valid", rid)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachRecord: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("visited %d records, want 2 (live + tombstone; vacuumed skipped): %v", len(got), got)
	}
	if got[live] != "live" || got[dead] != "dead" {
		t.Fatalf("wrong payloads visited: %v", got)
	}
	if _, ok := got[gone]; ok {
		t.Fatal("vacuumed record was visited")
	}
}
