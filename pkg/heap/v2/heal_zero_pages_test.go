package v2

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// TestHealZeroPages_FormatsHolesAndKeepsData pins the recovery primitive: a
// zero-filled hole page (left by a crash after an eviction flush extended
// the file) becomes a readable empty slotted page, real pages are untouched,
// and a second pass is a no-op.
func TestHealZeroPages_FormatsHolesAndKeepsData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "holes.heap")

	// Build the hole at the pagestore level: write page 2, never page 1.
	pf, err := pagestore.NewPageFile(path, nil)
	if err != nil {
		t.Fatalf("create page file: %v", err)
	}
	var page pagestore.Page
	sp := InitSlottedPage(&page, pf.UsableBodySize())
	if _, err := sp.Insert(RecordHeader{Valid: true, CreateLSN: 7}, []byte(`{"id":"kept"}`)); err != nil {
		t.Fatalf("insert record: %v", err)
	}
	if err := pf.WritePage(2, &page); err != nil {
		t.Fatalf("write page 2: %v", err)
	}
	if err := pf.Close(); err != nil {
		t.Fatalf("close page file: %v", err)
	}

	h, err := NewHeapV2(path, 8, nil)
	if err != nil {
		t.Fatalf("open heap: %v", err)
	}
	defer func() { _ = h.Close() }()

	ctx := context.Background()
	healed, err := h.HealZeroPages(ctx)
	if err != nil {
		t.Fatalf("HealZeroPages: %v", err)
	}
	if healed != 1 {
		t.Fatalf("healed = %d, want 1 (page 1 only)", healed)
	}

	// The whole heap must now be scannable, with the original record intact.
	var docs []string
	err = h.ForEachRecord(ctx, func(_ int64, rh RecordHeader, doc []byte) error {
		if rh.Valid {
			docs = append(docs, string(doc))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachRecord after heal: %v", err)
	}
	if len(docs) != 1 || docs[0] != `{"id":"kept"}` {
		t.Fatalf("records after heal = %v, want the one kept record", docs)
	}

	if again, err := h.HealZeroPages(ctx); err != nil || again != 0 {
		t.Fatalf("second pass healed %d (err=%v), want 0 and nil", again, err)
	}
}
