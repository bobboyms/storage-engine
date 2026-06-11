package v2

import (
	"math"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// TestApplyPageRedo_OverridesPoisonedDiskLSN mirrors the heap test: a tree
// page whose on-disk PageLSN carries the MaxUint64 sentinel (stamped before
// the vacuum horizon clamp) must not veto physical redo, and applying a redo
// image must heal the page to the entry's real LSN.
func TestApplyPageRedo_OverridesPoisonedDiskLSN(t *testing.T) {
	tr, err := NewBTreeV2(filepath.Join(t.TempDir(), "t.btree"), 16, nil)
	if err != nil {
		t.Fatalf("new btree: %v", err)
	}
	defer func() { _ = tr.Close() }()

	if err := tr.InsertWithLSN(types.IntKey(1), 100, 3); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := tr.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	// Stamp the root/leaf page with the poisoned sentinel.
	pid := pagestore.PageID(1)
	handle, err := tr.bp.FetchForWrite(pid)
	if err != nil {
		t.Fatalf("fetch page: %v", err)
	}
	handle.Page().AdvancePageLSN(math.MaxUint64)
	handle.MarkDirty()
	handle.Release()
	if err := tr.Sync(); err != nil {
		t.Fatalf("sync poisoned page: %v", err)
	}

	image, err := tr.pf.ReadPage(pid)
	if err != nil {
		t.Fatalf("read page image: %v", err)
	}

	applied, err := tr.ApplyPageRedo(pid, image, 7)
	if err != nil {
		t.Fatalf("apply page redo: %v", err)
	}
	if !applied {
		t.Fatal("ApplyPageRedo skipped the entry because the poisoned on-disk PageLSN looked newer")
	}

	onDisk, err := tr.pf.ReadPage(pid)
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
