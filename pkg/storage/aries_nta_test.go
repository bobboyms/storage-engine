package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// TestARIES_SplitEmitsNTABeginCommitPair pins the structural NTA
// invariant: every B+ tree split crossed during inserts produces a
// matching EntryNTABegin + EntryNTACommit, with the before-image of
// the modified page captured in the Begin payload.
func TestARIES_SplitEmitsNTABeginCommitPair(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("heap: %v", err)
	}
	tm := NewTableMenager()
	if err := tm.NewTable("users", []Index{{Name: "id", Primary: true, Type: TypeInt}}, 3, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	ww, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("wal: %v", err)
	}
	se, err := NewStorageEngine(tm, ww)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	t.Cleanup(func() { _ = se.Close() })

	// Insert enough keys to force at least one leaf split on the
	// primary index. Each leaf holds hundreds of fixed-key slots, so
	// we need a few thousand inserts to trigger the first split.
	for i := 1; i <= 2000; i++ {
		doc := `{"id":` + itoa(i) + `}`
		if err := se.Put(context.Background(), "users", "id", types.IntKey(i), doc); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	_ = se.WAL.Sync()

	reader, err := wal.NewWALReaderWithCipher(walPath, ww.Cipher())
	if err != nil {
		t.Fatalf("wal reader: %v", err)
	}
	defer reader.Close()

	begins := 0
	commits := 0
	openBegins := map[uint64]struct{}{}
	for {
		entry, err := reader.ReadEntry()
		if err != nil {
			break
		}
		switch entry.Header.EntryType {
		case wal.EntryNTABegin:
			begins++
			openBegins[entry.Header.LSN] = struct{}{}
			kind, pages, perr := deserializeNTABeginPayload(entry.Payload)
			if perr != nil {
				t.Fatalf("deserializeNTABeginPayload: %v", perr)
			}
			if kind != NTAKindBTreeSplit {
				t.Fatalf("unexpected NTA kind %d", kind)
			}
			if len(pages) == 0 {
				t.Fatal("NTA Begin must carry at least one before-image")
			}
		case wal.EntryNTACommit:
			commits++
			ntaID, perr := deserializeNTACommitPayload(entry.Payload)
			if perr != nil {
				t.Fatalf("deserializeNTACommitPayload: %v", perr)
			}
			if _, ok := openBegins[ntaID]; !ok {
				t.Fatalf("NTA Commit references unknown Begin LSN %d", ntaID)
			}
			delete(openBegins, ntaID)
		}
		wal.ReleaseEntry(entry)
	}

	if begins == 0 {
		t.Fatal("expected at least one EntryNTABegin from leaf splits")
	}
	if begins != commits {
		t.Fatalf("NTA imbalance: %d begins vs %d commits", begins, commits)
	}
	if len(openBegins) != 0 {
		t.Fatalf("dangling Begin LSNs: %v", openBegins)
	}
}

// TestARIES_PartialNTARolledBackOnRecovery checks that a Begin without
// a matching Commit (simulated by appending a Begin with a captured
// before-image and skipping the Commit) is reverted on recovery: the
// page on disk goes back to the before-image bytes captured by the
// Begin entry.
func TestARIES_PartialNTARolledBackOnRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("heap: %v", err)
	}
	tm := NewTableMenager()
	if err := tm.NewTable("users", []Index{{Name: "id", Primary: true, Type: TypeInt}}, 3, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	ww, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("wal: %v", err)
	}
	se, err := NewStorageEngine(tm, ww)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}

	// Seed multiple rows so the heap actually has page 1 allocated.
	for i := 1; i <= 16; i++ {
		if err := se.Put(context.Background(), "users", "id", types.IntKey(i), `{"id":`+itoa(i)+`}`); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if err := se.FuzzyCheckpoint(context.Background()); err != nil {
		t.Fatalf("FuzzyCheckpoint: %v", err)
	}

	// Capture the current bytes of heap page 1; this is our
	// synthetic before-image. Then write garbage to the same page on
	// disk and feed the engine an NTA Begin carrying the original.
	preImage := make([]byte, 8192)
	if err := readPageBytes(heapPath, 1, preImage); err != nil {
		t.Fatalf("read heap page 1: %v", err)
	}

	garbage := make([]byte, 8192)
	for i := range garbage {
		garbage[i] = 0xCC
	}
	if err := writePageBytes(heapPath, 1, garbage); err != nil {
		t.Fatalf("clobber heap page 1: %v", err)
	}

	if _, err := se.writeNTABegin(NTAKindBTreeSplit, []NTAPage{{Path: heapPath, PageID: 1, PreImage: preImage}}); err != nil {
		t.Fatalf("writeNTABegin: %v", err)
	}
	// Deliberately skip the Commit.
	if err := se.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen — recovery must restore the original page.
	hm2, _ := NewHeapForTable(HeapFormatV2, heapPath)
	tm2 := NewTableMenager()
	if err := tm2.NewTable("users", []Index{{Name: "id", Primary: true, Type: TypeInt}}, 3, hm2); err != nil {
		t.Fatalf("NewTable2: %v", err)
	}
	ww2, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se2, err := NewProductionStorageEngine(tm2, ww2)
	if err != nil {
		t.Fatalf("NewProductionStorageEngine: %v", err)
	}
	defer se2.Close()

	gotPage := make([]byte, 8192)
	if err := readPageBytes(heapPath, 1, gotPage); err != nil {
		t.Fatalf("post-recovery read: %v", err)
	}
	if !bytesEqual(gotPage, preImage) {
		t.Fatalf("partial NTA was not rolled back: page bytes still differ from before-image")
	}
}

// helpers — direct file I/O on the page file so we can simulate
// crash-mid-NTA without touching the buffer pool.

func readPageBytes(path string, pageID uint64, buf []byte) error {
	return readPageFile(path, pageID, buf)
}

func writePageBytes(path string, pageID uint64, data []byte) error {
	return writePageFile(path, pageID, data)
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
