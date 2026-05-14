package storage_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// TestNewStorageEngine_RejectsMidFileWALCorruption pins down the 6.1 bug:
// when scanMaxWALLSN encounters a corruption error that is NOT a tail
// truncation (e.g. CRC mismatch on a page in the middle of the WAL), the
// engine must refuse to open instead of silently treating the read as the
// end of the log. Returning a too-small maxLSN here causes the engine to
// re-use LSNs that already exist in the WAL, corrupting MVCC ordering.
func TestNewStorageEngine_RejectsMidFileWALCorruption(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	// Seed enough entries to span multiple WAL pages.
	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("heap: %v", err)
	}
	tm := storage.NewTableMenager()
	if err := tm.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	ww, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("wal: %v", err)
	}
	se, err := storage.NewStorageEngine(tm, ww)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}

	// 32 wide records easily exceed PageSize (8KB), so we definitely
	// write into page id >= 2.
	big := make([]byte, 600)
	for i := range big {
		big[i] = 'x'
	}
	for i := 1; i <= 32; i++ {
		if err := se.Put(context.Background(), "users", "id", types.IntKey(i), `{"id":`+itoa(i)+`,"blob":"`+string(big)+`"}`); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if err := se.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Flip a byte in the body of page 1 (mid-file). This breaks the
	// page-level CRC, surfacing pagestore.ErrChecksumMismatch from the
	// WAL reader.
	flipWALByte(t, walPath, int64(pagestore.PageSize+pagestore.HeaderSize+16))

	hm2, err := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("heap2: %v", err)
	}
	tm2 := storage.NewTableMenager()
	if err := tm2.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm2); err != nil {
		t.Fatalf("NewTable2: %v", err)
	}
	ww2, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		// Some WAL writers refuse to open a corrupted file outright;
		// that is also an acceptable signal — the engine never gets a
		// chance to mis-initialise.
		return
	}
	defer ww2.Close()

	se2, err := storage.NewStorageEngine(tm2, ww2)
	if err == nil {
		_ = se2.Close()
		t.Fatal("expected NewStorageEngine to fail when WAL has mid-file corruption")
	}
}

// TestStatsExposesWALTailTruncationsCounter verifies the visibility hook
// described in 6.1: operators can see how many tail truncations the engine
// has tolerated since startup. A freshly opened engine should report 0.
func TestStatsExposesWALTailTruncationsCounter(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	tm := storage.NewTableMenager()
	if err := tm.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	ww, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se, err := storage.NewStorageEngine(tm, ww)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	defer se.Close()

	stats := se.Stats()
	if stats.WALTailTruncations != 0 {
		t.Fatalf("WALTailTruncations: got %d want 0", stats.WALTailTruncations)
	}
}

func flipWALByte(t *testing.T, path string, offset int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	buf := []byte{0}
	if _, err := f.ReadAt(buf, offset); err != nil {
		t.Fatalf("read at %d: %v", offset, err)
	}
	buf[0] ^= 0x80
	if _, err := f.WriteAt(buf, offset); err != nil {
		t.Fatalf("write at %d: %v", offset, err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	if neg {
		digits = append([]byte{'-'}, digits...)
	}
	return string(digits)
}
