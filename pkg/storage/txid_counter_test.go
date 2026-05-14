package storage_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// TestStatsNextTxID_StartsAtOneOnFreshEngine pins down the 6.2 contract:
// the txID counter is independent from the LSN sequence and starts at 0,
// so Stats reports NextTxID == 1 before any work has been done.
func TestStatsNextTxID_StartsAtOneOnFreshEngine(t *testing.T) {
	tmpDir := t.TempDir()
	hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, filepath.Join(tmpDir, "heap.data"))
	tm := storage.NewTableMenager()
	if err := tm.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	ww, _ := wal.NewWALWriter(filepath.Join(tmpDir, "wal.log"), wal.DefaultOptions())
	se, err := storage.NewStorageEngine(tm, ww)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	defer se.Close()

	if got := se.Stats().NextTxID; got != 1 {
		t.Fatalf("NextTxID on fresh engine: got %d want 1", got)
	}
}

// TestStatsNextTxID_DoesNotInheritWALMaxLSN locks in the decoupling: after
// re-opening an engine on a WAL whose entries already pushed the LSN
// forward, the txID counter must NOT be initialised from that LSN. txIDs
// are process-local; restart resets them.
func TestStatsNextTxID_DoesNotInheritWALMaxLSN(t *testing.T) {
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

	// Push the WAL well past LSN 0.
	for i := 1; i <= 5; i++ {
		doc := `{"id":` + itoa(i) + `}`
		if err := se.Put(context.Background(), "users", "id", types.IntKey(i), doc); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	beforeStats := se.Stats()
	if beforeStats.CurrentLSN < 5 {
		t.Fatalf("CurrentLSN before close: got %d want >=5", beforeStats.CurrentLSN)
	}
	if err := se.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Re-open the engine; CurrentLSN should jump back to maxLSN of WAL,
	// but NextTxID must restart at 1.
	hm2, _ := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	tm2 := storage.NewTableMenager()
	if err := tm2.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm2); err != nil {
		t.Fatalf("NewTable2: %v", err)
	}
	ww2, _ := wal.NewWALWriter(walPath, wal.DefaultOptions())
	se2, err := storage.NewStorageEngine(tm2, ww2)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer se2.Close()

	stats := se2.Stats()
	if stats.NextTxID != 1 {
		t.Fatalf("NextTxID after reopen: got %d want 1 (independent of LSN=%d)", stats.NextTxID, stats.CurrentLSN)
	}
	if stats.CurrentLSN == 0 {
		t.Fatalf("expected CurrentLSN to inherit WAL maxLSN, got 0")
	}
}
