package storage

import (
	"context"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"testing"

	v2 "github.com/bobboyms/storage-engine/pkg/heap/v2"
	"github.com/bobboyms/storage-engine/pkg/pagestore"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// openTwoTableEngine builds (or reopens) a production engine over dir with two
// VARCHAR-keyed tables "a" and "b" sharing one WAL, mirroring how pkg/sql
// wires a database. Close the returned engine before reopening the dir.
func openTwoTableEngine(t *testing.T, dir string) *StorageEngine {
	t.Helper()

	meta := NewTableMenager()
	for _, name := range []string{"a", "b"} {
		hm, err := NewHeapForTable(HeapFormatV2, filepath.Join(dir, name+".heap"))
		if err != nil {
			t.Fatalf("create heap %s: %v", name, err)
		}
		if err := meta.NewTable(name, []Index{
			{Name: "id", Primary: true, Type: TypeVarchar, Unique: true},
		}, 0, hm); err != nil {
			t.Fatalf("create table %s: %v", name, err)
		}
	}

	walWriter, err := wal.NewWALWriter(filepath.Join(dir, "data.wal"), wal.DefaultOptions())
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}
	se, err := NewProductionStorageEngine(meta, walWriter)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	return se
}

func insertVarcharRow(t *testing.T, se *StorageEngine, table, id string) {
	t.Helper()
	doc := fmt.Sprintf(`{"id": "%s"}`, id)
	keys := map[string]types.Comparable{"id": types.VarcharKey(id)}
	if err := se.InsertRow(context.Background(), table, doc, keys); err != nil {
		t.Fatalf("insert %s into %s: %v", id, table, err)
	}
}

func countVisibleRows(t *testing.T, se *StorageEngine, table string) int {
	t.Helper()
	it, err := se.NewIterator(context.Background(), table, "id", IterOptions{})
	if err != nil {
		t.Fatalf("iterator on %s: %v", table, err)
	}
	defer func() { _ = it.Close() }()
	n := 0
	for it.Next() {
		n++
	}
	if err := it.Err(); err != nil {
		t.Fatalf("scan %s: %v", table, err)
	}
	return n
}

// maxWALEntryLSN scans every WAL segment under walPath and returns the
// highest entry header LSN found.
func maxWALEntryLSN(t *testing.T, walPath string) uint64 {
	t.Helper()
	reader, err := wal.NewWALReader(walPath)
	if err != nil {
		t.Fatalf("open WAL reader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	var maxLSN uint64
	for {
		entry, err := reader.ReadEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read WAL entry: %v", err)
		}
		if entry.Header.LSN > maxLSN {
			maxLSN = entry.Header.LSN
		}
		wal.ReleaseEntry(entry)
	}
	return maxLSN
}

// TestCheckpoint_ClampsPoisonedPageLSN guards the WAL against pages whose
// PageLSN already carries the MaxUint64 vacuum sentinel (stamped by a binary
// from before the Vacuum horizon clamp). Neither the before-flush page-redo
// hook nor the checkpoint record may copy such a PageLSN into a WAL entry
// header, where the next open would adopt it as the current LSN.
func TestCheckpoint_ClampsPoisonedPageLSN(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	se := openTwoTableEngine(t, dir)
	defer func() { _ = se.Close() }()

	for i := 0; i < 3; i++ {
		insertVarcharRow(t, se, "a", fmt.Sprintf("a-%d", i))
	}
	if _, err := se.Del(ctx, "a", "id", types.VarcharKey("a-0")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Flush everything so the poisoned page below is the only dirty one and
	// also drives the checkpoint's beginLSN.
	if err := se.FuzzyCheckpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	// Stamp the page the way a pre-fix binary did: vacuum the heap directly
	// with the raw idle-registry sentinel, bypassing the engine's clamp.
	table, err := se.TableMetaData.GetTableByName("a")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	heapV2, ok := table.Heap.(*v2.HeapV2)
	if !ok {
		t.Fatalf("table a heap is %T, want *v2.HeapV2", table.Heap)
	}
	reclaimed, err := heapV2.Vacuum(ctx, math.MaxUint64)
	if err != nil {
		t.Fatalf("heap vacuum: %v", err)
	}
	if reclaimed == 0 {
		t.Fatal("heap vacuum reclaimed nothing; test setup no longer creates a tombstone")
	}

	if err := se.FuzzyCheckpoint(ctx); err != nil {
		t.Fatalf("checkpoint after poisoned vacuum: %v", err)
	}

	currentLSN := se.Stats().CurrentLSN
	if got := maxWALEntryLSN(t, filepath.Join(dir, "data.wal")); got > currentLSN {
		t.Fatalf("WAL contains entry LSN %d beyond current LSN %d (poisoned PageLSN leaked into the log)", got, currentLSN)
	}
}

// TestRecover_IgnoresPoisonedWALLSN covers directories written by a binary
// from before the vacuum horizon clamp: their WAL already contains entries
// whose header LSN is the MaxUint64 sentinel (page-redo images of stamped
// pages, and checkpoint records whose beginLSN came from such a page).
// Opening such a directory must derive the next LSN from the real entries
// and must not trust the poisoned checkpoint's beginLSN.
func TestRecover_IgnoresPoisonedWALLSN(t *testing.T) {
	dir := t.TempDir()

	se := openTwoTableEngine(t, dir)
	for i := 0; i < 3; i++ {
		insertVarcharRow(t, se, "a", fmt.Sprintf("a-%d", i))
	}
	lastLSN := se.Stats().CurrentLSN
	if err := se.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Poison the WAL the way the old binary did.
	walWriter, err := wal.NewWALWriter(filepath.Join(dir, "data.wal"), wal.DefaultOptions())
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	payload, err := serializePageRedoPayload(filepath.Join(dir, "ghost.heap"), 1, &pagestore.Page{})
	if err != nil {
		t.Fatalf("build page redo payload: %v", err)
	}
	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = wal.WALVersion
	entry.Header.EntryType = wal.EntryPageRedo
	entry.Header.LSN = math.MaxUint64
	entry.Header.PayloadLen = uint32(len(payload)) //nolint:gosec // small test payload
	entry.Header.CRC32 = wal.CalculateCRC32(payload)
	entry.Payload = append(entry.Payload, payload...)
	if err := walWriter.WriteEntry(entry); err != nil {
		t.Fatalf("write poisoned page redo: %v", err)
	}
	wal.ReleaseEntry(entry)
	if err := walWriter.WriteCheckpointRecord(math.MaxUint64); err != nil {
		t.Fatalf("write poisoned checkpoint: %v", err)
	}
	if err := walWriter.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	se = openTwoTableEngine(t, dir)
	defer func() { _ = se.Close() }()

	if got := se.Stats().CurrentLSN; got != lastLSN {
		t.Fatalf("CurrentLSN after reopening poisoned dir = %d, want %d", got, lastLSN)
	}
	// The poisoned checkpoint must not have suppressed the logical replay.
	if got := countVisibleRows(t, se, "a"); got != 3 {
		t.Fatalf("table a has %d visible rows after reopening poisoned dir, want 3", got)
	}

	insertVarcharRow(t, se, "a", "a-new")
	if got := countVisibleRows(t, se, "a"); got != 4 {
		t.Fatalf("table a has %d visible rows after first write, want 4", got)
	}
}

// TestVacuum_IdleHorizonDoesNotPoisonLSN is the regression test for the
// "one INSERT hides every other table until reopen" bug: with no active
// transactions, Vacuum used GetMinActiveLSN() == MaxUint64 as its horizon and
// stamped it into the compacted page's PageLSN. The before-flush hook then
// wrote that sentinel as a WAL entry LSN, reopen adopted it as the current
// LSN, and the first write wrapped the counter to 0 — making every previously
// committed row invisible to new snapshots.
func TestVacuum_IdleHorizonDoesNotPoisonLSN(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	se := openTwoTableEngine(t, dir)
	for i := 0; i < 5; i++ {
		insertVarcharRow(t, se, "a", fmt.Sprintf("a-%d", i))
		insertVarcharRow(t, se, "b", fmt.Sprintf("b-%d", i))
	}
	// Flush everything so the delete's pages are the only dirty ones when the
	// vacuum + checkpoint pass runs (the maintenance-pass shape).
	if err := se.FuzzyCheckpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	if _, err := se.Del(ctx, "a", "id", types.VarcharKey("a-0")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// No transaction is active here, so the vacuum horizon falls back to
	// "everything committed is reclaimable".
	if err := se.Vacuum(ctx, "a"); err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if err := se.FuzzyCheckpoint(ctx); err != nil {
		t.Fatalf("checkpoint after vacuum: %v", err)
	}
	lastLSN := se.Stats().CurrentLSN
	if err := se.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	se = openTwoTableEngine(t, dir)
	defer func() { _ = se.Close() }()

	if got := se.Stats().CurrentLSN; got != lastLSN {
		t.Fatalf("CurrentLSN after reopen = %d, want %d (a poisoned WAL LSN leaked into recovery)", got, lastLSN)
	}

	// The first write after reopen must not wrap the LSN counter.
	insertVarcharRow(t, se, "a", "a-new")
	if got := se.Stats().CurrentLSN; got <= lastLSN || got == math.MaxUint64 {
		t.Fatalf("CurrentLSN after first write = %d, want > %d (LSN counter wrapped)", got, lastLSN)
	}
	if got := countVisibleRows(t, se, "b"); got != 5 {
		t.Fatalf("table b has %d visible rows after a write to table a, want 5", got)
	}
	if got := countVisibleRows(t, se, "a"); got != 5 {
		t.Fatalf("table a has %d visible rows, want 5 (4 surviving + 1 new)", got)
	}
}
