package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/codec/bsoncodec"
	heapv2 "github.com/bobboyms/storage-engine/pkg/heap/v2"
	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestRebuildIndex_RepairsDanglingPrimaryPointer(t *testing.T) {
	ctx := context.Background()
	se := newVerifyEngine(t, t.TempDir())
	table := usersTable(t, se)

	if err := table.Indices["id"].Tree.Replace(types.VarcharKey("u-0"), heapv2.EncodeRecordID(99, 3)); err != nil {
		t.Fatalf("corrupt index pointer: %v", err)
	}
	if report := mustVerifyTables(t, se); !report.HasErrors() {
		t.Fatal("setup did not corrupt the index")
	}

	if err := RebuildIndex(ctx, se.TableMetaData, nil, "users", "id"); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	if report := mustVerifyTables(t, se); len(report.Findings) != 0 {
		t.Fatalf("findings after rebuild: %v", report.Findings)
	}
	if got := countVisibleRows(t, se, "users"); got != 3 {
		t.Fatalf("users has %d visible rows after rebuild, want 3", got)
	}
}

func TestRebuildIndex_RepairsSecondaryAfterLostEntry(t *testing.T) {
	ctx := context.Background()
	se := newVerifyEngine(t, t.TempDir())
	table := usersTable(t, se)

	physical := types.NewCompositeKey(types.VarcharKey("n-0"), types.VarcharKey("u-0"))
	if _, err := table.Indices["name"].Tree.Remove(physical); err != nil {
		t.Fatalf("remove secondary entry: %v", err)
	}
	if report := mustVerifyTables(t, se); !report.HasErrors() {
		t.Fatal("setup did not corrupt the index")
	}

	if err := RebuildIndex(ctx, se.TableMetaData, nil, "users", "name"); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	if report := mustVerifyTables(t, se); len(report.Findings) != 0 {
		t.Fatalf("findings after rebuild: %v", report.Findings)
	}
}

// TestRebuildIndex_AbortsOnDuplicateKeys pins the conflict policy: when two
// live heap records claim the same key on a unique index, the heap is
// ambiguous and the rebuild must fail without replacing the existing index.
func TestRebuildIndex_AbortsOnDuplicateKeys(t *testing.T) {
	ctx := context.Background()
	se := newVerifyEngine(t, t.TempDir())
	table := usersTable(t, se)

	// A second live record claiming pk "u-0", written behind the index's back.
	doc, err := bsoncodec.New().Parse(`{"id": "u-0", "name": "imposter"}`)
	if err != nil {
		t.Fatalf("parse doc: %v", err)
	}
	raw, err := doc.Bytes()
	if err != nil {
		t.Fatalf("encode doc: %v", err)
	}
	if _, err := table.Heap.Write(raw, 99, heapv2.NoRecordID); err != nil {
		t.Fatalf("write duplicate: %v", err)
	}

	err = RebuildIndex(ctx, se.TableMetaData, nil, "users", "id")
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("RebuildIndex with ambiguous heap returned %v, want duplicate-key error", err)
	}

	// The original index must have survived the aborted rebuild.
	if got := countVisibleRows(t, se, "users"); got != 3 {
		t.Fatalf("users has %d visible rows after aborted rebuild, want 3", got)
	}
}

// TestRebuildIndex_RecoveryModeResolvesDuplicatesByCreateLSN pins the crash
// recovery policy: when an eviction persisted an UPDATE's new version but not
// the page holding the old version's delete mark, both look live. The rebuild
// must keep the newest CreateLSN and re-stamp the lost delete mark instead of
// aborting.
func TestRebuildIndex_RecoveryModeResolvesDuplicatesByCreateLSN(t *testing.T) {
	ctx := context.Background()
	se := newVerifyEngine(t, t.TempDir())
	table := usersTable(t, se)

	// A second live record claiming pk "u-0" with a NEWER CreateLSN — the
	// shape a lost delete mark leaves behind.
	doc, err := bsoncodec.New().Parse(`{"id": "u-0", "name": "n-0"}`)
	if err != nil {
		t.Fatalf("parse doc: %v", err)
	}
	raw, err := doc.Bytes()
	if err != nil {
		t.Fatalf("encode doc: %v", err)
	}
	newerLSN := uint64(999)
	winnerRid, err := table.Heap.Write(raw, newerLSN, heapv2.NoRecordID)
	if err != nil {
		t.Fatalf("write duplicate: %v", err)
	}
	// Recovery always finishes with the tracker at the WAL's max LSN, so
	// snapshots taken after it can see the surviving version.
	se.lsnTracker.Set(newerLSN)

	if err := rebuildIndex(ctx, se.TableMetaData, nil, "users", "id", true); err != nil {
		t.Fatalf("recovery-mode rebuild: %v", err)
	}

	offset, found, err := table.Indices["id"].Tree.Get(types.VarcharKey("u-0"))
	if err != nil || !found {
		t.Fatalf("u-0 lookup after rebuild: found=%v err=%v", found, err)
	}
	if offset != winnerRid {
		t.Fatalf("u-0 points at rid %d, want the newer record %d", offset, winnerRid)
	}
	if got := countVisibleRows(t, se, "users"); got != 3 {
		t.Fatalf("users has %d visible rows after rebuild, want 3", got)
	}
	// The older duplicate must carry the re-stamped delete mark.
	liveCopies := 0
	heapV2 := table.Heap.(*heapv2.HeapV2)
	err = heapV2.ForEachRecord(ctx, func(_ int64, rh heapv2.RecordHeader, _ []byte) error {
		if rh.Valid {
			liveCopies++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan heap: %v", err)
	}
	if liveCopies != 3 {
		t.Fatalf("heap has %d live records, want 3 (loser re-stamped as deleted)", liveCopies)
	}
}
