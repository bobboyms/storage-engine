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
