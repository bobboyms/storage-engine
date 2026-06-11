package storage

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func newLimitTestEngine(t *testing.T, maxTxBytes int64) *StorageEngine {
	t.Helper()
	tmpDir := t.TempDir()

	hm, err := NewHeapForTable(HeapFormatV2, filepath.Join(tmpDir, "heap.data"))
	if err != nil {
		t.Fatalf("NewHeapForTable: %v", err)
	}
	tableMgr := NewTableMenager()
	tableMgr.NewTable("users", []Index{{Name: "id", Primary: true, Type: TypeInt}}, 3, hm)

	ww, err := wal.NewWALWriter(filepath.Join(tmpDir, "wal.log"), wal.DefaultOptions())
	if err != nil {
		t.Fatalf("NewWALWriter: %v", err)
	}
	se, err := NewStorageEngineWithOptions(tableMgr, ww, Options{MaxTxWriteSetBytes: maxTxBytes})
	if err != nil {
		t.Fatalf("NewStorageEngineWithOptions: %v", err)
	}
	t.Cleanup(func() { _ = ww.Close() })
	return se
}

// TestWriteTransaction_WriteSetLimit verifies the OOM guard for the
// no-steal design: a transaction buffers its entire write set in memory
// until Commit, so an unbounded transaction can take the process down.
// Once the configured byte budget is exceeded the next buffered operation
// must fail with ErrTxWriteSetLimit while the transaction stays usable
// and previously buffered operations still commit.
func TestWriteTransaction_WriteSetLimit(t *testing.T) {
	se := newLimitTestEngine(t, 1024)
	ctx := context.Background()

	tx := se.BeginWriteTransaction()
	smallDoc := `{"name":"ok"}`
	if err := tx.Put(ctx, "users", "id", types.IntKey(1), smallDoc); err != nil {
		t.Fatalf("small Put within budget: %v", err)
	}

	bigDoc := `{"blob":"` + strings.Repeat("x", 4096) + `"}`
	err := tx.Put(ctx, "users", "id", types.IntKey(2), bigDoc)
	if err == nil {
		t.Fatal("expected Put to fail once the transaction write-set budget is exceeded")
	}
	if !errors.Is(err, ErrTxWriteSetLimit) {
		t.Fatalf("expected ErrTxWriteSetLimit, got: %v", err)
	}

	// The transaction must remain usable and commit what was accepted.
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit after rejected oversized op: %v", err)
	}
	if _, found, err := se.GetBytes(ctx, "users", "id", types.IntKey(1)); err != nil || !found {
		t.Fatalf("accepted row must be committed, found=%v err=%v", found, err)
	}
	if _, found, err := se.GetBytes(ctx, "users", "id", types.IntKey(2)); err != nil || found {
		t.Fatalf("rejected row must not exist, found=%v err=%v", found, err)
	}
}

// TestWriteTransaction_WriteSetLimitUnlimitedByDefault guards backward
// compatibility: with no limit configured, large write sets keep working.
func TestWriteTransaction_WriteSetLimitUnlimitedByDefault(t *testing.T) {
	se := newLimitTestEngine(t, 0)
	ctx := context.Background()

	tx := se.BeginWriteTransaction()
	bigDoc := `{"blob":"` + strings.Repeat("x", 4096) + `"}`
	for i := 1; i <= 8; i++ {
		if err := tx.Put(ctx, "users", "id", types.IntKey(int64(i)), bigDoc); err != nil {
			t.Fatalf("Put %d with unlimited budget: %v", i, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}
