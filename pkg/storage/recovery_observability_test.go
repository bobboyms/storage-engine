package storage

import (
	"context"
	"sync"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// openRecoveryEngineWithOptions mirrors openRecoveryEngine(production=true)
// but threads Options so a test can capture the RecoveryEvent emitted at
// the end of the implicit Recover call.
func openRecoveryEngineWithOptions(t *testing.T, fx recoveryFixture, opts Options) *StorageEngine {
	t.Helper()

	hm, err := NewHeapForTable(HeapFormatV2, fx.heapPath)
	if err != nil {
		t.Fatalf("NewHeapForTable: %v", err)
	}
	idxTree, err := NewBTreeForIndex(BTreeFormatV2, true, TypeInt, fx.indexPath, nil)
	if err != nil {
		t.Fatalf("NewBTreeForIndex: %v", err)
	}

	meta := NewTableMenager()
	if err := meta.NewTable(fx.tableName, []Index{
		{Name: "id", Primary: true, Type: TypeInt, Tree: idxTree},
	}, 0, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}

	ww, err := wal.NewWALWriter(fx.walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("NewWALWriter: %v", err)
	}

	se, err := NewProductionStorageEngineWithOptions(meta, ww, opts)
	if err != nil {
		_ = ww.Close()
		t.Fatalf("NewProductionStorageEngineWithOptions: %v", err)
	}
	return se
}

// TestRecoveryEvent_ReportsUndoPhaseMetrics verifies that the
// RecoveryEvent surfaces the undo phase work (loser transactions undone,
// compensation log records applied) and a non-zero recovery duration, so
// operators can monitor recovery cost.
func TestRecoveryEvent_ReportsUndoPhaseMetrics(t *testing.T) {
	dir := t.TempDir()
	fx := newRecoveryFixture(dir, "users")

	// Committed baseline so the table file exists and is consistent.
	base := openRecoveryEngine(t, fx, false)
	if err := base.Put(context.Background(), "users", "id", types.IntKey(1), `{"id":1,"name":"committed"}`); err != nil {
		t.Fatalf("base Put: %v", err)
	}
	if err := base.Close(); err != nil {
		t.Fatalf("base Close: %v", err)
	}

	// Inject a loser transaction: BEGIN + two inserts, no COMMIT.
	inject := openRecoveryEngine(t, fx, false)
	const loserTx uint64 = 7777
	writeTxMarkerForTest(t, inject.WAL, loserTx, 10, wal.EntryBegin)
	writeTxDocumentForTest(t, inject.WAL, loserTx, 11, "users", "id", types.IntKey(100), `{"id":100,"name":"loser-a"}`)
	writeTxDocumentForTest(t, inject.WAL, loserTx, 12, "users", "id", types.IntKey(101), `{"id":101,"name":"loser-b"}`)
	if err := inject.Close(); err != nil {
		t.Fatalf("inject Close: %v", err)
	}

	var (
		mu sync.Mutex
		ev RecoveryEvent
	)
	recovered := openRecoveryEngineWithOptions(t, fx, Options{
		Listener: EventListener{
			OnRecoveryComplete: func(e RecoveryEvent) {
				mu.Lock()
				ev = e
				mu.Unlock()
			},
		},
	})
	defer recovered.Close()

	mu.Lock()
	defer mu.Unlock()
	if ev.LoserTxsUndone != 1 {
		t.Fatalf("LoserTxsUndone=%d, expected 1", ev.LoserTxsUndone)
	}
	if ev.CLRsApplied != 2 {
		t.Fatalf("CLRsApplied=%d, expected 2 (one per loser insert)", ev.CLRsApplied)
	}
	if ev.Duration <= 0 {
		t.Fatalf("Duration=%v, expected a positive recovery duration", ev.Duration)
	}
}
