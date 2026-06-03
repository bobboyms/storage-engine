package storage

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// openHealEngine builds an engine over fresh files. Returns the engine
// and the on-disk paths so the test can reopen if needed.
func openHealEngine(t *testing.T, walPath, heapPath string, opts Options) *StorageEngine {
	t.Helper()
	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("new heap: %v", err)
	}
	tableMgr := NewTableMenager()
	if err := tableMgr.NewTable("users", []Index{{Name: "id", Primary: true, Type: TypeInt}}, 4, hm); err != nil {
		t.Fatalf("new table: %v", err)
	}
	ww, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("new wal writer: %v", err)
	}
	se, err := NewProductionStorageEngineWithOptions(tableMgr, ww, opts)
	if err != nil {
		_ = ww.Close()
		t.Fatalf("new engine: %v", err)
	}
	return se
}

// degradeViaFailedApply commits a two-op transaction whose post-commit
// apply is forced to fail at the first op, leaving the engine degraded.
func degradeViaFailedApply(t *testing.T, se *StorageEngine) {
	t.Helper()
	injected := errors.New("injected post-commit apply failure")
	se.testHooks.onPostCommitApplyStage = func(info postCommitApplyInfo) error {
		if info.Stage == postCommitStageAfterHeapMutation && info.Step == 1 {
			return injected
		}
		return nil
	}
	tx := se.BeginWriteTransaction()
	if err := tx.Put(context.Background(), "users", "id", types.IntKey(1), `{"id":1,"name":"Alice"}`); err != nil {
		t.Fatalf("put k1: %v", err)
	}
	if err := tx.Put(context.Background(), "users", "id", types.IntKey(2), `{"id":2,"name":"Bob"}`); err != nil {
		t.Fatalf("put k2: %v", err)
	}
	if err := tx.Commit(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("expected injected apply failure on commit, got %v", err)
	}
	if !se.Stats().DegradedSince {
		t.Fatalf("engine should be degraded after failed post-commit apply")
	}
}

// TestHeal_RecoversDegradedEngineInPlace verifies that a degraded engine
// can be recovered in-process via Heal, without closing and reopening,
// and that the committed transaction converges to its full state.
func TestHeal_RecoversDegradedEngineInPlace(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	se := openHealEngine(t, filepath.Join(dir, "wal.log"), filepath.Join(dir, "heap.data"), Options{})
	defer se.Close()

	degradeViaFailedApply(t, se)

	// While degraded, reads are blocked.
	if _, _, err := getDocString(t, se, "users", "id", types.IntKey(1)); !errors.Is(err, ErrEngineDegraded) {
		t.Fatalf("expected degraded read before heal, got %v", err)
	}

	// Clear the injected fault (it has been "resolved") and heal in place.
	se.testHooks.onPostCommitApplyStage = nil
	if err := se.Heal(ctx); err != nil {
		t.Fatalf("Heal: %v", err)
	}

	if se.Stats().DegradedSince {
		t.Fatalf("engine should no longer be degraded after Heal")
	}

	// Both committed keys are now visible without reopening.
	if doc, found, err := getDocString(t, se, "users", "id", types.IntKey(1)); err != nil || !found || doc != `{"id":1,"name":"Alice"}` {
		t.Fatalf("k1 after heal: found=%v doc=%q err=%v", found, doc, err)
	}
	if doc, found, err := getDocString(t, se, "users", "id", types.IntKey(2)); err != nil || !found || doc != `{"id":2,"name":"Bob"}` {
		t.Fatalf("k2 after heal: found=%v doc=%q err=%v", found, doc, err)
	}

	// Writes are accepted again.
	if err := se.Put(ctx, "users", "id", types.IntKey(3), `{"id":3,"name":"Carol"}`); err != nil {
		t.Fatalf("put after heal: %v", err)
	}
}

// TestHeal_NoOpWhenHealthy verifies Heal is a no-op on a healthy engine.
func TestHeal_NoOpWhenHealthy(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	se := openHealEngine(t, filepath.Join(dir, "wal.log"), filepath.Join(dir, "heap.data"), Options{})
	defer se.Close()

	if err := se.Put(ctx, "users", "id", types.IntKey(1), `{"id":1}`); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := se.Heal(ctx); err != nil {
		t.Fatalf("Heal on healthy engine should be a no-op, got %v", err)
	}
	if se.Stats().DegradedSince {
		t.Fatalf("healthy engine must not become degraded after Heal")
	}
}
