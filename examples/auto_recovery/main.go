// Demonstrates in-process recovery from a degraded engine.
//
// Background: a WriteTransaction writes its full BEGIN+ops+COMMIT to the
// WAL durably, then applies the changes to the heap and indexes. If that
// post-commit apply hits a fault (e.g. a transient I/O error), the engine
// marks itself "degraded" and refuses all traffic with
// storage.ErrEngineDegraded — the COMMIT is durable but the in-memory
// state is partial.
//
// Two ways to recover without restarting the process:
//
//  1. Explicit: catch storage.ErrEngineDegraded and call se.Heal(ctx),
//     which replays the WAL idempotently against the open files and
//     restores full consistency. See putWithHeal below.
//
//  2. Automatic: open the engine with Options.AutoHealAfterApplyFailure
//     so Commit heals itself; callers never observe the degraded state.
//
// This program wires both, runs a normal workload, and prints the engine
// stats. (Forcing a real apply fault requires package-internal hooks, so
// here the workload succeeds and the recovery paths stay dormant — the
// point is to show the production-grade wiring.)
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func main() {
	tmpDir, err := os.MkdirTemp("", "auto_recovery_demo")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Printf("Database: %s\n\n", tmpDir)

	// A listener makes the degrade/heal lifecycle observable:
	//   - OnBackgroundError fires when a post-commit apply fault degrades
	//     the engine;
	//   - OnRecoveryComplete fires for every replay, including an in-place
	//     Heal, and now reports the undo phase and duration.
	listener := storage.EventListener{
		OnBackgroundError: func(err error) {
			fmt.Printf("degraded: %v\n", err)
		},
		OnRecoveryComplete: func(ev storage.RecoveryEvent) {
			fmt.Printf("recovery: logical=%d/%d clrs=%d losers=%d duration=%s\n",
				ev.LogicalApplied, ev.LogicalApplied+ev.LogicalSkipped,
				ev.CLRsApplied, ev.LoserTxsUndone, ev.Duration)
		},
	}

	heapPath := filepath.Join(tmpDir, "accounts.heap")
	walPath := filepath.Join(tmpDir, "accounts.wal")

	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	if err != nil {
		log.Fatalf("NewHeapForTable: %v", err)
	}
	meta := storage.NewTableMenager()
	if err := meta.NewTable("accounts", []storage.Index{
		{Name: "id", Type: storage.TypeInt, Primary: true},
	}, 4, hm); err != nil {
		log.Fatalf("NewTable: %v", err)
	}
	ww, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		log.Fatalf("NewWALWriter: %v", err)
	}

	// AutoHealAfterApplyFailure makes Commit self-heal on a post-commit
	// apply fault, so callers never see a degraded engine.
	se, err := storage.NewProductionStorageEngineWithOptions(meta, ww, storage.Options{
		Listener:                  listener,
		AutoHealAfterApplyFailure: true,
	})
	if err != nil {
		log.Fatalf("NewProductionStorageEngineWithOptions: %v", err)
	}
	defer se.Close()

	ctx := context.Background()

	// Normal workload — commits succeed and apply cleanly.
	fmt.Println("--- workload ---")
	for i := 1; i <= 3; i++ {
		if err := putWithHeal(ctx, se, "accounts", "id", types.IntKey(i),
			fmt.Sprintf(`{"id": %d, "balance": %d}`, i, i*100)); err != nil {
			log.Fatalf("put %d: %v", i, err)
		}
		fmt.Printf("put account %d\n", i)
	}

	// Heal is always safe to call: on a healthy engine it is a no-op.
	if err := se.Heal(ctx); err != nil {
		log.Fatalf("Heal on healthy engine should be a no-op: %v", err)
	}

	st := se.Stats()
	fmt.Println("\n--- engine.Stats() ---")
	fmt.Printf("CurrentLSN     = %d\n", st.CurrentLSN)
	fmt.Printf("Recoveries     = %d\n", st.Recoveries)
	fmt.Printf("DegradedSince  = %v\n", st.DegradedSince)
}

// putWithHeal shows the explicit recovery pattern: if a write reports the
// engine is degraded, recover in place with Heal and retry once. With
// AutoHealAfterApplyFailure enabled this branch is rarely taken, but the
// idiom is what you want when running with the default fail-stop policy.
func putWithHeal(ctx context.Context, se *storage.StorageEngine, table, index string, key types.Comparable, doc string) error {
	err := se.Put(ctx, table, index, key, doc)
	if errors.Is(err, storage.ErrEngineDegraded) {
		fmt.Println("engine degraded; healing in place...")
		if healErr := se.Heal(ctx); healErr != nil {
			return fmt.Errorf("heal failed, engine still degraded: %w", healErr)
		}
		err = se.Put(ctx, table, index, key, doc)
	}
	return err
}
