// Demonstra como integrar o storage engine com:
//   - um *slog.Logger customizado;
//   - um EventListener que reage a recovery, vacuum, checkpoint, deadlock,
//     lock timeout e erro de background;
//   - a struct Stats() para snapshots periódicos de contadores.
//
// Saída: log estruturado no stdout + um "dashboard" final lido via Stats().
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func main() {
	tmpDir, err := os.MkdirTemp("", "observability_demo")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Printf("📂 Database: %s\n\n", tmpDir)

	// 1. Logger pluggable (JSON estruturado direto pro stdout).
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// 2. Contadores próprios alimentados pelo EventListener. Em produção
	//    isso geralmente vira métricas Prometheus / OpenTelemetry.
	var (
		recoveries    atomic.Uint64
		vacuums       atomic.Uint64
		reclaimed     atomic.Uint64
		checkpoints   atomic.Uint64
		deadlocks     atomic.Uint64
		lockTimeouts  atomic.Uint64
		bgErrors      atomic.Uint64
		lastVacuumLSN atomic.Uint64
	)

	listener := storage.EventListener{
		OnRecoveryComplete: func(ev storage.RecoveryEvent) {
			recoveries.Add(1)
			fmt.Printf("🟢 recovery: physical=%d/%d logical=%d/%d max_lsn=%d\n",
				ev.PhysicalApplied, ev.PhysicalApplied+ev.PhysicalSkipped,
				ev.LogicalApplied, ev.LogicalApplied+ev.LogicalSkipped,
				ev.MaxLSN)
		},
		OnVacuumComplete: func(ev storage.VacuumEvent) {
			vacuums.Add(1)
			reclaimed.Add(uint64(ev.Reclaimed))
			lastVacuumLSN.Store(ev.MinLSN)
			fmt.Printf("🧹 vacuum: table=%s reclaimed=%d min_lsn=%d\n",
				ev.Table, ev.Reclaimed, ev.MinLSN)
		},
		OnCheckpoint: func(ev storage.CheckpointEvent) {
			checkpoints.Add(1)
			fmt.Printf("💾 checkpoint: begin_lsn=%d\n", ev.BeginLSN)
		},
		OnDeadlock: func(ev storage.DeadlockEvent) {
			deadlocks.Add(1)
			fmt.Printf("⚠️  deadlock: victim=%d cycle=%v\n", ev.VictimTxID, ev.Cycle)
		},
		OnLockWaitTimeout: func(ev storage.LockWaitTimeoutEvent) {
			lockTimeouts.Add(1)
			fmt.Printf("⏰ lock timeout: tx=%d resource=%q\n", ev.TxID, ev.Resource)
		},
		OnBackgroundError: func(err error) {
			bgErrors.Add(1)
			fmt.Printf("🔥 background error: %v\n", err)
		},
	}

	// 3. Setup do engine usando Options.
	heapPath := filepath.Join(tmpDir, "products.heap")
	walPath := filepath.Join(tmpDir, "products.wal")

	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	if err != nil {
		log.Fatalf("NewHeapForTable: %v", err)
	}
	meta := storage.NewTableMenager()
	if err := meta.NewTable("products", []storage.Index{
		{Name: "id", Type: storage.TypeInt, Primary: true},
	}, 4, hm); err != nil {
		log.Fatalf("NewTable: %v", err)
	}
	ww, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		log.Fatalf("NewWALWriter: %v", err)
	}

	se, err := storage.NewProductionStorageEngineWithOptions(meta, ww, storage.Options{
		Logger:   logger,
		Listener: listener,
	})
	if err != nil {
		log.Fatalf("NewProductionStorageEngineWithOptions: %v", err)
	}
	defer se.Close()

	// 4. Workload: alguns inserts, delete + vacuum, checkpoint fuzzy.
	fmt.Println("\n--- workload ---")
	for i := 1; i <= 5; i++ {
		doc := fmt.Sprintf(`{"id": %d, "name": "item-%d", "ts": %q}`,
			i, i, time.Now().UTC().Format(time.RFC3339))
		if err := se.InsertRow(context.Background(), "products", doc, map[string]types.Comparable{
			"id": types.IntKey(i),
		}); err != nil {
			log.Fatalf("InsertRow: %v", err)
		}
	}

	if _, err := se.Del(context.Background(), "products", "id", types.IntKey(3)); err != nil {
		log.Fatalf("Del: %v", err)
	}

	if err := se.Vacuum(context.Background(), "products"); err != nil {
		log.Fatalf("Vacuum: %v", err)
	}

	if err := se.FuzzyCheckpoint(context.Background()); err != nil {
		log.Fatalf("FuzzyCheckpoint: %v", err)
	}

	// 5. Snapshot final via Stats().
	st := se.Stats()
	fmt.Println("\n--- engine.Stats() ---")
	fmt.Printf("CurrentLSN          = %d\n", st.CurrentLSN)
	fmt.Printf("ActiveTransactions  = %d\n", st.ActiveTransactions)
	fmt.Printf("Recoveries          = %d\n", st.Recoveries)
	fmt.Printf("VacuumRuns          = %d\n", st.VacuumRuns)
	fmt.Printf("VacuumReclaimed     = %d\n", st.VacuumReclaimed)
	fmt.Printf("Checkpoints         = %d\n", st.Checkpoints)
	fmt.Printf("DeadlocksDetected   = %d\n", st.DeadlocksDetected)
	fmt.Printf("LockWaitTimeouts    = %d\n", st.LockWaitTimeouts)
	fmt.Printf("DegradedSince       = %v\n", st.DegradedSince)

	fmt.Println("\n--- contadores do listener ---")
	fmt.Printf("recoveries=%d vacuums=%d reclaimed=%d checkpoints=%d deadlocks=%d lockTimeouts=%d bgErrors=%d lastVacuumLSN=%d\n",
		recoveries.Load(), vacuums.Load(), reclaimed.Load(),
		checkpoints.Load(), deadlocks.Load(), lockTimeouts.Load(),
		bgErrors.Load(), lastVacuumLSN.Load())
}
