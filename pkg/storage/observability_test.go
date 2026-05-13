package storage

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func newObservabilityTestEngine(t *testing.T, opts Options) *StorageEngine {
	t.Helper()
	dir := t.TempDir()
	heapPath := filepath.Join(dir, "obs.heap")
	walPath := filepath.Join(dir, "obs.wal")

	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("NewHeapForTable: %v", err)
	}
	meta := NewTableMenager()
	if err := meta.NewTable("obs", []Index{
		{Name: "id", Primary: true, Type: TypeInt},
	}, 0, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	ww, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("NewWALWriter: %v", err)
	}
	se, err := NewProductionStorageEngineWithOptions(meta, ww, opts)
	if err != nil {
		t.Fatalf("NewProductionStorageEngineWithOptions: %v", err)
	}
	t.Cleanup(func() { _ = se.Close() })
	return se
}

func TestStatsDefaultValues(t *testing.T) {
	se := newObservabilityTestEngine(t, Options{})
	st := se.Stats()
	if st.DegradedSince {
		t.Fatalf("nova engine não deve estar degradada")
	}
	if st.ActiveTransactions != 0 {
		t.Fatalf("ActiveTransactions inicial=%d, esperado 0", st.ActiveTransactions)
	}
	if st.Recoveries != 1 {
		t.Fatalf("Recoveries=%d, esperado 1 (NewProductionStorageEngine roda Recover uma vez)", st.Recoveries)
	}
}

func TestStatsTracksActiveTransactionsAndVacuum(t *testing.T) {
	var (
		recoveryCount uint64
		vacuumEvents  []VacuumEvent
		mu            sync.Mutex
	)
	se := newObservabilityTestEngine(t, Options{
		Listener: EventListener{
			OnRecoveryComplete: func(RecoveryEvent) {
				atomic.AddUint64(&recoveryCount, 1)
			},
			OnVacuumComplete: func(ev VacuumEvent) {
				mu.Lock()
				vacuumEvents = append(vacuumEvents, ev)
				mu.Unlock()
			},
		},
	})

	if got := atomic.LoadUint64(&recoveryCount); got != 1 {
		t.Fatalf("OnRecoveryComplete não chamou (count=%d)", got)
	}

	if err := se.Put("obs", "id", types.IntKey(1), `{"id":1}`); err != nil {
		t.Fatalf("Put: %v", err)
	}

	tx := se.BeginRead()
	st := se.Stats()
	if st.ActiveTransactions != 1 {
		t.Fatalf("ActiveTransactions=%d, esperado 1", st.ActiveTransactions)
	}
	tx.Close()
	st = se.Stats()
	if st.ActiveTransactions != 0 {
		t.Fatalf("ActiveTransactions pós-close=%d, esperado 0", st.ActiveTransactions)
	}
	if st.CurrentLSN == 0 {
		t.Fatalf("CurrentLSN=0 após Put bem-sucedido")
	}

	if _, err := se.Del("obs", "id", types.IntKey(1)); err != nil {
		t.Fatalf("Del: %v", err)
	}
	if err := se.Vacuum("obs"); err != nil {
		t.Fatalf("Vacuum: %v", err)
	}

	st = se.Stats()
	if st.VacuumRuns != 1 {
		t.Fatalf("VacuumRuns=%d, esperado 1", st.VacuumRuns)
	}
	mu.Lock()
	got := len(vacuumEvents)
	mu.Unlock()
	if got != 1 {
		t.Fatalf("OnVacuumComplete chamado %d vezes, esperado 1", got)
	}
}

func TestLoggerReceivesEvents(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	se := newObservabilityTestEngine(t, Options{Logger: logger})

	if err := se.Put("obs", "id", types.IntKey(42), `{"id":42}`); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := se.Vacuum("obs"); err != nil {
		t.Fatalf("Vacuum: %v", err)
	}

	out := buf.String()
	if !contains(out, "recovery complete") {
		t.Fatalf("logger não recebeu evento de recovery; saída=%q", out)
	}
	if !contains(out, "vacuum complete") {
		t.Fatalf("logger não recebeu evento de vacuum; saída=%q", out)
	}
}

func TestStatsDegradedAfterBackgroundError(t *testing.T) {
	var bgErr atomic.Pointer[error]
	se := newObservabilityTestEngine(t, Options{
		Listener: EventListener{
			OnBackgroundError: func(err error) {
				bgErr.Store(&err)
			},
		},
	})

	se.markDegraded(os.ErrInvalid)

	st := se.Stats()
	if !st.DegradedSince {
		t.Fatalf("Stats.DegradedSince=false após markDegraded")
	}
	if got := bgErr.Load(); got == nil || *got == nil {
		t.Fatalf("OnBackgroundError não foi disparado")
	}
}

func TestNilOptionsKeepsLoggerSafe(t *testing.T) {
	// Logger deve ser não-nil mesmo quando Options.Logger=nil.
	se := newObservabilityTestEngine(t, Options{})
	if se.Logger() == nil {
		t.Fatalf("Logger() devolveu nil")
	}
	// Não pode panicar ao usar.
	se.Logger().Info("smoke")
}

func contains(haystack, needle string) bool {
	return bytes.Contains([]byte(haystack), []byte(needle))
}
