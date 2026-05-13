package storage

import (
	"io"
	"log/slog"
	"sync/atomic"

	"github.com/bobboyms/storage-engine/pkg/codec"
)

// EventListener receives notifications about significant engine activity.
//
// All callbacks are optional; nil entries are skipped. Callbacks run on
// engine code paths (some under internal locks) and must not block or
// re-enter the engine. Heavy work belongs in a goroutine owned by the
// caller.
type EventListener struct {
	OnRecoveryComplete func(RecoveryEvent)
	OnVacuumComplete   func(VacuumEvent)
	OnCheckpoint       func(CheckpointEvent)
	OnDeadlock         func(DeadlockEvent)
	OnLockWaitTimeout  func(LockWaitTimeoutEvent)
	OnBackgroundError  func(error)
}

type RecoveryEvent struct {
	PhysicalApplied int
	PhysicalSkipped int
	LogicalApplied  int
	LogicalSkipped  int
	CheckpointLSN   uint64
	MaxLSN          uint64
}

type VacuumEvent struct {
	Table     string
	MinLSN    uint64
	Reclaimed int
}

type CheckpointEvent struct {
	BeginLSN uint64
}

type DeadlockEvent struct {
	VictimTxID uint64
	Cycle      []uint64
}

type LockWaitTimeoutEvent struct {
	TxID     uint64
	Resource string
}

// Options configures pluggable engine behavior. Zero value is valid:
// logging is discarded, no listener callbacks fire, and documents are
// encoded with the default BSON codec.
type Options struct {
	// Logger receives structured engine events. Defaults to a logger
	// that discards all output.
	Logger *slog.Logger

	// Listener receives lifecycle callbacks. Zero value disables them.
	Listener EventListener

	// Codec encodes/decodes documents stored by the engine. When nil,
	// bsoncodec.New() is used so the engine keeps its historical
	// JSON-in / BSON-on-disk behavior.
	Codec codec.Codec
}

// Stats is a point-in-time snapshot of cumulative engine counters.
// Returned by StorageEngine.Stats().
type Stats struct {
	CurrentLSN         uint64
	ActiveTransactions int
	Recoveries         uint64
	VacuumRuns         uint64
	VacuumReclaimed    uint64
	Checkpoints        uint64
	DeadlocksDetected  uint64
	LockWaitTimeouts   uint64
	DegradedSince      bool
}

type engineCounters struct {
	recoveries        atomic.Uint64
	vacuumRuns        atomic.Uint64
	vacuumReclaimed   atomic.Uint64
	checkpoints       atomic.Uint64
	deadlocksDetected atomic.Uint64
	lockWaitTimeouts  atomic.Uint64
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// Stats returns a snapshot of cumulative engine counters and current state.
func (se *StorageEngine) Stats() Stats {
	if se == nil {
		return Stats{}
	}
	se.runtimeMu.RLock()
	degraded := se.degradedErr != nil
	se.runtimeMu.RUnlock()
	return Stats{
		CurrentLSN:         se.lsnTracker.Current(),
		ActiveTransactions: se.TxRegistry.Active(),
		Recoveries:         se.counters.recoveries.Load(),
		VacuumRuns:         se.counters.vacuumRuns.Load(),
		VacuumReclaimed:    se.counters.vacuumReclaimed.Load(),
		Checkpoints:        se.counters.checkpoints.Load(),
		DeadlocksDetected:  se.counters.deadlocksDetected.Load(),
		LockWaitTimeouts:   se.counters.lockWaitTimeouts.Load(),
		DegradedSince:      degraded,
	}
}

// Logger returns the engine's logger. Always non-nil.
func (se *StorageEngine) Logger() *slog.Logger {
	if se == nil || se.logger == nil {
		return discardLogger()
	}
	return se.logger
}

func (se *StorageEngine) fireRecoveryComplete(ev RecoveryEvent) {
	se.counters.recoveries.Add(1)
	se.logger.Info("storage: recovery complete",
		"physical_applied", ev.PhysicalApplied,
		"physical_skipped", ev.PhysicalSkipped,
		"logical_applied", ev.LogicalApplied,
		"logical_skipped", ev.LogicalSkipped,
		"checkpoint_lsn", ev.CheckpointLSN,
		"max_lsn", ev.MaxLSN,
	)
	if se.listener.OnRecoveryComplete != nil {
		se.listener.OnRecoveryComplete(ev)
	}
}

func (se *StorageEngine) fireVacuumComplete(ev VacuumEvent) {
	se.counters.vacuumRuns.Add(1)
	if ev.Reclaimed > 0 {
		se.counters.vacuumReclaimed.Add(uint64(ev.Reclaimed))
	}
	se.logger.Info("storage: vacuum complete",
		"table", ev.Table,
		"min_lsn", ev.MinLSN,
		"reclaimed", ev.Reclaimed,
	)
	if se.listener.OnVacuumComplete != nil {
		se.listener.OnVacuumComplete(ev)
	}
}

func (se *StorageEngine) fireCheckpoint(ev CheckpointEvent) {
	se.counters.checkpoints.Add(1)
	se.logger.Info("storage: checkpoint complete", "begin_lsn", ev.BeginLSN)
	if se.listener.OnCheckpoint != nil {
		se.listener.OnCheckpoint(ev)
	}
}

func (se *StorageEngine) fireBackgroundError(err error) {
	se.logger.Error("storage: background error", "err", err)
	if se.listener.OnBackgroundError != nil {
		se.listener.OnBackgroundError(err)
	}
}
