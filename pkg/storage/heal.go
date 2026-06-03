package storage

import (
	"context"
	"fmt"
)

// Heal recovers an engine that went degraded after a failed post-commit
// apply, in-process and without closing/reopening the files. It replays
// the WAL idempotently against the open heap and indexes — the same
// recovery performed at open — so a transaction whose COMMIT is durable
// but whose in-memory application stopped midway converges to its full
// committed state and the engine becomes available again.
//
// Heal is a no-op (returns nil) on a healthy engine. If the replay fails
// — typically because the underlying fault that caused the degradation
// is still present (e.g. a full or failing disk) — the engine stays
// degraded and the error is returned, so the fail-stop guarantee holds.
//
// Concurrency: Heal takes the engine-wide exclusive barrier, so at the
// moment it runs every transaction recorded in the WAL is terminal
// (COMMIT or ABORT) and no undo work is pending.
func (se *StorageEngine) Heal(ctx context.Context) error {
	if se == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	se.opMu.Lock()
	defer se.opMu.Unlock()
	return se.healLocked(ctx)
}

// healLocked performs the in-place WAL replay. The caller must hold
// se.opMu (exclusively) so the replay sees a quiescent engine. It is
// shared by the public Heal entry point and the optional auto-heal path
// in Commit.
func (se *StorageEngine) healLocked(ctx context.Context) error {
	if se == nil || se.WAL == nil {
		return nil
	}
	se.runtimeMu.RLock()
	degraded := se.degradedErr != nil
	se.runtimeMu.RUnlock()
	if !degraded {
		return nil
	}

	// Flush the WAL so a fresh reader observes every durable entry,
	// including the COMMIT whose apply failed. Without this the current
	// in-memory WAL page may not yet be on disk for the replay reader.
	if err := se.WAL.Sync(); err != nil {
		return fmt.Errorf("storage: heal sync wal: %w", err)
	}

	// RecoverWithCipher replays redo idempotently and clears the degraded
	// flag on success; on failure it returns the error and leaves the
	// engine degraded.
	if err := se.RecoverWithCipher(ctx, se.WAL.Path(), se.walCipher()); err != nil {
		return fmt.Errorf("storage: heal replay: %w", err)
	}
	return nil
}
