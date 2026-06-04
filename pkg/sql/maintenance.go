package sql

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// tmpSuffix is the extension used by the engine's atomic-write pattern
// (write temp -> fsync -> rename). A lingering temp file is therefore always a
// crash leftover, safe to remove.
const tmpSuffix = ".tmp"

// maintenanceMinAge is how old an orphan temp file must be before a scheduled
// or on-demand sweep removes it, so an in-flight atomic write is never deleted
// out from under a concurrent operation.
const maintenanceMinAge = time.Minute

// maintenanceRunner controls the background maintenance goroutine.
type maintenanceRunner struct {
	stop chan struct{}
	done chan struct{}
}

// RunMaintenance performs one maintenance pass: a fuzzy checkpoint (which
// flushes dirty pages and prunes write-ahead-log segments already covered by
// the checkpoint) followed by a sweep of orphan temp files left in the database
// directory by interrupted atomic writes. It returns the number of temp files
// removed.
func (e *Executor) RunMaintenance(ctx context.Context) (int, error) {
	if e.engine != nil {
		// Reclaim dead heap space from tables that had DELETE/UPDATE, then
		// checkpoint so the compaction is flushed in the same pass.
		if err := e.vacuumDirty(ctx); err != nil {
			return 0, err
		}
		// Activity gating: only checkpoint when the LSN advanced since the last
		// checkpoint, so an idle database does no periodic I/O.
		if e.engine.Stats().CurrentLSN != e.lastCheckpointLSN.Load() {
			if err := e.engine.FuzzyCheckpoint(ctx); err != nil {
				return 0, err
			}
			e.lastCheckpointLSN.Store(e.engine.Stats().CurrentLSN)
		}
	}
	if e.ddl == nil {
		return 0, nil
	}
	return sweepTempFiles(e.ddl.dir, maintenanceMinAge)
}

// markGarbage records that a table accumulated dead heap space (from a
// DELETE/UPDATE) and should be vacuumed by the next maintenance pass.
func (e *Executor) markGarbage(table string) {
	e.gcMu.Lock()
	defer e.gcMu.Unlock()
	if e.dirtyTables == nil {
		e.dirtyTables = make(map[string]struct{})
	}
	e.dirtyTables[table] = struct{}{}
}

// takeDirtyTables returns and clears the set of tables pending a vacuum.
func (e *Executor) takeDirtyTables() []string {
	e.gcMu.Lock()
	defer e.gcMu.Unlock()
	if len(e.dirtyTables) == 0 {
		return nil
	}
	tables := make([]string, 0, len(e.dirtyTables))
	for t := range e.dirtyTables {
		tables = append(tables, t)
	}
	e.dirtyTables = nil
	return tables
}

// vacuumDirty vacuums every table that accumulated dead space since the last
// pass. On failure the unprocessed tables are re-marked so a later pass retries
// them.
func (e *Executor) vacuumDirty(ctx context.Context) error {
	tables := e.takeDirtyTables()
	for i, table := range tables {
		if err := e.engine.Vacuum(ctx, table); err != nil {
			for _, rest := range tables[i:] {
				e.markGarbage(rest)
			}
			return err
		}
	}
	return nil
}

// StartMaintenance launches a background goroutine that runs RunMaintenance
// every interval. It is a no-op if maintenance is already running or interval
// is non-positive. Close stops it. Errors from periodic passes are ignored;
// call RunMaintenance directly if you need to observe them.
func (e *Executor) StartMaintenance(interval time.Duration) {
	if interval <= 0 {
		return
	}
	e.maintMu.Lock()
	defer e.maintMu.Unlock()
	if e.maint != nil {
		return
	}
	r := &maintenanceRunner{stop: make(chan struct{}), done: make(chan struct{})}
	e.maint = r
	go func() {
		defer close(r.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-r.stop:
				return
			case <-ticker.C:
				_, _ = e.RunMaintenance(context.Background())
			}
		}
	}()
}

// stopMaintenance stops the background maintenance goroutine if running.
func (e *Executor) stopMaintenance() {
	e.maintMu.Lock()
	r := e.maint
	e.maint = nil
	e.maintMu.Unlock()
	if r != nil {
		close(r.stop)
		<-r.done
	}
}

// sweepTempFiles removes orphan "*.tmp" files directly under dir. Only files
// whose modification time is older than minAge are removed (minAge == 0 removes
// all, used at open time when nothing is writing yet). It returns the number of
// files removed.
func sweepTempFiles(dir string, minAge time.Duration) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	cutoff := time.Now().Add(-minAge)
	removed := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), tmpSuffix) {
			continue
		}
		if minAge > 0 {
			info, err := entry.Info()
			if err != nil {
				continue
			}
			if info.ModTime().After(cutoff) {
				continue
			}
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err == nil {
			removed++
		}
	}
	return removed, nil
}
