package storage

// FuzzyCheckpoint is a non-blocking checkpoint for writes.
//
// Difference from CreateCheckpoint (hard checkpoint):
//   - CreateCheckpoint: identical behavior, but does NOT write a WAL record.
//     Recovery must reprocess the entire WAL.
//   - FuzzyCheckpoint: writes an EntryCheckpoint record in the WAL with the
//     beginLSN. Recovery uses that LSN to skip old entries and start the
//     redo only from there, reducing O(full WAL) to
//     O(WAL since the last checkpoint).
//
// Non-blocking semantics:
//   It does not acquire a global table lock. Pages are flushed with
//   per-frame latches (as always), so writes to pages DIFFERENT from the
//   ones being flushed proceed in parallel. The only "blocking" is
//   per-page and very short-lived.
//
// Guarantee for recovery:
//   All dirty pages with LSN ≤ beginLSN are flushed before the checkpoint
//   record is written. Therefore, recovery can assume that operations with
//   LSN < beginLSN are durably on disk and can skip their redo.
//
// Recommended use: replaces CreateCheckpoint in production. The engine
// keeps CreateCheckpoint for compatibility and use in tests.

import (
	"context"
	"fmt"
	"math"

	"github.com/bobboyms/storage-engine/pkg/btree"
	btreev2 "github.com/bobboyms/storage-engine/pkg/btree/v2"
	"github.com/bobboyms/storage-engine/pkg/heap"
	v2 "github.com/bobboyms/storage-engine/pkg/heap/v2"
	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// FuzzyCheckpoint runs a non-blocking checkpoint and writes a
// checkpoint record to the WAL, letting recovery skip entries older
// than beginLSN.
//
// Cancellation: ctx is honored up to and including WAL.Sync. Once Sync
// returns, the dirty-page flush + checkpoint WAL record are part of the
// durability path and proceed regardless of ctx.
func (se *StorageEngine) FuzzyCheckpoint(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	se.opMu.RLock()
	defer se.opMu.RUnlock()
	if err := se.runtimeReadyError(); err != nil {
		return err
	}

	return se.fuzzyCheckpointLocked(ctx)
}

func (se *StorageEngine) fuzzyCheckpointLocked(ctx context.Context) error {
	if se.WAL == nil {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	beginLSN := se.oldestDirtyPageLSN()
	if beginLSN == 0 {
		beginLSN = se.lsnTracker.Current()
	}

	if err := se.WAL.Sync(); err != nil {
		return fmt.Errorf("fuzzy checkpoint: sync WAL: %w", err)
	}

	// ARIES: snapshot the Dirty Page Table BEFORE the flush so the
	// recorded recLSNs reflect what was unflushed at the moment we
	// declared the checkpoint. We still flush below; future analyses
	// honor the snapshot to bound the redo start. ATT also captures
	// the active transactions' lastLSN at the same instant.
	dpt := se.snapshotDirtyPageTable()
	att := se.snapshotActiveTxTable()

	if err := se.flushAllDirtyPages(); err != nil {
		return fmt.Errorf("fuzzy checkpoint: flush pages: %w", err)
	}

	// 4. Write the checkpoint record in the WAL with the beginLSN.
	//    Recovery will find this record and start the redo from beginLSN.
	payload := serializeCheckpointPayloadV2(beginLSN, dpt, att)
	if err := se.WAL.WriteCheckpointRecordPayload(beginLSN, payload); err != nil {
		return fmt.Errorf("fuzzy checkpoint: write WAL record: %w", err)
	}

	if err := se.WAL.CheckpointLifecycle(beginLSN); err != nil {
		return fmt.Errorf("fuzzy checkpoint: lifecycle WAL: %w", err)
	}

	se.fireCheckpoint(CheckpointEvent{BeginLSN: beginLSN})
	return nil
}

// snapshotDirtyPageTable collects (path, pageID, recLSN) for every
// page currently dirty in any storage engine buffer pool, suitable for
// persisting in the next checkpoint record.
func (se *StorageEngine) snapshotDirtyPageTable() []dirtyPageEntry {
	out := []dirtyPageEntry{}
	collect := func(path string, infos []pagestore.DirtyPageInfo) {
		for _, info := range infos {
			if info.RecLSN == 0 {
				continue
			}
			out = append(out, dirtyPageEntry{
				Path:   path,
				PageID: uint64(info.PageID),
				RecLSN: info.RecLSN,
			})
		}
	}

	for _, tableName := range se.TableMetaData.ListTables() {
		table, err := se.TableMetaData.GetTableByName(tableName)
		if err != nil {
			continue
		}
		if heapV2, ok := table.Heap.(*v2.HeapV2); ok {
			collect(heapV2.Path(), heapV2.DirtyPages())
		}
		for _, idx := range table.GetIndices() {
			if treeV2, ok := idx.Tree.(*btreev2.BTreeV2); ok {
				collect(treeV2.Path(), treeV2.DirtyPages())
			}
		}
	}
	return out
}

// snapshotActiveTxTable returns one (txID, lastLSN) entry per active
// write transaction known to the engine. The current engine tracks the
// read view in TxRegistry but does not persist per-tx WAL LSNs there;
// returning an empty slice is correct for now (recovery falls back to
// scanning the WAL from CheckpointLSN). Future evolution can plug a
// real source of truth here without changing the on-disk format.
func (se *StorageEngine) snapshotActiveTxTable() []activeTxEntry {
	return nil
}

func (se *StorageEngine) oldestDirtyPageLSN() uint64 {
	oldest := uint64(math.MaxUint64)
	found := false

	for _, tableName := range se.TableMetaData.ListTables() {
		table, err := se.TableMetaData.GetTableByName(tableName)
		if err != nil {
			continue
		}

		if hookable, ok := table.Heap.(redoHookable); ok {
			for _, info := range hookable.DirtyPages() {
				if info.PageLSN == 0 {
					continue
				}
				found = true
				if info.PageLSN < oldest {
					oldest = info.PageLSN
				}
			}
		}

		for _, idx := range table.GetIndices() {
			hookable, ok := idx.Tree.(redoHookable)
			if !ok {
				continue
			}
			for _, info := range hookable.DirtyPages() {
				if info.PageLSN == 0 {
					continue
				}
				found = true
				if info.PageLSN < oldest {
					oldest = info.PageLSN
				}
			}
		}
	}

	if !found {
		return 0
	}
	return oldest
}

// flushAllDirtyPages flushes all dirty pages of heaps and trees
// without acquiring global table locks.
func (se *StorageEngine) flushAllDirtyPages() error {
	syncedTrees := make(map[btree.Tree]bool)
	syncedHeaps := make(map[heap.Heap]bool)

	for _, tableName := range se.TableMetaData.ListTables() {
		table, err := se.TableMetaData.GetTableByName(tableName)
		if err != nil {
			continue
		}

		for _, idx := range table.GetIndices() {
			if idx.Tree == nil || syncedTrees[idx.Tree] {
				continue
			}
			if treeV2, ok := idx.Tree.(*btreev2.BTreeV2); ok {
				if err := treeV2.Sync(); err != nil {
					return err
				}
			}
			syncedTrees[idx.Tree] = true
		}

		if table.Heap == nil || syncedHeaps[table.Heap] {
			continue
		}
		if heapV2, ok := table.Heap.(*v2.HeapV2); ok {
			if err := heapV2.Sync(); err != nil {
				return err
			}
		}
		syncedHeaps[table.Heap] = true
	}
	return nil
}
