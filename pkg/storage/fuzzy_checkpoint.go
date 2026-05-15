package storage

// FuzzyCheckpoint é um checkpoint not-bloqueante para writes.
//
// Diferença do CreateCheckpoint (hard checkpoint):
//   - CreateCheckpoint: comportamento idêntico, mas NOT grava record WAL.
//     Recovery precisa reprocessar o WAL inteiro.
//   - FuzzyCheckpoint: grava um record EntryCheckpoint no WAL com o
//     beginLSN. Recovery usa esse LSN para pular entradas antigas e
//     iniciar o redo só a partir daí, reduzindo O(WAL completo) para
//     O(WAL desde o último checkpoint).
//
// Semântica de not-bloqueio:
//   Not adquire lock global de tabelas. As pages são flushadas com
//   latches por-frame (como sempre), então writes em pages DIFERENTES
//   das que estão sendo flushadas prosseguem em paralelo. O único
//   "bloqueio" é por-page e é de curtíssima duração.
//
// Garantia para recovery:
//   Todas as pages sujas com LSN ≤ beginLSN são flushadas antes do
//   record de checkpoint ser escrito. Portanto, recovery pode assumir
//   que operações com LSN < beginLSN estão duravelmente em disco e pode
//   pular o redo delas.
//
// Uso recomendado: substitui CreateCheckpoint em produção. O engine
// mantém CreateCheckpoint para compatibilidade e uso em testes.

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

	// 4. Grava o record de checkpoint no WAL com o beginLSN.
	//    Recovery encontrará este record e iniciará o redo a partir de beginLSN.
	payload := serializeCheckpointPayloadV2(beginLSN, dpt, att)
	if err := se.WAL.WriteCheckpointRecordPayload(beginLSN, payload); err != nil {
		return fmt.Errorf("fuzzy checkpoint: escrever record WAL: %w", err)
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

// flushAllDirtyPages flusha todas as pages sujas de heaps e trees
// sem adquirir locks globais de tabela.
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
