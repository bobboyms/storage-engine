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
	"fmt"
	"math"

	"github.com/bobboyms/storage-engine/pkg/btree"
	btreev2 "github.com/bobboyms/storage-engine/pkg/btree/v2"
	"github.com/bobboyms/storage-engine/pkg/heap"
	v2 "github.com/bobboyms/storage-engine/pkg/heap/v2"
)

// FuzzyCheckpoint executa um checkpoint not-bloqueante e grava um record
// de checkpoint no WAL, permitindo que recovery pule entradas anteriores
// ao beginLSN.
func (se *StorageEngine) FuzzyCheckpoint() error {
	se.opMu.RLock()
	defer se.opMu.RUnlock()
	if err := se.runtimeReadyError(); err != nil {
		return err
	}

	return se.fuzzyCheckpointLocked()
}

func (se *StorageEngine) fuzzyCheckpointLocked() error {
	if se.WAL == nil {
		// Sem WAL there is no recovery, checkpoint fuzzy é no-op.
		return nil
	}

	// 1. Determina o menor pageLSN ainda sujo. Esse é o ponto seguro de
	//    redo para o checkpoint, porque qualquer page anterior já está
	//    durável e qualquer page suja a partir daqui será flushada já.
	beginLSN := se.oldestDirtyPageLSN()
	if beginLSN == 0 {
		beginLSN = se.lsnTracker.Current()
	}

	// 2. Flush do WAL: garante que entradas até beginLSN estão em disco.
	if err := se.WAL.Sync(); err != nil {
		return fmt.Errorf("fuzzy checkpoint: sync WAL: %w", err)
	}

	// 3. Flush das pages sujas — not bloqueia writes (per-frame latch).
	if err := se.flushAllDirtyPages(); err != nil {
		return fmt.Errorf("fuzzy checkpoint: flush pages: %w", err)
	}

	// 4. Grava o record de checkpoint no WAL com o beginLSN.
	//    Recovery encontrará este record e iniciará o redo a partir de beginLSN.
	if err := se.WAL.WriteCheckpointRecord(beginLSN); err != nil {
		return fmt.Errorf("fuzzy checkpoint: escrever record WAL: %w", err)
	}

	if err := se.WAL.CheckpointLifecycle(beginLSN); err != nil {
		return fmt.Errorf("fuzzy checkpoint: lifecycle WAL: %w", err)
	}

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
