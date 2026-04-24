package storage

// FuzzyCheckpoint é um checkpoint não-bloqueante para escritas.
//
// Diferença do CreateCheckpoint (hard checkpoint):
//   - CreateCheckpoint: comportamento idêntico, mas NÃO grava registro WAL.
//     Recovery precisa reprocessar o WAL inteiro.
//   - FuzzyCheckpoint: grava um registro EntryCheckpoint no WAL com o
//     beginLSN. Recovery usa esse LSN para pular entradas antigas e
//     iniciar o redo só a partir daí, reduzindo O(WAL completo) para
//     O(WAL desde o último checkpoint).
//
// Semântica de não-bloqueio:
//   Não adquire lock global de tabelas. As páginas são flushadas com
//   latches por-frame (como sempre), então escritas em páginas DIFERENTES
//   das que estão sendo flushadas prosseguem em paralelo. O único
//   "bloqueio" é por-página e é de curtíssima duração.
//
// Garantia para recovery:
//   Todas as páginas sujas com LSN ≤ beginLSN são flushadas antes do
//   registro de checkpoint ser escrito. Portanto, recovery pode assumir
//   que operações com LSN < beginLSN estão duravelmente em disco e pode
//   pular o redo delas.
//
// Uso recomendado: substitui CreateCheckpoint em produção. O engine
// mantém CreateCheckpoint para compatibilidade e uso em testes.

import (
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/btree"
	btreev2 "github.com/bobboyms/storage-engine/pkg/btree/v2"
	"github.com/bobboyms/storage-engine/pkg/heap"
	v2 "github.com/bobboyms/storage-engine/pkg/heap/v2"
)

// FuzzyCheckpoint executa um checkpoint não-bloqueante e grava um registro
// de checkpoint no WAL, permitindo que recovery pule entradas anteriores
// ao beginLSN.
func (se *StorageEngine) FuzzyCheckpoint() error {
	if se.WAL == nil {
		// Sem WAL não há recovery, checkpoint fuzzy é no-op.
		return nil
	}

	// 1. Captura o LSN atual ANTES de começar o flush.
	//    Qualquer operação com LSN ≤ beginLSN foi escrita no WAL e no
	//    heap/tree antes deste ponto — será coberta pelo flush abaixo.
	beginLSN := se.lsnTracker.Current()

	// 2. Flush do WAL: garante que entradas até beginLSN estão em disco.
	if err := se.WAL.Sync(); err != nil {
		return fmt.Errorf("fuzzy checkpoint: sync WAL: %w", err)
	}

	// 3. Flush das páginas sujas — não bloqueia escritas (per-frame latch).
	if err := se.flushAllDirtyPages(); err != nil {
		return fmt.Errorf("fuzzy checkpoint: flush páginas: %w", err)
	}

	// 4. Grava o registro de checkpoint no WAL com o beginLSN.
	//    Recovery encontrará este registro e iniciará o redo a partir de beginLSN.
	if err := se.WAL.WriteCheckpointRecord(beginLSN); err != nil {
		return fmt.Errorf("fuzzy checkpoint: escrever registro WAL: %w", err)
	}

	return nil
}

// flushAllDirtyPages flusha todas as páginas sujas de heaps e árvores
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
