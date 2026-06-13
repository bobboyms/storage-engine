package storage

import (
	"context"
	"fmt"

	btreev2 "github.com/bobboyms/storage-engine/pkg/btree/v2"
	heapv2 "github.com/bobboyms/storage-engine/pkg/heap/v2"
)

// rebuildEvictedIndexes rebuilds, from the heap, every B+ tree belonging to
// a table whose files took buffer pool flushes (evictions) after the last
// checkpoint. Such flushes persist an arbitrary subset of pages: the on-disk
// tree is a mixed-vintage cut, possibly with zero-filled holes where
// never-flushed pages should be, and replaying logical redo through it can
// corrupt it further (lost keys, cyclic leaf chains) or fail outright.
//
// It must run after physical redo (so the heap holds every flushed page at
// its newest logged image) and before logical redo (which probes the primary
// index for idempotency and needs it consistent with the heap). Returns the
// set of rebuilt tree paths — their pending partial-NTA before-images target
// the replaced files and must not be restored — and how many indexes were
// rebuilt.
func (se *StorageEngine) rebuildEvictedIndexes(ctx context.Context, analysis *recoveryAnalysis) (map[string]struct{}, int, error) {
	if analysis == nil || len(analysis.FlushedSinceCheckpoint) == 0 {
		return nil, 0, nil
	}

	rebuiltPaths := make(map[string]struct{})
	rebuilt := 0
	for _, tableName := range se.TableMetaData.ListTables() {
		if err := ctx.Err(); err != nil {
			return rebuiltPaths, rebuilt, err
		}
		table, err := se.TableMetaData.GetTableByName(tableName)
		if err != nil {
			continue
		}
		heapV2, ok := table.Heap.(*heapv2.HeapV2)
		if !ok {
			continue
		}

		suspect := recordedFlushMatches(analysis.FlushedSinceCheckpoint, heapV2.Path())
		var primaryName string
		var secondaryNames []string
		for _, idx := range table.GetIndices() {
			if idx.Primary {
				primaryName = idx.Name
			} else {
				secondaryNames = append(secondaryNames, idx.Name)
			}
			treeV2, isV2 := idx.Tree.(*btreev2.BTreeV2)
			if !isV2 {
				continue
			}
			if recordedFlushMatches(analysis.FlushedSinceCheckpoint, treeV2.Path()) {
				suspect = true
			}
		}
		if !suspect || primaryName == "" {
			continue
		}

		// Heap holes (pages allocated by an eviction flush but never written)
		// would break the rebuild scan; format them as empty slotted pages —
		// their records exist only in the WAL and logical redo re-adds them.
		if _, err := heapV2.HealZeroPages(ctx); err != nil {
			return rebuiltPaths, rebuilt, fmt.Errorf("storage: recovery heal heap %s: %w", tableName, err)
		}

		// Primary first: resolving duplicate keys re-stamps delete marks in
		// the heap, so the secondaries already see an unambiguous heap.
		for _, indexName := range append([]string{primaryName}, secondaryNames...) {
			index, err := table.GetIndex(indexName)
			if err != nil {
				return rebuiltPaths, rebuilt, err
			}
			if _, isV2 := index.Tree.(*btreev2.BTreeV2); !isV2 {
				continue
			}
			if err := rebuildIndex(ctx, se.TableMetaData, se.codec, tableName, indexName, true); err != nil {
				return rebuiltPaths, rebuilt, fmt.Errorf("storage: recovery rebuild %s.%s: %w", tableName, indexName, err)
			}
			rebuilt++
			if treeV2, isV2 := index.Tree.(*btreev2.BTreeV2); isV2 {
				rebuiltPaths[treeV2.Path()] = struct{}{}
			}
		}
	}
	return rebuiltPaths, rebuilt, nil
}
