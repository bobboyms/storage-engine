package storage

import (
	"context"
	"fmt"
	"os"

	"github.com/bobboyms/storage-engine/pkg/btree"
	btreev2 "github.com/bobboyms/storage-engine/pkg/btree/v2"
	"github.com/bobboyms/storage-engine/pkg/codec"
	"github.com/bobboyms/storage-engine/pkg/codec/bsoncodec"
	heapv2 "github.com/bobboyms/storage-engine/pkg/heap/v2"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// RebuildIndex reconstructs one index of a table from its heap — the heap is
// the source of truth, so this is the repair for any index-side corruption
// (dangling pointers, lost or mismatched entries). Every live record gets
// exactly one entry; tombstones and old versions get none, which is
// equivalent to a fully vacuumed index.
//
// The new index is built in a sibling temp file and atomically renamed over
// the old one only after it is complete, so a failed rebuild (I/O error, or
// two live records claiming the same key — an ambiguity only a human can
// resolve) leaves the existing index untouched.
//
// Offline tooling only: the table must be quiescent. c decodes heap
// documents; nil uses the default BSON codec.
func RebuildIndex(ctx context.Context, tm *TableMetaData, c codec.Codec, tableName, indexName string) error {
	return rebuildIndex(ctx, tm, c, tableName, indexName, false)
}

// rebuildIndex is RebuildIndex with an explicit duplicate policy. With
// resolveDuplicates false (repair tooling), two live records claiming the
// same key abort the rebuild: the ambiguity needs a human. With true
// (crash recovery), the newest CreateLSN wins and the older record gets the
// delete mark the crash threw away — an eviction can persist an UPDATE's new
// version without the page that recorded the old version's deletion, and the
// WAL is the proof the newer version superseded it.
func rebuildIndex(ctx context.Context, tm *TableMetaData, c codec.Codec, tableName, indexName string, resolveDuplicates bool) error {
	if c == nil {
		c = bsoncodec.New()
	}
	table, err := tm.GetTableByName(tableName)
	if err != nil {
		return err
	}
	index, err := table.GetIndex(indexName)
	if err != nil {
		return err
	}
	oldTree, ok := index.Tree.(*btreev2.BTreeV2)
	if !ok {
		return fmt.Errorf("storage: rebuild index %s.%s: tree type %T not supported", tableName, indexName, index.Tree)
	}
	heapV2, ok := table.Heap.(*heapv2.HeapV2)
	if !ok {
		return fmt.Errorf("storage: rebuild index %s.%s: heap type %T not supported", tableName, indexName, table.Heap)
	}
	primaryName := ""
	for _, idx := range table.GetIndices() {
		if idx.Primary {
			primaryName = idx.Name
		}
	}
	if primaryName == "" {
		return fmt.Errorf("storage: rebuild index %s.%s: table has no primary index", tableName, indexName)
	}

	path := oldTree.Path()
	tmpPath := path + ".rebuild.tmp"
	_ = os.Remove(tmpPath)
	cipher := tm.indexCipher()

	openTree := func(p string) (btree.Tree, error) {
		if index.Primary {
			return NewBTreeForIndex(BTreeFormatV2, true, index.Type, p, cipher)
		}
		return btreev2.NewBTreeV2Varchar(p, indexBufferPoolPages, cipher, btreev2.CompositeKeyCodec{})
	}

	fresh, err := openTree(tmpPath)
	if err != nil {
		return fmt.Errorf("storage: rebuild index %s.%s: create scratch tree: %w", tableName, indexName, err)
	}
	abort := func(cause error) error {
		_ = fresh.Close()
		_ = os.Remove(tmpPath)
		return cause
	}

	// Heap delete marks for duplicate losers are deferred until the scan
	// finishes so the rebuild never mutates pages it is still iterating.
	type lostDuplicate struct {
		rid       int64
		deleteLSN uint64
	}
	var lostDuplicates []lostDuplicate

	err = heapV2.ForEachRecord(ctx, func(rid int64, rh heapv2.RecordHeader, doc []byte) error {
		if !rh.Valid {
			return nil // tombstones and superseded versions carry no entry.
		}
		physicalKey, err := rebuildKeyFor(c, index, primaryName, doc)
		if err != nil {
			return fmt.Errorf("record %d: %w", rid, err)
		}
		existingRid, exists, err := fresh.Get(physicalKey)
		if err != nil {
			return fmt.Errorf("record %d: probe scratch tree: %w", rid, err)
		}
		if exists {
			if !resolveDuplicates {
				return fmt.Errorf("record %d: duplicate key %v — two live records claim it, heap is ambiguous", rid, physicalKey)
			}
			_, existingHdr, err := heapV2.Read(existingRid)
			if err != nil {
				return fmt.Errorf("record %d: read duplicate peer %d: %w", rid, existingRid, err)
			}
			if rh.CreateLSN <= existingHdr.CreateLSN {
				// The record already indexed is the survivor; this one lost.
				lostDuplicates = append(lostDuplicates, lostDuplicate{rid: rid, deleteLSN: existingHdr.CreateLSN})
				return nil
			}
			lostDuplicates = append(lostDuplicates, lostDuplicate{rid: existingRid, deleteLSN: rh.CreateLSN})
		}
		if treeV2, ok := fresh.(*btreev2.BTreeV2); ok {
			return treeV2.ReplaceWithLSN(physicalKey, rid, rh.CreateLSN)
		}
		return fresh.Replace(physicalKey, rid)
	})
	if err != nil {
		return abort(fmt.Errorf("storage: rebuild index %s.%s: %w", tableName, indexName, err))
	}
	for _, lost := range lostDuplicates {
		if err := heapV2.Delete(lost.rid, lost.deleteLSN); err != nil {
			return abort(fmt.Errorf("storage: rebuild index %s.%s: re-stamp lost delete on record %d: %w", tableName, indexName, lost.rid, err))
		}
	}
	if err := fresh.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("storage: rebuild index %s.%s: close scratch tree: %w", tableName, indexName, err)
	}

	// Swap: the scratch tree is complete and durable; replace the old file
	// and reopen at the canonical path.
	if err := oldTree.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("storage: rebuild index %s.%s: close old tree: %w", tableName, indexName, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("storage: rebuild index %s.%s: swap tree file: %w", tableName, indexName, err)
	}
	rebuilt, err := openTree(path)
	if err != nil {
		return fmt.Errorf("storage: rebuild index %s.%s: reopen rebuilt tree: %w", tableName, indexName, err)
	}
	index.Tree = rebuilt
	// Re-arm engine hooks (page-redo flush hook, structural logger) on the
	// freshly opened tree.
	tm.notifyTopologyChange()
	return nil
}

// rebuildKeyFor derives the physical tree key an index entry must carry for
// a live document: the logical field value for the primary index, or the
// (logical, primary) composite for a secondary.
func rebuildKeyFor(c codec.Codec, index *Index, primaryName string, doc []byte) (types.Comparable, error) {
	parsed, err := c.Open(doc)
	if err != nil {
		return nil, fmt.Errorf("undecodable document: %w", err)
	}
	logical, ok, err := parsed.Key(index.Name)
	if err != nil || !ok {
		return nil, fmt.Errorf("document missing indexed field %q (present=%v err=%v)", index.Name, ok, err)
	}
	if index.Primary {
		return logical, nil
	}
	primaryKey, ok, err := parsed.Key(primaryName)
	if err != nil || !ok {
		return nil, fmt.Errorf("document missing primary field %q (present=%v err=%v)", primaryName, ok, err)
	}
	return types.NewCompositeKey(logical, primaryKey), nil
}
