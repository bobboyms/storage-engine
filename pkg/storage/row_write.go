package storage

import (
	"context"
	"fmt"

	btreev2 "github.com/bobboyms/storage-engine/pkg/btree/v2"
	"github.com/bobboyms/storage-engine/pkg/codec"
	"github.com/bobboyms/storage-engine/pkg/errors"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

type indexUpdateUndo struct {
	index   *Index
	key     types.Comparable
	old     int64
	exists  bool
	changed bool
}

func (se *StorageEngine) writeRow(ctx context.Context, tableName string, doc string, providedKeys map[string]types.Comparable, insertOnly bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	se.opMu.RLock()
	defer se.opMu.RUnlock()
	if err := se.runtimeReadyError(); err != nil {
		return err
	}

	return se.writeRowLocked(ctx, tableName, doc, providedKeys, insertOnly)
}

func (se *StorageEngine) writeRowLocked(ctx context.Context, tableName string, doc string, providedKeys map[string]types.Comparable, insertOnly bool) error {
	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return err
	}

	bsonData, keys, err := prepareRowDocument(se.codec, table, doc, providedKeys)
	if err != nil {
		return err
	}

	resources, err := lockResourcesForKeys(tableName, keys)
	if err != nil {
		return err
	}

	return se.withAutoCommitLocks(ctx, resources, func() error {
		table.Lock()
		defer table.Unlock()

		primary, primaryKey, err := primaryIndexAndKey(table, keys)
		if err != nil {
			return err
		}

		oldPrimaryOffset, primaryExists, err := primary.Tree.Get(primaryKey)
		if err != nil {
			return fmt.Errorf("primary index get failed: %w", err)
		}
		if insertOnly && primaryExists {
			// A DELETE keeps the primary key's B-tree entry under MVCC (it only
			// tombstones the heap version), so tree presence alone does not mean
			// the key is occupied. Mirror the visibility-aware UNIQUE check: a
			// tombstoned head version is not a live duplicate, so the key is free
			// for reuse. Snapshot the latest committed state — the table write
			// lock is held, so no concurrent writer can change visibility here.
			snap := &Transaction{SnapshotLSN: se.lsnTracker.Current(), Level: RepeatableRead, engine: se}
			rec, err := se.readVisibleRecordRaw(ctx, snap, table, primaryKey, oldPrimaryOffset)
			if err != nil {
				return err
			}
			if rec.Found {
				return fmt.Errorf("duplicate key error: key %v already exists in index %s", primaryKey, primary.Name)
			}
		}

		// Enforce UNIQUE constraints under the table write lock, so the check and
		// the write are atomic with respect to other writers (the race a
		// service-layer read-before-write cannot close).
		if err := se.checkUniqueConstraints(ctx, table, keys, primaryKey); err != nil {
			return err
		}

		currentLSN := se.lsnTracker.Next()
		if se.WAL != nil {
			if err := se.writeMultiIndexWAL(tableName, keys, bsonData, currentLSN); err != nil {
				return err
			}
		}

		prevOffset := int64(-1)
		if primaryExists {
			prevOffset = oldPrimaryOffset
		}
		offset, err := table.Heap.Write(bsonData, currentLSN, prevOffset)
		if err != nil {
			return fmt.Errorf("heap write failed: %w", err)
		}

		if err := applyIndexPointersWithLSN(table, keys, offset, currentLSN); err != nil {
			return err
		}

		if primaryExists {
			if err := table.Heap.Delete(oldPrimaryOffset, currentLSN); err != nil && !isChainEndErr(err) {
				_ = applyIndexPointers(table, map[string]types.Comparable{primary.Name: primaryKey}, oldPrimaryOffset)
				return fmt.Errorf("heap delete previous version failed: %w", err)
			}
		}

		for indexName := range keys {
			se.appliedLSN.MarkApplied(tableName, indexName, currentLSN)
		}
		return nil
	})
}

// checkUniqueConstraints rejects the write if any UNIQUE secondary index
// already has the row's logical key on a different, currently-visible row. It
// must be called while the table write lock is held so the check and the
// ensuing write are atomic with respect to other writers.
func (se *StorageEngine) checkUniqueConstraints(ctx context.Context, table *Table, keys map[string]types.Comparable, primaryKey types.Comparable) error {
	for name, key := range keys {
		idx, ok := table.Indices[name]
		if !ok || !idx.Unique || idx.Primary || key == nil {
			continue
		}
		conflict, err := se.uniqueConflictExists(ctx, table, idx, key, primaryKey)
		if err != nil {
			return err
		}
		if conflict {
			return &errors.DuplicateKeyError{Key: fmt.Sprintf("%v", key)}
		}
	}
	return nil
}

// uniqueConflictExists reports whether a visible row other than the one keyed by
// excludePrimary already carries logicalKey on the given unique index. It scans
// the index's logical-key range and resolves each candidate's MVCC visibility
// against a snapshot of the latest committed state.
func (se *StorageEngine) uniqueConflictExists(ctx context.Context, table *Table, idx *Index, logicalKey, excludePrimary types.Comparable) (bool, error) {
	treeV2, ok := idx.Tree.(*btreev2.BTreeV2)
	if !ok {
		return false, fmt.Errorf("storage: unique check: index %s uses unsupported tree type %T", idx.Name, idx.Tree)
	}
	// A snapshot of the current committed state. The table write lock is held,
	// so no concurrent writer can change visibility during the scan; reading the
	// LSN directly avoids re-entering the engine's op lock.
	snap := &Transaction{SnapshotLSN: se.lsnTracker.Current(), Level: RepeatableRead, engine: se}

	cur, err := treeV2.NewCursor(secondaryLowerBound(logicalKey), secondaryUpperBound(logicalKey))
	if err != nil {
		return false, err
	}
	defer func() { _ = cur.Close() }()

	for cur.Next() {
		if composite, ok := cur.Key().(types.CompositeKey); ok && composite.Primary != nil && excludePrimary != nil {
			if cmp, err := composite.Primary.Compare(excludePrimary); err == nil && cmp == 0 {
				continue // an entry for the same row being written; not a conflict
			}
		}
		rec, err := se.readVisibleRecordRaw(ctx, snap, table, logicalKey, cur.Value())
		if err != nil {
			return false, err
		}
		if rec.Found {
			return true, nil
		}
	}
	return false, cur.Err()
}

func (se *StorageEngine) writeMultiIndexWAL(tableName string, keys map[string]types.Comparable, bsonData []byte, lsn uint64) error {
	payload, err := SerializeMultiIndexEntry(tableName, keys, bsonData)
	if err != nil {
		return err
	}

	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = 1
	entry.Header.EntryType = wal.EntryMultiInsert
	entry.Header.LSN = lsn
	entry.Header.PayloadLen = uint32(len(payload)) //nolint:gosec // payload size bounded by record limits
	entry.Header.CRC32 = wal.CalculateCRC32(payload)
	entry.Payload = append(entry.Payload, payload...)

	err = se.WAL.WriteEntry(entry)
	wal.ReleaseEntry(entry)
	if err != nil {
		return fmt.Errorf("wal write failed: %w", err)
	}
	return nil
}

func prepareRowDocument(c codec.Codec, table *Table, doc string, providedKeys map[string]types.Comparable) ([]byte, map[string]types.Comparable, error) {
	if providedKeys == nil {
		providedKeys = map[string]types.Comparable{}
	}

	parsedDoc, err := c.Parse(doc)
	if err == nil {
		keys, ok, err := keysFromCodecDocForAllIndexes(table, parsedDoc)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			return nil, nil, fmt.Errorf("storage: JSON document does not contain all indexed fields")
		}
		for name, provided := range providedKeys {
			derived, ok := keys[name]
			if !ok {
				return nil, nil, &errors.IndexNotFoundError{Name: name}
			}
			if !sameComparableKey(derived, provided) {
				return nil, nil, fmt.Errorf("storage: key informada %s=%v diverge do documento (%v)", name, provided, derived)
			}
		}
		bsonData, err := parsedDoc.Bytes()
		if err != nil {
			return nil, nil, err
		}
		return bsonData, keys, nil
	}

	keys := make(map[string]types.Comparable, len(providedKeys))
	for name, key := range providedKeys {
		idx, ok := table.Indices[name]
		if !ok {
			return nil, nil, &errors.IndexNotFoundError{Name: name}
		}
		if err := validateKeyForIndex(idx, key); err != nil {
			return nil, nil, err
		}
		keys[name] = key
	}
	for _, idx := range table.GetIndices() {
		if _, ok := keys[idx.Name]; !ok {
			return nil, nil, fmt.Errorf("storage: key obrigatoria para indice %s ausente", idx.Name)
		}
	}
	return []byte(doc), keys, nil
}

func keysFromCodecDocForAllIndexes(table *Table, doc codec.Document) (map[string]types.Comparable, bool, error) {
	return keysFromCodecDocForIndexes(table.GetIndices(), doc)
}

func keysFromCodecDocForIndexes(indexes []*Index, doc codec.Document) (map[string]types.Comparable, bool, error) {
	keys := make(map[string]types.Comparable)
	for _, idx := range indexes {
		key, ok, err := doc.Key(idx.Name)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		if err := validateKeyForIndex(idx, key); err != nil {
			return nil, false, err
		}
		keys[idx.Name] = key
	}
	return keys, true, nil
}

func validateKeyForIndex(index *Index, key types.Comparable) error {
	if getTypeFromKey(key) != index.Type {
		return &errors.InvalidKeyTypeError{
			Name:     index.Name,
			TypeName: getTypeFromKey(key).String(),
		}
	}
	return nil
}

func sameComparableKey(a, b types.Comparable) bool {
	if a == nil || b == nil {
		return a == b
	}
	if getTypeFromKey(a) != getTypeFromKey(b) {
		return false
	}
	cmp, err := a.Compare(b)
	return err == nil && cmp == 0
}

func physicalIndexKey(index *Index, logicalKey, primaryKey types.Comparable) types.Comparable {
	if index.Primary {
		return logicalKey
	}
	return types.NewCompositeKey(logicalKey, primaryKey)
}

func singleIndexPhysicalKey(index *Index, logicalKey types.Comparable) types.Comparable {
	if index.Primary {
		return logicalKey
	}
	return types.NewCompositeKey(logicalKey, logicalKey)
}

func logicalIndexKey(index *Index, physicalKey types.Comparable) types.Comparable {
	if index.Primary {
		return physicalKey
	}
	if composite, ok := physicalKey.(types.CompositeKey); ok {
		return composite.Secondary
	}
	return physicalKey
}

func secondaryLowerBound(key types.Comparable) types.Comparable {
	if key == nil {
		return nil
	}
	return types.CompositeLowerBound(key)
}

func secondaryUpperBound(key types.Comparable) types.Comparable {
	if key == nil {
		return nil
	}
	return types.CompositeUpperBound(key)
}

func primaryIndexAndKey(table *Table, keys map[string]types.Comparable) (*Index, types.Comparable, error) {
	for _, idx := range table.GetIndicesUnsafe() {
		if !idx.Primary {
			continue
		}
		key, ok := keys[idx.Name]
		if !ok {
			return nil, nil, fmt.Errorf("storage: primary key %s missing", idx.Name)
		}
		return idx, key, nil
	}
	return nil, nil, fmt.Errorf("storage: table %s has no primary key", table.Name)
}

func applyIndexPointers(table *Table, keys map[string]types.Comparable, offset int64) error {
	return applyIndexPointersWithLSN(table, keys, offset, 0)
}

func applyIndexPointersWithLSN(table *Table, keys map[string]types.Comparable, offset int64, lsn uint64) error {
	undos := make([]indexUpdateUndo, 0, len(keys))
	_, primaryKey, err := primaryIndexAndKey(table, keys)
	if err != nil {
		return err
	}
	for indexName, key := range keys {
		idx, ok := table.Indices[indexName]
		if !ok {
			rollbackIndexPointers(undos)
			return &errors.IndexNotFoundError{Name: indexName}
		}
		physicalKey := physicalIndexKey(idx, key, primaryKey)
		old, exists, err := idx.Tree.Get(physicalKey)
		if err != nil {
			rollbackIndexPointers(undos)
			return fmt.Errorf("index %s get failed: %w", indexName, err)
		}
		undo := indexUpdateUndo{index: idx, key: physicalKey, old: old, exists: exists}
		if treeV2, ok := idx.Tree.(*btreev2.BTreeV2); ok {
			if err := treeV2.ReplaceWithLSN(physicalKey, offset, lsn); err != nil {
				rollbackIndexPointers(undos)
				return fmt.Errorf("failed to update index %s: %w", indexName, err)
			}
		} else if err := idx.Tree.Replace(physicalKey, offset); err != nil {
			rollbackIndexPointers(undos)
			return fmt.Errorf("failed to update index %s: %w", indexName, err)
		}
		undo.changed = true
		undos = append(undos, undo)
	}
	return nil
}

func rollbackIndexPointers(undos []indexUpdateUndo) {
	for i := len(undos) - 1; i >= 0; i-- {
		undo := undos[i]
		if !undo.changed {
			continue
		}
		if undo.exists {
			_ = undo.index.Tree.Replace(undo.key, undo.old)
		} else {
			_, _ = undo.index.Tree.Remove(undo.key)
		}
	}
}
