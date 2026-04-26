package storage

import (
	"fmt"

	btreev2 "github.com/bobboyms/storage-engine/pkg/btree/v2"
	"github.com/bobboyms/storage-engine/pkg/errors"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type indexUpdateUndo struct {
	index   *Index
	key     types.Comparable
	old     int64
	exists  bool
	changed bool
}

func (se *StorageEngine) writeRow(tableName string, doc string, providedKeys map[string]types.Comparable, insertOnly bool) error {
	se.opMu.RLock()
	defer se.opMu.RUnlock()
	if err := se.runtimeReadyError(); err != nil {
		return err
	}

	return se.writeRowLocked(tableName, doc, providedKeys, insertOnly)
}

func (se *StorageEngine) writeRowLocked(tableName string, doc string, providedKeys map[string]types.Comparable, insertOnly bool) error {
	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return err
	}

	bsonData, keys, err := prepareRowDocument(table, doc, providedKeys)
	if err != nil {
		return err
	}

	resources, err := lockResourcesForKeys(tableName, keys)
	if err != nil {
		return err
	}

	return se.withAutoCommitLocks(resources, func() error {
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
			return fmt.Errorf("duplicate key error: key %v already exists in index %s", primaryKey, primary.Name)
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
	entry.Header.PayloadLen = uint32(len(payload))
	entry.Header.CRC32 = wal.CalculateCRC32(payload)
	entry.Payload = append(entry.Payload, payload...)

	err = se.WAL.WriteEntry(entry)
	wal.ReleaseEntry(entry)
	if err != nil {
		return fmt.Errorf("wal write failed: %w", err)
	}
	return nil
}

func prepareRowDocument(table *Table, doc string, providedKeys map[string]types.Comparable) ([]byte, map[string]types.Comparable, error) {
	if providedKeys == nil {
		providedKeys = map[string]types.Comparable{}
	}

	bsonDoc, err := JsonToBson(doc)
	if err == nil {
		keys, ok, err := keysFromBSONForAllIndexes(table, bsonDoc)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			return nil, nil, fmt.Errorf("storage: documento JSON nao contem todos os campos indexados")
		}
		for name, provided := range providedKeys {
			derived, ok := keys[name]
			if !ok {
				return nil, nil, &errors.IndexNotFoundError{Name: name}
			}
			if !sameComparableKey(derived, provided) {
				return nil, nil, fmt.Errorf("storage: chave informada %s=%v diverge do documento (%v)", name, provided, derived)
			}
		}
		bsonData, err := MarshalBson(bsonDoc)
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
			return nil, nil, fmt.Errorf("storage: chave obrigatoria para indice %s ausente", idx.Name)
		}
	}
	return []byte(doc), keys, nil
}

func keysFromBSONForAllIndexes(table *Table, bsonDoc bson.D) (map[string]types.Comparable, bool, error) {
	keys := make(map[string]types.Comparable)
	for _, idx := range table.GetIndices() {
		key, err := GetValueFromBson(bsonDoc, idx.Name)
		if err != nil {
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
	return a.Compare(b) == 0
}

func primaryIndexAndKey(table *Table, keys map[string]types.Comparable) (*Index, types.Comparable, error) {
	for _, idx := range table.GetIndicesUnsafe() {
		if !idx.Primary {
			continue
		}
		key, ok := keys[idx.Name]
		if !ok {
			return nil, nil, fmt.Errorf("storage: chave primaria %s ausente", idx.Name)
		}
		return idx, key, nil
	}
	return nil, nil, fmt.Errorf("storage: tabela %s sem chave primaria", table.Name)
}

func applyIndexPointers(table *Table, keys map[string]types.Comparable, offset int64) error {
	return applyIndexPointersWithLSN(table, keys, offset, 0)
}

func applyIndexPointersWithLSN(table *Table, keys map[string]types.Comparable, offset int64, lsn uint64) error {
	undos := make([]indexUpdateUndo, 0, len(keys))
	for indexName, key := range keys {
		idx, ok := table.Indices[indexName]
		if !ok {
			rollbackIndexPointers(undos)
			return &errors.IndexNotFoundError{Name: indexName}
		}
		old, exists, err := idx.Tree.Get(key)
		if err != nil {
			rollbackIndexPointers(undos)
			return fmt.Errorf("index %s get failed: %w", indexName, err)
		}
		undo := indexUpdateUndo{index: idx, key: key, old: old, exists: exists}
		if treeV2, ok := idx.Tree.(*btreev2.BTreeV2); ok {
			if err := treeV2.ReplaceWithLSN(key, offset, lsn); err != nil {
				rollbackIndexPointers(undos)
				return fmt.Errorf("failed to update index %s: %w", indexName, err)
			}
		} else if err := idx.Tree.Replace(key, offset); err != nil {
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
