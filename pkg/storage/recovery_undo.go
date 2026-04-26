package storage

import (
	"fmt"
	"io"
	"slices"

	btreev2 "github.com/bobboyms/storage-engine/pkg/btree/v2"
	"github.com/bobboyms/storage-engine/pkg/crypto"
	v2 "github.com/bobboyms/storage-engine/pkg/heap/v2"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type undoTask struct {
	txID      uint64
	entryType uint8
	lsn       uint64
	payload   []byte
}

type compensationEntry struct {
	OriginalLSN       uint64
	OriginalEntryType uint8
	OriginalPayload   []byte
	UndoNextLSN       uint64
}

func (se *StorageEngine) undoLoserTransactions(walPath string, cipher crypto.Cipher, analysis *recoveryAnalysis) error {
	_, err := se.undoLoserTransactionsWithLimit(walPath, cipher, analysis, 0)
	return err
}

func (se *StorageEngine) undoLoserTransactionsWithLimit(walPath string, cipher crypto.Cipher, analysis *recoveryAnalysis, maxSteps int) (int, error) {
	tasks, err := se.collectLoserUndoTasks(walPath, cipher, analysis)
	if err != nil {
		return 0, err
	}
	if len(tasks) == 0 {
		for txID := range analysis.LoserTxs {
			if err := se.writeTxAbortMarker(txID); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}

	remainingPerTx := make(map[uint64]int)
	descendingLSNs := make(map[uint64][]uint64)
	for _, task := range tasks {
		remainingPerTx[task.txID]++
		descendingLSNs[task.txID] = append(descendingLSNs[task.txID], task.lsn)
	}
	for txID := range descendingLSNs {
		slices.Sort(descendingLSNs[txID])
		slices.Reverse(descendingLSNs[txID])
	}

	processed := 0
	for i := len(tasks) - 1; i >= 0; i-- {
		task := tasks[i]
		if maxSteps > 0 && processed >= maxSteps {
			break
		}

		remaining := descendingLSNs[task.txID]
		undoNextLSN := uint64(0)
		for idx, lsn := range remaining {
			if lsn != task.lsn {
				continue
			}
			if idx+1 < len(remaining) {
				undoNextLSN = remaining[idx+1]
			}
			descendingLSNs[task.txID] = append(remaining[:idx], remaining[idx+1:]...)
			break
		}

		clr := compensationEntry{
			OriginalLSN:       task.lsn,
			OriginalEntryType: task.entryType,
			OriginalPayload:   task.payload,
			UndoNextLSN:       undoNextLSN,
		}
		clrLSN, err := se.writeCompensationLogRecord(task.txID, clr)
		if err != nil {
			return processed, err
		}
		if err := se.applyCompensation(clrLSN, clr); err != nil {
			return processed, err
		}

		if _, ok := analysis.UndoneLSNs[task.txID]; !ok {
			analysis.UndoneLSNs[task.txID] = make(map[uint64]struct{})
		}
		analysis.UndoneLSNs[task.txID][task.lsn] = struct{}{}
		processed++

		remainingPerTx[task.txID]--
		if remainingPerTx[task.txID] == 0 {
			if err := se.writeTxAbortMarker(task.txID); err != nil {
				return processed, err
			}
		}
	}

	return processed, nil
}

func (se *StorageEngine) collectLoserUndoTasks(walPath string, cipher crypto.Cipher, analysis *recoveryAnalysis) ([]undoTask, error) {
	reader, err := wal.NewWALReaderWithCipher(walPath, cipher)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	tasks := make([]undoTask, 0)
	for count := 0; ; count++ {
		entry, err := reader.ReadEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			if isExpectedWALTail(err) {
				break
			}
			return nil, fmt.Errorf("undo scan error at entry %d: %w", count, err)
		}

		txID, payload, transactional, err := unwrapTxPayload(entry.Header, entry.Payload)
		if err != nil {
			wal.ReleaseEntry(entry)
			return nil, fmt.Errorf("undo scan unwrap failed at entry %d: %w", count, err)
		}
		if !transactional {
			wal.ReleaseEntry(entry)
			continue
		}
		if _, loser := analysis.LoserTxs[txID]; !loser {
			wal.ReleaseEntry(entry)
			continue
		}
		if undone, ok := analysis.UndoneLSNs[txID]; ok {
			if _, alreadyUndone := undone[entry.Header.LSN]; alreadyUndone {
				wal.ReleaseEntry(entry)
				continue
			}
		}

		switch entry.Header.EntryType {
		case wal.EntryInsert, wal.EntryUpdate, wal.EntryDelete, wal.EntryMultiInsert:
			body := append([]byte(nil), payload...)
			tasks = append(tasks, undoTask{
				txID:      txID,
				entryType: entry.Header.EntryType,
				lsn:       entry.Header.LSN,
				payload:   body,
			})
		}

		wal.ReleaseEntry(entry)
	}
	return tasks, nil
}

func (se *StorageEngine) writeCompensationLogRecord(txID uint64, clr compensationEntry) (uint64, error) {
	lsn := se.lsnTracker.Next()
	payload := wrapTxPayload(txID, SerializeCompensationEntry(clr.OriginalLSN, clr.OriginalEntryType, clr.OriginalPayload, clr.UndoNextLSN))

	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = txAwareWALVersion
	entry.Header.EntryType = wal.EntryCLR
	entry.Header.LSN = lsn
	entry.Header.PayloadLen = uint32(len(payload))
	entry.Header.CRC32 = wal.CalculateCRC32(payload)
	entry.Payload = append(entry.Payload, payload...)

	err := se.WAL.WriteEntry(entry)
	wal.ReleaseEntry(entry)
	if err != nil {
		return 0, fmt.Errorf("wal write clr failed: %w", err)
	}
	return lsn, nil
}

func (se *StorageEngine) writeTxAbortMarker(txID uint64) error {
	if se.WAL == nil {
		return nil
	}
	lsn := se.lsnTracker.Next()
	payload := wrapTxPayload(txID, nil)

	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = txAwareWALVersion
	entry.Header.EntryType = wal.EntryAbort
	entry.Header.LSN = lsn
	entry.Header.PayloadLen = uint32(len(payload))
	entry.Header.CRC32 = wal.CalculateCRC32(payload)
	entry.Payload = append(entry.Payload, payload...)

	err := se.WAL.WriteEntry(entry)
	wal.ReleaseEntry(entry)
	if err != nil {
		return fmt.Errorf("wal write abort failed: %w", err)
	}
	return nil
}

func (se *StorageEngine) redoCompensationEntry(entry *wal.WALEntry, payload []byte) error {
	originalLSN, originalEntryType, originalPayload, undoNextLSN, err := DeserializeCompensationEntry(payload)
	if err != nil {
		return err
	}
	return se.applyCompensation(entry.Header.LSN, compensationEntry{
		OriginalLSN:       originalLSN,
		OriginalEntryType: originalEntryType,
		OriginalPayload:   originalPayload,
		UndoNextLSN:       undoNextLSN,
	})
}

func (se *StorageEngine) applyCompensation(clrLSN uint64, clr compensationEntry) error {
	switch clr.OriginalEntryType {
	case wal.EntryInsert, wal.EntryUpdate, wal.EntryDelete:
		return se.undoDocumentEntry(clr.OriginalEntryType, clr.OriginalLSN, clr.OriginalPayload, clrLSN)
	case wal.EntryMultiInsert:
		return se.undoMultiInsertEntry(clr.OriginalLSN, clr.OriginalPayload, clrLSN)
	default:
		return fmt.Errorf("unsupported compensation entry type %d", clr.OriginalEntryType)
	}
}

func (se *StorageEngine) undoDocumentEntry(entryType uint8, originalLSN uint64, payload []byte, clrLSN uint64) error {
	tableName, indexName, key, _, err := DeserializeDocumentEntry(payload)
	if err != nil {
		return err
	}

	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return nil
	}
	index, err := table.GetIndex(indexName)
	if err != nil {
		return nil
	}

	table.Lock()
	defer table.Unlock()

	head, found, err := index.Tree.Get(key)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	switch entryType {
	case wal.EntryDelete:
		deleteRID, _, err := findRecordByDeleteLSN(table, head, originalLSN)
		if err != nil {
			return err
		}
		if deleteRID == -1 {
			return nil
		}
		if err := undeleteRecord(table.Heap, deleteRID, originalLSN, clrLSN); err != nil {
			return err
		}
	case wal.EntryInsert, wal.EntryUpdate:
		targetRID, targetHdr, err := findRecordByCreateLSN(table, head, originalLSN)
		if err != nil {
			return err
		}
		if targetRID == -1 {
			return nil
		}
		if targetHdr.PrevRecordID == -1 {
			if err := removeIndexKeyWithLSN(index, key, clrLSN); err != nil {
				return err
			}
		} else if err := replaceIndexKeyWithLSN(index, key, targetHdr.PrevRecordID, clrLSN); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported undo document entry type %d", entryType)
	}

	se.appliedLSN.MarkApplied(tableName, indexName, clrLSN)
	return nil
}

func (se *StorageEngine) undoMultiInsertEntry(originalLSN uint64, payload []byte, clrLSN uint64) error {
	tableName, newKeys, _, err := DeserializeMultiIndexEntry(payload)
	if err != nil {
		return err
	}

	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return nil
	}

	primary, primaryKey, err := primaryIndexAndKey(table, newKeys)
	if err != nil {
		return err
	}

	table.Lock()
	defer table.Unlock()

	head, found, err := primary.Tree.Get(primaryKey)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	targetRID, targetHdr, err := findRecordByCreateLSN(table, head, originalLSN)
	if err != nil {
		return err
	}
	if targetRID == -1 {
		return nil
	}

	if targetHdr.PrevRecordID == -1 {
		for indexName, key := range newKeys {
			idx, ok := table.Indices[indexName]
			if !ok {
				continue
			}
			if err := removeIndexKeyIfMatchesWithLSN(idx, key, targetRID, clrLSN); err != nil {
				return err
			}
			se.appliedLSN.MarkApplied(tableName, indexName, clrLSN)
		}
		return nil
	}

	prevDoc, _, err := table.Heap.Read(targetHdr.PrevRecordID)
	if err != nil {
		return err
	}
	oldKeys, err := keysFromStoredDocument(table, prevDoc)
	if err != nil {
		return err
	}
	if err := undeleteRecord(table.Heap, targetHdr.PrevRecordID, originalLSN, clrLSN); err != nil {
		return err
	}
	if err := applyIndexPointersWithLSN(table, oldKeys, targetHdr.PrevRecordID, clrLSN); err != nil {
		return err
	}
	for indexName := range oldKeys {
		se.appliedLSN.MarkApplied(tableName, indexName, clrLSN)
	}
	for indexName, newKey := range newKeys {
		oldKey, exists := oldKeys[indexName]
		if exists && sameComparableKey(oldKey, newKey) {
			continue
		}
		idx, ok := table.Indices[indexName]
		if !ok {
			continue
		}
		if err := removeIndexKeyIfMatchesWithLSN(idx, newKey, targetRID, clrLSN); err != nil {
			return err
		}
		se.appliedLSN.MarkApplied(tableName, indexName, clrLSN)
	}
	return nil
}

func findRecordByCreateLSN(table *Table, head int64, createLSN uint64) (int64, *v2.RecordHeader, error) {
	for rid := head; rid != -1; {
		_, hdr, err := table.Heap.Read(rid)
		if isChainEndErr(err) {
			return -1, nil, nil
		}
		if err != nil {
			return -1, nil, err
		}
		if hdr.CreateLSN == createLSN {
			return rid, hdr, nil
		}
		rid = hdr.PrevRecordID
	}
	return -1, nil, nil
}

func findRecordByDeleteLSN(table *Table, head int64, deleteLSN uint64) (int64, *v2.RecordHeader, error) {
	for rid := head; rid != -1; {
		_, hdr, err := table.Heap.Read(rid)
		if isChainEndErr(err) {
			return -1, nil, nil
		}
		if err != nil {
			return -1, nil, err
		}
		if hdr.DeleteLSN == deleteLSN {
			return rid, hdr, nil
		}
		rid = hdr.PrevRecordID
	}
	return -1, nil, nil
}

func undeleteRecord(h interface{}, rid int64, expectedDeleteLSN uint64, pageLSN uint64) error {
	type undeleteHeap interface {
		Undelete(int64, uint64, uint64) error
	}
	if heap, ok := h.(undeleteHeap); ok {
		return heap.Undelete(rid, expectedDeleteLSN, pageLSN)
	}
	return fmt.Errorf("heap does not support undelete")
}

func replaceIndexKeyWithLSN(index *Index, key types.Comparable, offset int64, lsn uint64) error {
	if treeV2, ok := index.Tree.(*btreev2.BTreeV2); ok {
		return treeV2.ReplaceWithLSN(key, offset, lsn)
	}
	return index.Tree.Replace(key, offset)
}

func removeIndexKeyWithLSN(index *Index, key types.Comparable, lsn uint64) error {
	if treeV2, ok := index.Tree.(*btreev2.BTreeV2); ok {
		_, err := treeV2.DeleteWithLSN(key, lsn)
		return err
	}
	_, err := index.Tree.Remove(key)
	return err
}

func removeIndexKeyIfMatchesWithLSN(index *Index, key types.Comparable, expectedOffset int64, lsn uint64) error {
	current, found, err := index.Tree.Get(key)
	if err != nil || !found {
		return err
	}
	if current != expectedOffset {
		return nil
	}
	return removeIndexKeyWithLSN(index, key, lsn)
}

func keysFromStoredDocument(table *Table, docBytes []byte) (map[string]types.Comparable, error) {
	if bsonDoc, err := UnmarshalBson(docBytes); err == nil {
		keys, ok, keysErr := keysFromBSONForIndexes(table.GetIndicesUnsafe(), bsonDoc)
		if keysErr != nil {
			return nil, keysErr
		}
		if ok {
			return keys, nil
		}
	}
	bsonDoc, err := JsonToBson(string(docBytes))
	if err != nil {
		return nil, err
	}
	keys, ok, err := keysFromBSONForIndexes(table.GetIndicesUnsafe(), bsonDoc)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("storage: stored document missing indexed fields")
	}
	return keys, nil
}

func keysFromBSONForIndexes(indexes []*Index, bsonDoc bson.D) (map[string]types.Comparable, bool, error) {
	keys := make(map[string]types.Comparable)
	for _, idx := range indexes {
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
