package storage

import (
	"fmt"
	"sync"

	"github.com/bobboyms/storage-engine/pkg/errors"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// WriteTransaction accumulates operations for atomic commit
type WriteTransaction struct {
	engine    *StorageEngine
	txID      uint64
	writeSet  []writeOp
	committed bool
	aborted   bool
	walBegun  bool
	mu        sync.Mutex
}

type writeOp struct {
	opType    uint8 // wal.EntryType
	tableName string
	indexName string
	key       types.Comparable
	document  string
	lsn       uint64
}

// BeginWriteTransaction starts a new write transaction
func (se *StorageEngine) BeginWriteTransaction() *WriteTransaction {
	return &WriteTransaction{
		engine:   se,
		txID:     se.nextTxID(),
		writeSet: make([]writeOp, 0),
	}
}

// Put adds a put operation to the transaction buffer
func (tx *WriteTransaction) Put(tableName string, indexName string, key types.Comparable, document string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureWritableLocked(); err != nil {
		return err
	}

	// Validate metadata immediately to fail fast
	table, err := tx.engine.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return err
	}
	index, err := table.GetIndex(indexName)
	if err != nil {
		return err
	}

	// Validate types
	// Using generic check here, full validation happens at commit or we duplicate logic?
	// Better to duplicate critical checks or reuse existing private methods
	// We will validate basically here
	if index.Type != getTypeFromKey(key) {
		return &errors.InvalidKeyTypeError{
			Name:     indexName,
			TypeName: index.Type.String(),
		}
	}

	resource, err := lockResourceForKey(tableName, indexName, key)
	if err != nil {
		return err
	}
	if err := tx.acquireLockLocked(resource); err != nil {
		return err
	}

	tx.writeSet = append(tx.writeSet, writeOp{
		opType:    wal.EntryInsert, // We treat updates as inserts (log-structured)
		tableName: tableName,
		indexName: indexName,
		key:       key,
		document:  document,
	})
	return nil
}

// Del adds a delete operation to the transaction buffer
func (tx *WriteTransaction) Del(tableName string, indexName string, key types.Comparable) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureWritableLocked(); err != nil {
		return err
	}

	// Validate metadata
	table, err := tx.engine.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return err
	}
	if _, err := table.GetIndex(indexName); err != nil {
		return err
	}

	resource, err := lockResourceForKey(tableName, indexName, key)
	if err != nil {
		return err
	}
	if err := tx.acquireLockLocked(resource); err != nil {
		return err
	}

	tx.writeSet = append(tx.writeSet, writeOp{
		opType:    wal.EntryDelete,
		tableName: tableName,
		indexName: indexName,
		key:       key,
	})
	return nil
}

// Commit persists all operations atomically
func (tx *WriteTransaction) Commit() (err error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.engine.LockManager != nil {
		defer tx.engine.LockManager.ReleaseAll(tx.txID)
	}
	defer func() {
		if err != nil && !tx.committed {
			tx.aborted = true
			tx.writeSet = nil
		}
	}()

	if err := tx.ensureWritableLocked(); err != nil {
		return err
	}

	se := tx.engine
	se.opMu.Lock()
	defer se.opMu.Unlock()
	if err := se.runtimeReadyError(); err != nil {
		return err
	}

	if len(tx.writeSet) == 0 {
		if se.WAL != nil {
			beginLSN := se.lsnTracker.Next()
			if err := tx.writeWALMarker(wal.EntryBegin, beginLSN); err != nil {
				return err
			}
			tx.walBegun = true

			commitLSN := se.lsnTracker.Next()
			if err := tx.writeWALMarker(wal.EntryCommit, commitLSN); err != nil {
				return err
			}
		}
		tx.committed = true
		return nil
	}

	beginLSN := se.lsnTracker.Next()
	for i := range tx.writeSet {
		tx.writeSet[i].lsn = se.lsnTracker.Next()
	}

	// 1. WAL Writing (Phase 1: Persistence)
	if se.WAL != nil {
		// Write BEGIN
		if err := tx.writeWALMarker(wal.EntryBegin, beginLSN); err != nil {
			return err
		}
		tx.walBegun = true

		// Write Ops
		for i := range tx.writeSet {
			op := &tx.writeSet[i]
			opLSN := op.lsn

			var payload []byte
			var err error

			if op.opType == wal.EntryDelete {
				payload, err = SerializeDocumentEntry(op.tableName, op.indexName, op.key, nil)
			} else {
				// Convert doc to bytes (BSON conversion logic duplicated from Put)
				bsonDoc, errBson := JsonToBson(op.document)
				var bsonData []byte
				if errBson == nil {
					bsonData, _ = MarshalBson(bsonDoc)
				} else {
					bsonData = []byte(op.document)
				}
				payload, err = SerializeDocumentEntry(op.tableName, op.indexName, op.key, bsonData)
			}

			if err != nil {
				_ = tx.rollbackWAL()
				return err
			}

			entry := wal.AcquireEntry()
			entry.Header.Magic = wal.WALMagic
			entry.Header.Version = txAwareWALVersion
			entry.Header.EntryType = op.opType
			entry.Header.LSN = opLSN
			payload = wrapTxPayload(tx.txID, payload)
			entry.Header.PayloadLen = uint32(len(payload))
			entry.Header.CRC32 = wal.CalculateCRC32(payload)
			entry.Payload = append(entry.Payload, payload...)

			if err := se.WAL.WriteEntry(entry); err != nil {
				wal.ReleaseEntry(entry)
				_ = tx.rollbackWAL()
				return fmt.Errorf("wal write failed: %w", err)
			}
			wal.ReleaseEntry(entry)
		}

		// Write COMMIT
		commitLSN := se.lsnTracker.Next()
		if err := tx.writeWALMarker(wal.EntryCommit, commitLSN); err != nil {
			return err
		}
	}
	tx.committed = true

	// 2. Memory Application (Phase 2: Visibility)
	// Apply all changes to Heap and Trees under the engine-wide write barrier.
	for i, op := range tx.writeSet {
		if err := tx.applyCommittedWriteOp(i+1, len(tx.writeSet), op); err != nil {
			applyErr := fmt.Errorf("post-commit apply failed for tx %d at op %d/%d (%s.%s): %w", tx.txID, i+1, len(tx.writeSet), op.tableName, op.indexName, err)
			se.markDegraded(applyErr)
			return applyErr
		}
	}

	return nil
}

// Rollback discards all pending operations
func (tx *WriteTransaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.engine.LockManager != nil {
		defer tx.engine.LockManager.ReleaseAll(tx.txID)
	}

	if tx.committed || tx.aborted {
		return nil
	}

	se := tx.engine
	se.opMu.RLock()
	defer se.opMu.RUnlock()

	if se.WAL != nil {
		if !tx.walBegun {
			beginLSN := se.lsnTracker.Next()
			if err := tx.writeWALMarker(wal.EntryBegin, beginLSN); err != nil {
				return err
			}
			tx.walBegun = true
		}
		if err := tx.rollbackWAL(); err != nil {
			return err
		}
	}

	tx.writeSet = nil
	tx.aborted = true
	return nil
}

func (tx *WriteTransaction) ensureWritableLocked() error {
	if tx.committed {
		return fmt.Errorf("transaction already finished")
	}
	if tx.aborted {
		if err := tx.lockManagerAbortErrorLocked(); err != nil {
			return err
		}
		return fmt.Errorf("transaction already finished")
	}
	if err := tx.lockManagerAbortErrorLocked(); err != nil {
		return err
	}
	return nil
}

func (tx *WriteTransaction) acquireLockLocked(resource string) error {
	if tx.engine.LockManager == nil {
		return nil
	}
	if err := tx.engine.LockManager.Acquire(tx.txID, resource); err != nil {
		tx.aborted = true
		tx.writeSet = nil
		return err
	}
	return nil
}

func (tx *WriteTransaction) lockManagerAbortErrorLocked() error {
	if tx.engine.LockManager == nil {
		return nil
	}
	if err := tx.engine.LockManager.IsAborted(tx.txID); err != nil {
		tx.aborted = true
		tx.writeSet = nil
		return err
	}
	return nil
}

func (tx *WriteTransaction) writeWALMarker(typeID uint8, lsn uint64) error {
	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = txAwareWALVersion
	entry.Header.EntryType = typeID
	entry.Header.LSN = lsn
	entry.Payload = append(entry.Payload, wrapTxPayload(tx.txID, nil)...)
	entry.Header.PayloadLen = uint32(len(entry.Payload))
	entry.Header.CRC32 = wal.CalculateCRC32(entry.Payload)

	if tx.engine.WAL == nil {
		wal.ReleaseEntry(entry)
		return nil
	}

	err := tx.engine.WAL.WriteEntry(entry)
	wal.ReleaseEntry(entry)
	return err
}

func (tx *WriteTransaction) rollbackWAL() error {
	if !tx.walBegun || tx.engine.WAL == nil {
		return nil
	}
	abortLSN := tx.engine.lsnTracker.Next()
	return tx.writeWALMarker(wal.EntryAbort, abortLSN)
}

func getTypeFromKey(k types.Comparable) DataType {
	// Helper to match Key type to DataType enum
	// In table.go DataTypeInt matches TypeInt, etc.
	// We need to implement this switch or use common util
	// For now, minimal implementation:
	switch k.(type) {
	case types.IntKey:
		return TypeInt
	case types.VarcharKey:
		return TypeVarchar
	case types.BoolKey:
		return TypeBoolean
	case types.FloatKey:
		return TypeFloat
	case types.DateKey:
		return TypeDate
	default:
		return TypeVarchar // Fallback
	}
}

func (tx *WriteTransaction) applyCommittedWriteOp(step int, total int, op writeOp) error {
	table, err := tx.engine.TableMetaData.GetTableByName(op.tableName)
	if err != nil {
		return err
	}
	index, err := table.GetIndex(op.indexName)
	if err != nil {
		return err
	}

	info := postCommitApplyInfo{
		TxID:      tx.txID,
		Step:      step,
		Total:     total,
		OpType:    op.opType,
		TableName: op.tableName,
		IndexName: op.indexName,
		Key:       op.key,
	}
	if err := tx.engine.runPostCommitApplyHook(withPostCommitStage(info, postCommitStageBeforeOp)); err != nil {
		return err
	}

	if op.opType == wal.EntryDelete {
		err = index.Tree.Upsert(op.key, func(oldOffset int64, exists bool) (int64, error) {
			if !exists {
				return 0, nil
			}
			if err := table.Heap.Delete(oldOffset, op.lsn); err != nil {
				if isChainEndErr(err) {
					return oldOffset, nil
				}
				return 0, fmt.Errorf("heap delete failed: %w", err)
			}
			if err := tx.engine.runPostCommitApplyHook(withPostCommitStage(info, postCommitStageAfterHeapMutation)); err != nil {
				return 0, err
			}
			return oldOffset, nil
		})
		if err != nil {
			return err
		}
	} else {
		bsonData, err := tx.opDocumentBytes(op)
		if err != nil {
			return err
		}

		err = index.Tree.Upsert(op.key, func(oldOffset int64, exists bool) (int64, error) {
			prevOffset := int64(-1)
			if exists {
				prevOffset = oldOffset
			}
			offset, err := table.Heap.Write(bsonData, op.lsn, prevOffset)
			if err != nil {
				return 0, err
			}
			if err := tx.engine.runPostCommitApplyHook(withPostCommitStage(info, postCommitStageAfterHeapMutation)); err != nil {
				return 0, err
			}
			return offset, nil
		})
		if err != nil {
			return err
		}
	}

	if err := tx.engine.runPostCommitApplyHook(withPostCommitStage(info, postCommitStageAfterIndexInstall)); err != nil {
		return err
	}

	tx.engine.appliedLSN.MarkApplied(op.tableName, op.indexName, op.lsn)
	return nil
}

func (tx *WriteTransaction) opDocumentBytes(op writeOp) ([]byte, error) {
	bsonDoc, errBson := JsonToBson(op.document)
	if errBson == nil {
		bsonData, err := MarshalBson(bsonDoc)
		if err != nil {
			return nil, err
		}
		return bsonData, nil
	}
	return []byte(op.document), nil
}

func withPostCommitStage(info postCommitApplyInfo, stage postCommitApplyStage) postCommitApplyInfo {
	info.Stage = stage
	return info
}
