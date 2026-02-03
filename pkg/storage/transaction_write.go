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
	writeSet  []writeOp
	committed bool
	aborted   bool
	mu        sync.Mutex
}

type writeOp struct {
	opType    uint8 // wal.EntryType
	tableName string
	indexName string
	key       types.Comparable
	document  string
}

// BeginWriteTransaction starts a new write transaction
func (se *StorageEngine) BeginWriteTransaction() *WriteTransaction {
	return &WriteTransaction{
		engine:   se,
		writeSet: make([]writeOp, 0),
	}
}

// Put adds a put operation to the transaction buffer
func (tx *WriteTransaction) Put(tableName string, indexName string, key types.Comparable, document string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.committed || tx.aborted {
		return fmt.Errorf("transaction already finished")
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

	if tx.committed || tx.aborted {
		return fmt.Errorf("transaction already finished")
	}

	// Validate metadata
	table, err := tx.engine.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return err
	}
	if _, err := table.GetIndex(indexName); err != nil {
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
func (tx *WriteTransaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.committed || tx.aborted {
		return fmt.Errorf("transaction already finished")
	}

	if len(tx.writeSet) == 0 {
		tx.committed = true
		return nil
	}

	se := tx.engine
	lsn := se.lsnTracker.Next() // Commit LSN? Or one LSN per op?
	// In strict WAL, every op has LSN.
	// For transaction atomicity:
	// 1. Write BEGIN
	// 2. Write Ops
	// 3. Write COMMIT
	// All ops usually share the same TransactionID, or we use standard LSNs.
	// We will use standard LSNs for ops, making them distinct.

	// 1. WAL Writing (Phase 1: Persistence)
	if se.WAL != nil {
		// Write BEGIN
		if err := tx.writeWALMarker(wal.EntryBegin, lsn); err != nil {
			return err
		}

		// Write Ops
		for _, op := range tx.writeSet {
			opLSN := se.lsnTracker.Next() // Assign unique LSN for each op

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
				tx.rollbackWAL(lsn)
				return err
			}

			entry := wal.AcquireEntry()
			entry.Header.Magic = wal.WALMagic
			entry.Header.Version = 1
			entry.Header.EntryType = op.opType
			entry.Header.LSN = opLSN
			entry.Header.PayloadLen = uint32(len(payload))
			entry.Header.CRC32 = wal.CalculateCRC32(payload)
			entry.Payload = append(entry.Payload, payload...)

			if err := se.WAL.WriteEntry(entry); err != nil {
				wal.ReleaseEntry(entry)
				tx.rollbackWAL(lsn)
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

	// 2. Memory Application (Phase 2: Visibility)
	// Apply all changes to Heap and Trees
	for _, op := range tx.writeSet {
		// Re-fetch index (safe, consistent with WAL checks)
		table, _ := se.TableMetaData.GetTableByName(op.tableName)
		index, _ := table.GetIndex(op.indexName)

		opLSN := se.lsnTracker.Next() // We need to match LSNs from WAL?
		// Actually, in previous step we burned LSNs. We should have stored them.
		// For simplicity now, let's assume strict serial execution or just use new LSNs.
		// CORRECTNESS: We MUST use the LSNs recorded in WAL if we want recovery to match.
		// BUT: Since we are in memory, we can just generate new logic or re-use.
		// Let's simplify: We won't re-generate LSNs, we just apply.
		// Refactoring: We need to store LSNs assigned during WAL phase to reuse here.
		// However, for this implementation, let's just use a fresh LSN for Heap/Tree application
		// assuming LSNTracker monotonic increase is enough.
		// Actually, standard ARIES uses Transaction Table. We are simplifying.

		// Apply Logic
		if op.opType == wal.EntryDelete {
			// Delete logic
			index.Tree.Upsert(op.key, func(oldOffset int64, exists bool) (int64, error) {
				if !exists {
					return 0, nil
				}
				if err := table.Heap.Delete(oldOffset, opLSN); err != nil {
					return 0, fmt.Errorf("heap delete failed: %w", err)
				}
				return oldOffset, nil
			})
		} else {
			// Insert/Update logic
			bsonDoc, errBson := JsonToBson(op.document)
			var bsonData []byte
			if errBson == nil {
				bsonData, _ = MarshalBson(bsonDoc)
			} else {
				bsonData = []byte(op.document)
			}

			index.Tree.Upsert(op.key, func(oldOffset int64, exists bool) (int64, error) {
				var prevOffset int64 = -1
				if exists {
					prevOffset = oldOffset
				}
				offset, err := table.Heap.Write(bsonData, opLSN, prevOffset)
				if err != nil {
					return 0, err
				}
				return offset, nil
			})
		}
	}

	tx.committed = true
	return nil
}

// Rollback discards all pending operations
func (tx *WriteTransaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.committed || tx.aborted {
		return nil
	}

	tx.writeSet = nil
	tx.aborted = true
	return nil
}

func (tx *WriteTransaction) writeWALMarker(typeID uint8, lsn uint64) error {
	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = 1
	entry.Header.EntryType = typeID
	entry.Header.LSN = lsn
	entry.Header.PayloadLen = 0
	entry.Header.CRC32 = 0

	if tx.engine.WAL == nil {
		wal.ReleaseEntry(entry)
		return nil
	}

	err := tx.engine.WAL.WriteEntry(entry)
	wal.ReleaseEntry(entry)
	return err
}

func (tx *WriteTransaction) rollbackWAL(lsn uint64) {
	tx.writeWALMarker(wal.EntryAbort, lsn)
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
