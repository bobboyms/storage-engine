package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/bobboyms/storage-engine/pkg/codec"
	storageerrors "github.com/bobboyms/storage-engine/pkg/errors"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

var ErrSerializationConflict = errors.New("storage: serialization conflict")

type SerializationConflictError struct {
	TableName string
	IndexName string
	Key       types.Comparable
}

func (e *SerializationConflictError) Error() string {
	return fmt.Sprintf("storage: serialization conflict on %s.%s key %v", e.TableName, e.IndexName, e.Key)
}

func (e *SerializationConflictError) Unwrap() error {
	return ErrSerializationConflict
}

// WriteTransaction accumulates operations for atomic commit.
//
// lastLSN tracks the most recent WAL LSN written for this transaction.
// ARIES uses it to back-link each new entry to its predecessor
// (prevLSN), forming the per-tx chain required for safe restartable
// undo. lastLSN is updated as BEGIN / op / CLR / COMMIT entries are
// flushed to the WAL.
type WriteTransaction struct {
	engine     *StorageEngine
	txID       uint64
	readView   *Transaction
	writeSet   []writeOp
	readSet    map[string]readObservation
	pending    map[string]int
	savepoints []savepoint
	lastLSN    uint64
	committed  bool
	aborted    bool
	abortErr   error
	walBegun   bool
	mu         sync.Mutex
}

// savepoint marks a position in the transaction's buffered write set.
// Because a WriteTransaction stages all writes in memory and only flushes
// them to the WAL at Commit, a savepoint is simply the write-set length
// at the moment it was established; rolling back to it truncates the
// buffer. Row locks acquired after the savepoint are intentionally kept
// (matching standard SQL: ROLLBACK TO SAVEPOINT does not release locks).
type savepoint struct {
	name        string
	writeSetLen int
}

type readObservation struct {
	found     bool
	createLSN uint64
}

type writeOp struct {
	opType    uint8 // wal.EntryType
	tableName string
	indexName string
	key       types.Comparable
	// keys is set only for multi-index row ops (opType == wal.EntryMultiInsert):
	// it maps every index name to the row's logical key for that index. For
	// single-index ops it is nil and indexName/key are used instead.
	keys     map[string]types.Comparable
	document string
	lsn      uint64
}

// BeginWriteTransaction starts a new write transaction
func (se *StorageEngine) BeginWriteTransaction() *WriteTransaction {
	return se.BeginWriteTransactionWithIsolation(RepeatableRead)
}

func (se *StorageEngine) BeginWriteTransactionWithIsolation(level IsolationLevel) *WriteTransaction {
	return &WriteTransaction{
		engine:   se,
		txID:     se.nextTxID(),
		readView: se.BeginTransaction(level),
		writeSet: make([]writeOp, 0),
		readSet:  make(map[string]readObservation),
		pending:  make(map[string]int),
	}
}

// Put adds a put operation to the transaction buffer. ctx is honored
// while waiting for row locks; once the operation has been buffered,
// the work for it happens at Commit time.
func (tx *WriteTransaction) Put(ctx context.Context, tableName string, indexName string, key types.Comparable, document string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureWritableLocked(); err != nil {
		return err
	}

	table, err := tx.engine.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return err
	}
	index, err := table.GetIndex(indexName)
	if err != nil {
		return err
	}

	if index.Type != getTypeFromKey(key) {
		return &storageerrors.InvalidKeyTypeError{
			Name:     indexName,
			TypeName: index.Type.String(),
		}
	}

	resource, err := lockResourceForKey(tableName, indexName, key)
	if err != nil {
		return err
	}
	if err := tx.acquireLockLocked(ctx, resource); err != nil {
		return err
	}
	if err := tx.checkReadWriteConflictLocked(resource, tableName, indexName, key); err != nil {
		return err
	}

	tx.writeSet = append(tx.writeSet, writeOp{
		opType:    wal.EntryInsert, // We treat updates as inserts (log-structured)
		tableName: tableName,
		indexName: indexName,
		key:       key,
		document:  document,
	})
	tx.pending[resource] = len(tx.writeSet) - 1
	return nil
}

// Del adds a delete operation to the transaction buffer. ctx is honored
// while waiting for row locks.
func (tx *WriteTransaction) Del(ctx context.Context, tableName string, indexName string, key types.Comparable) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureWritableLocked(); err != nil {
		return err
	}

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
	if err := tx.acquireLockLocked(ctx, resource); err != nil {
		return err
	}
	if err := tx.checkReadWriteConflictLocked(resource, tableName, indexName, key); err != nil {
		return err
	}

	tx.writeSet = append(tx.writeSet, writeOp{
		opType:    wal.EntryDelete,
		tableName: tableName,
		indexName: indexName,
		key:       key,
	})
	tx.pending[resource] = len(tx.writeSet) - 1
	return nil
}

// WriteRow buffers a multi-index row write (insert or upsert) that keeps every
// index of the table in sync, mirroring the engine's auto-commit InsertRow /
// UpsertRow but staged transactionally. keys must contain the logical key for
// every index of the table (including the primary). When insertOnly is true the
// row is rejected if its primary key already exists in the latest committed
// state. ctx is honored while waiting for row locks; the actual heap and index
// mutations happen atomically at Commit.
func (tx *WriteTransaction) WriteRow(ctx context.Context, tableName string, document string, keys map[string]types.Comparable, insertOnly bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureWritableLocked(); err != nil {
		return err
	}
	if len(keys) == 0 {
		return fmt.Errorf("storage: WriteRow requires at least the primary key")
	}

	table, err := tx.engine.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return err
	}
	for indexName, key := range keys {
		index, err := table.GetIndex(indexName)
		if err != nil {
			return err
		}
		if index.Type != getTypeFromKey(key) {
			return &storageerrors.InvalidKeyTypeError{Name: indexName, TypeName: index.Type.String()}
		}
	}
	primary, primaryKey, err := primaryIndexAndKey(table, keys)
	if err != nil {
		return err
	}

	// Acquire every row lock in a deterministic order to avoid deadlocks,
	// then check for read/write conflicts on each indexed key.
	resources, err := lockResourcesForKeys(tableName, keys)
	if err != nil {
		return err
	}
	for _, resource := range resources {
		if err := tx.acquireLockLocked(ctx, resource); err != nil {
			return err
		}
	}
	for indexName, key := range keys {
		resource, err := lockResourceForKey(tableName, indexName, key)
		if err != nil {
			return err
		}
		if err := tx.checkReadWriteConflictLocked(resource, tableName, indexName, key); err != nil {
			return err
		}
	}

	if insertOnly {
		rec, err := tx.currentCommittedRecordRawLocked(tableName, primary.Name, primaryKey)
		if err != nil {
			return err
		}
		if rec.Found {
			return fmt.Errorf("duplicate key error: key %v already exists in index %s", primaryKey, primary.Name)
		}
	}

	opIdx := len(tx.writeSet)
	tx.writeSet = append(tx.writeSet, writeOp{
		opType:    wal.EntryMultiInsert,
		tableName: tableName,
		keys:      keys,
		document:  document,
	})
	for _, resource := range resources {
		tx.pending[resource] = opIdx
	}
	return nil
}

// GetBytes returns the raw heap bytes visible to this write transaction,
// honoring its read view and any pending writes already staged in the
// transaction. Pending Put operations are returned as their encoded
// canonical form (via the engine's codec); pending Deletes report
// found=false. ctx is checked at entry only — once the read view holds
// the snapshot LSN there is no further blocking work.
func (tx *WriteTransaction) GetBytes(ctx context.Context, tableName string, indexName string, key types.Comparable) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureWritableLocked(); err != nil {
		return nil, false, err
	}

	resource, err := lockResourceForKey(tableName, indexName, key)
	if err != nil {
		return nil, false, err
	}
	if idx, ok := tx.pending[resource]; ok {
		op := tx.writeSet[idx]
		if op.opType == wal.EntryDelete {
			return nil, false, nil
		}
		return encodeDocumentOrRaw(tx.engine.codec, op.document), true, nil
	}

	record, err := tx.readCommittedRecordRawLocked(tableName, indexName, key)
	if err != nil {
		return nil, false, err
	}
	tx.readSet[resource] = readObservation{
		found:     record.Found,
		createLSN: record.CreateLSN,
	}
	if !record.Found {
		return nil, false, nil
	}
	return record.Raw, true, nil
}

// GetForUpdate reads a row for a read-then-write decision, taking the
// row lock and returning the latest committed version (not the
// transaction's fixed snapshot). Holding the lock until the transaction
// ends serializes it against any other GetForUpdate or write on the same
// key, and reading the latest version means that once a blocking holder
// commits, this transaction observes their change. Together this lets
// callers prevent write skew on a known set of rows: lock every row the
// invariant depends on with GetForUpdate before deciding.
//
// It is the engine's equivalent of SQL `SELECT ... FOR UPDATE`. ctx is
// honored while waiting for the row lock.
func (tx *WriteTransaction) GetForUpdate(ctx context.Context, tableName string, indexName string, key types.Comparable) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureWritableLocked(); err != nil {
		return nil, false, err
	}

	resource, err := lockResourceForKey(tableName, indexName, key)
	if err != nil {
		return nil, false, err
	}

	// Acquire the row lock first so concurrent FOR-UPDATE reads / writes
	// on this key block until we finish. Once granted, any previous
	// holder has committed, so the latest version below reflects it.
	if err := tx.acquireLockLocked(ctx, resource); err != nil {
		return nil, false, err
	}

	// A pending write in this transaction wins, mirroring GetBytes.
	if idx, ok := tx.pending[resource]; ok {
		op := tx.writeSet[idx]
		if op.opType == wal.EntryDelete {
			return nil, false, nil
		}
		return encodeDocumentOrRaw(tx.engine.codec, op.document), true, nil
	}

	record, err := tx.currentCommittedRecordRawLocked(tableName, indexName, key)
	if err != nil {
		return nil, false, err
	}
	tx.readSet[resource] = readObservation{
		found:     record.Found,
		createLSN: record.CreateLSN,
	}
	if !record.Found {
		return nil, false, nil
	}
	return record.Raw, true, nil
}

// Commit persists all operations atomically.
//
// Cancellation: ctx is honored up to and including the moment the WAL
// BEGIN marker has been synced. Once BEGIN is durable, ctx cancellation
// no longer aborts the call — the commit either succeeds and applies
// to the heap/tree, or returns a degradation error. Cancelling after
// the point of no return is silently ignored so the engine does not
// leave half-applied state behind.
func (tx *WriteTransaction) Commit(ctx context.Context) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.engine.LockManager != nil {
		defer tx.engine.LockManager.ReleaseAll(tx.txID)
	}
	defer tx.closeReadViewLocked()
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

	// Last chance to abort cleanly before any WAL bytes hit disk.
	if err := ctx.Err(); err != nil {
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

			switch op.opType {
			case wal.EntryDelete:
				payload, err = SerializeDocumentEntry(op.tableName, op.indexName, op.key, nil)
			case wal.EntryMultiInsert:
				bsonData := encodeDocumentOrRaw(tx.engine.codec, op.document)
				payload, err = SerializeMultiIndexEntry(op.tableName, op.keys, bsonData)
			default:
				bsonData := encodeDocumentOrRaw(tx.engine.codec, op.document)
				payload, err = SerializeDocumentEntry(op.tableName, op.indexName, op.key, bsonData)
			}

			if err != nil {
				_ = tx.rollbackWAL()
				return err
			}

			entry := wal.AcquireEntry()
			entry.Header.Magic = wal.WALMagic
			entry.Header.Version = ariesWALVersion
			entry.Header.EntryType = op.opType
			entry.Header.LSN = opLSN
			payload = wrapAriesPayload(tx.txID, tx.lastLSN, payload)
			entry.Header.PayloadLen = uint32(len(payload)) //nolint:gosec // payload size bounded by record limits
			entry.Header.CRC32 = wal.CalculateCRC32(payload)
			entry.Payload = append(entry.Payload, payload...)

			if err := se.WAL.WriteEntry(entry); err != nil {
				wal.ReleaseEntry(entry)
				_ = tx.rollbackWAL()
				return fmt.Errorf("wal write failed: %w", err)
			}
			wal.ReleaseEntry(entry)
			tx.lastLSN = opLSN
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
			// Optionally recover in place: the COMMIT is durable, so an
			// idempotent WAL replay reconstructs the full committed state
			// without a reopen. We already hold se.opMu exclusively, so
			// call the lock-free heal variant. On success the commit is
			// complete; on failure the engine stays degraded.
			if se.autoHealAfterApplyFailure {
				if healErr := se.healLocked(ctx); healErr == nil {
					return nil
				}
			}
			return applyErr
		}
	}

	return nil
}

// Rollback discards all pending operations
// Rollback aborts the transaction. ctx is accepted for symmetry with
// the other operations but is intentionally not used to short-circuit
// the rollback path itself — once a rollback is in flight we always
// run it to completion to keep WAL state consistent.
func (tx *WriteTransaction) Rollback(ctx context.Context) error {
	_ = ctx
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.engine.LockManager != nil {
		defer tx.engine.LockManager.ReleaseAll(tx.txID)
	}
	defer tx.closeReadViewLocked()

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

// Savepoint establishes a named savepoint at the current point in the
// transaction. A later RollbackToSavepoint with the same name discards
// every operation buffered after this call while preserving earlier
// work. Establishing a savepoint with an existing name shadows the
// previous one until it is rolled back to or the transaction ends.
func (tx *WriteTransaction) Savepoint(name string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureWritableLocked(); err != nil {
		return err
	}
	tx.savepoints = append(tx.savepoints, savepoint{name: name, writeSetLen: len(tx.writeSet)})
	return nil
}

// RollbackToSavepoint discards every operation buffered after the most
// recent savepoint with the given name. The savepoint itself remains
// usable, but any savepoints established after it are removed. Row locks
// acquired after the savepoint are retained, matching standard SQL
// semantics. Returns an error if no matching savepoint exists.
func (tx *WriteTransaction) RollbackToSavepoint(name string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureWritableLocked(); err != nil {
		return err
	}

	idx := -1
	for i := len(tx.savepoints) - 1; i >= 0; i-- {
		if tx.savepoints[i].name == name {
			idx = i
			break
		}
	}
	if idx == -1 {
		return fmt.Errorf("storage: savepoint %q does not exist", name)
	}

	target := tx.savepoints[idx]
	if target.writeSetLen <= len(tx.writeSet) {
		tx.writeSet = tx.writeSet[:target.writeSetLen]
	}
	// Drop savepoints established after the matched one; keep the
	// matched savepoint so it can be rolled back to again.
	tx.savepoints = tx.savepoints[:idx+1]
	tx.rebuildPendingLocked()
	return nil
}

// rebuildPendingLocked recomputes the resource→write-set-index map from
// the surviving write set after a savepoint truncation. The last write
// to a resource wins, mirroring how Put/Del overwrite pending entries.
func (tx *WriteTransaction) rebuildPendingLocked() {
	tx.pending = make(map[string]int, len(tx.writeSet))
	for i := range tx.writeSet {
		op := &tx.writeSet[i]
		if op.opType == wal.EntryMultiInsert {
			for indexName, key := range op.keys {
				if resource, err := lockResourceForKey(op.tableName, indexName, key); err == nil {
					tx.pending[resource] = i
				}
			}
			continue
		}
		resource, err := lockResourceForKey(op.tableName, op.indexName, op.key)
		if err != nil {
			continue
		}
		tx.pending[resource] = i
	}
}

func (tx *WriteTransaction) ensureWritableLocked() error {
	if tx.committed {
		return fmt.Errorf("transaction already finished")
	}
	if tx.aborted {
		if tx.abortErr != nil {
			return tx.abortErr
		}
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

func (tx *WriteTransaction) closeReadViewLocked() {
	if tx.readView != nil {
		tx.readView.Close()
		tx.readView = nil
	}
}

func (tx *WriteTransaction) acquireLockLocked(ctx context.Context, resource string) error {
	if tx.engine.LockManager == nil {
		return nil
	}
	if err := tx.engine.LockManager.Acquire(ctx, tx.txID, resource); err != nil {
		tx.aborted = true
		tx.abortErr = err
		tx.writeSet = nil
		return err
	}
	return nil
}

func (tx *WriteTransaction) checkReadWriteConflictLocked(resource string, tableName string, indexName string, key types.Comparable) error {
	if _, alreadyPending := tx.pending[resource]; alreadyPending {
		return nil
	}

	observed, ok := tx.readSet[resource]
	if !ok {
		return nil
	}

	current, err := tx.currentCommittedObservationLocked(tableName, indexName, key)
	if err != nil {
		return err
	}
	if observed != current {
		tx.aborted = true
		conflictErr := &SerializationConflictError{
			TableName: tableName,
			IndexName: indexName,
			Key:       key,
		}
		tx.abortErr = conflictErr
		tx.writeSet = nil
		tx.pending = make(map[string]int)
		return conflictErr
	}
	return nil
}

func (tx *WriteTransaction) lockManagerAbortErrorLocked() error {
	if tx.engine.LockManager == nil {
		return nil
	}
	if err := tx.engine.LockManager.IsAborted(tx.txID); err != nil {
		tx.aborted = true
		tx.abortErr = err
		tx.writeSet = nil
		return err
	}
	return nil
}

func (tx *WriteTransaction) readCommittedRecordRawLocked(tableName string, indexName string, key types.Comparable) (visibleRecordRaw, error) {
	se := tx.engine
	se.opMu.RLock()
	defer se.opMu.RUnlock()
	if err := se.runtimeReadyError(); err != nil {
		return visibleRecordRaw{}, err
	}

	if tx.readView == nil {
		return visibleRecordRaw{}, fmt.Errorf("transaction already finished")
	}
	tx.readView.refreshSnapshot()
	return se.visibleRecordForKeyRaw(context.Background(), tx.readView, tableName, indexName, key)
}

// currentCommittedRecordRawLocked reads the latest committed version of
// the row (at the engine's current LSN) rather than the transaction's
// fixed snapshot. Used by GetForUpdate so a locking read observes the
// freshest committed state.
func (tx *WriteTransaction) currentCommittedRecordRawLocked(tableName string, indexName string, key types.Comparable) (visibleRecordRaw, error) {
	se := tx.engine
	se.opMu.RLock()
	defer se.opMu.RUnlock()
	if err := se.runtimeReadyError(); err != nil {
		return visibleRecordRaw{}, err
	}

	view := &Transaction{
		SnapshotLSN: se.lsnTracker.Current(),
		Level:       RepeatableRead,
		engine:      se,
	}
	return se.visibleRecordForKeyRaw(context.Background(), view, tableName, indexName, key)
}

func (tx *WriteTransaction) currentCommittedObservationLocked(tableName string, indexName string, key types.Comparable) (readObservation, error) {
	se := tx.engine
	se.opMu.RLock()
	defer se.opMu.RUnlock()
	if err := se.runtimeReadyError(); err != nil {
		return readObservation{}, err
	}

	view := &Transaction{
		SnapshotLSN: se.lsnTracker.Current(),
		Level:       RepeatableRead,
		engine:      se,
	}
	record, err := se.visibleRecordForKey(view, tableName, indexName, key)
	if err != nil {
		return readObservation{}, err
	}
	return readObservation{
		found:     record.Found,
		createLSN: record.CreateLSN,
	}, nil
}

func (tx *WriteTransaction) writeWALMarker(typeID uint8, lsn uint64) error {
	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = ariesWALVersion
	entry.Header.EntryType = typeID
	entry.Header.LSN = lsn
	prevLSN := tx.lastLSN
	entry.Payload = append(entry.Payload, wrapAriesPayload(tx.txID, prevLSN, nil)...)
	entry.Header.PayloadLen = uint32(len(entry.Payload)) //nolint:gosec // payload size bounded by tx marker size
	entry.Header.CRC32 = wal.CalculateCRC32(entry.Payload)

	if tx.engine.WAL == nil {
		wal.ReleaseEntry(entry)
		tx.lastLSN = lsn
		return nil
	}

	err := tx.engine.WAL.WriteEntry(entry)
	wal.ReleaseEntry(entry)
	if err == nil {
		tx.lastLSN = lsn
	}
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
	case types.BytesKey:
		return TypeBytes
	case types.UUIDKey:
		return TypeUUID
	case types.DecimalKey:
		return TypeDecimal
	case types.DateOnlyKey:
		return TypeDateOnly
	default:
		return TypeVarchar // Fallback
	}
}

func (tx *WriteTransaction) applyCommittedWriteOp(step int, total int, op writeOp) error {
	if op.opType == wal.EntryMultiInsert {
		return tx.applyCommittedRowOp(step, total, op)
	}

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
		physicalKey := singleIndexPhysicalKey(index, op.key)
		err = index.Tree.Upsert(physicalKey, func(oldOffset int64, exists bool) (int64, error) {
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
		bsonData := tx.opDocumentBytes(op)
		physicalKey := singleIndexPhysicalKey(index, op.key)

		err = index.Tree.Upsert(physicalKey, func(oldOffset int64, exists bool) (int64, error) {
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

// applyCommittedRowOp installs a committed multi-index row write: it writes the
// document to the heap exactly once and points every index at that single
// offset, mirroring the auto-commit writeRowLocked path so recovery (which
// redoes the same EntryMultiInsert) reconstructs an identical state. On an
// upsert the previous primary version is tombstoned.
func (tx *WriteTransaction) applyCommittedRowOp(step int, total int, op writeOp) error {
	table, err := tx.engine.TableMetaData.GetTableByName(op.tableName)
	if err != nil {
		return err
	}
	primary, primaryKey, err := primaryIndexAndKey(table, op.keys)
	if err != nil {
		return err
	}

	info := postCommitApplyInfo{
		TxID:      tx.txID,
		Step:      step,
		Total:     total,
		OpType:    op.opType,
		TableName: op.tableName,
		IndexName: primary.Name,
		Key:       primaryKey,
	}
	if err := tx.engine.runPostCommitApplyHook(withPostCommitStage(info, postCommitStageBeforeOp)); err != nil {
		return err
	}

	oldOffset, exists, err := primary.Tree.Get(primaryKey)
	if err != nil {
		return fmt.Errorf("primary index get failed: %w", err)
	}

	bsonData := tx.opDocumentBytes(op)
	prevOffset := int64(-1)
	if exists {
		prevOffset = oldOffset
	}
	offset, err := table.Heap.Write(bsonData, op.lsn, prevOffset)
	if err != nil {
		return fmt.Errorf("heap write failed: %w", err)
	}
	if err := tx.engine.runPostCommitApplyHook(withPostCommitStage(info, postCommitStageAfterHeapMutation)); err != nil {
		return err
	}

	if err := applyIndexPointersWithLSN(table, op.keys, offset, op.lsn); err != nil {
		return err
	}

	if exists {
		if err := table.Heap.Delete(oldOffset, op.lsn); err != nil && !isChainEndErr(err) {
			return fmt.Errorf("heap delete previous version failed: %w", err)
		}
	}
	if err := tx.engine.runPostCommitApplyHook(withPostCommitStage(info, postCommitStageAfterIndexInstall)); err != nil {
		return err
	}

	for indexName := range op.keys {
		tx.engine.appliedLSN.MarkApplied(op.tableName, indexName, op.lsn)
	}
	return nil
}

func (tx *WriteTransaction) opDocumentBytes(op writeOp) []byte {
	return encodeDocumentOrRaw(tx.engine.codec, op.document)
}

// encodeDocumentOrRaw encodes `doc` using the supplied codec, falling back to
// the raw bytes of the string when the codec cannot parse it (e.g. legacy
// non-JSON payloads). The fallback mirrors historical engine behavior.
func encodeDocumentOrRaw(c codec.Codec, doc string) []byte {
	if c != nil {
		if parsed, err := c.Parse(doc); err == nil {
			if raw, err := parsed.Bytes(); err == nil {
				return raw
			}
		}
	}
	return []byte(doc)
}

func withPostCommitStage(info postCommitApplyInfo, stage postCommitApplyStage) postCommitApplyInfo {
	info.Stage = stage
	return info
}
