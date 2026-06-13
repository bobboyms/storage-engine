package storage

import (
	"context"
	goerrors "errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bobboyms/storage-engine/pkg/btree"
	btreev2 "github.com/bobboyms/storage-engine/pkg/btree/v2"
	"github.com/bobboyms/storage-engine/pkg/codec"
	"github.com/bobboyms/storage-engine/pkg/codec/bsoncodec"
	"github.com/bobboyms/storage-engine/pkg/crypto"
	"github.com/bobboyms/storage-engine/pkg/errors"
	"github.com/bobboyms/storage-engine/pkg/heap"
	v2 "github.com/bobboyms/storage-engine/pkg/heap/v2"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
	"github.com/google/uuid"
)

// isChainEndErr returns true if err indicates the slot/record was
// reclaimed by vacuum — while walking the chain, we must treat it as the
// end (not as a real I/O error).
func isChainEndErr(err error) bool {
	return goerrors.Is(err, v2.ErrVacuumed)
}

func GenerateKey() (string, error) {
	// NewV7 generates a UUID from the current time and secure randomness.
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

type StorageEngine struct {
	TableMetaData *TableMetaData
	WAL           *wal.WALWriter // persistent WAL
	LockManager   *LockManager
	lsnTracker    *LSNTracker
	// txIDCounter is an independent monotonic source for transaction
	// IDs. It is intentionally NOT seeded from the WAL's max LSN: txIDs
	// are unique within a single engine instance (like Postgres XIDs)
	// and reset to 0 on restart. Mixing the two counters previously hid
	// transaction identity in the WAL, since txID and LSN values could
	// collide for unrelated entries.
	txIDCounter atomic.Uint64
	appliedLSN  *AppliedLSNTracker
	TxRegistry  *TransactionRegistry
	runtimeMu   sync.RWMutex
	degradedErr error
	testHooks   storageEngineTestHooks
	opMu        sync.RWMutex // Writes use RLock; online backup and DropTable use Lock for exclusivity
	logger      *slog.Logger
	listener    EventListener
	codec       codec.Codec
	counters    engineCounters
	// autoHealAfterApplyFailure mirrors Options.AutoHealAfterApplyFailure:
	// when set, Commit attempts an in-process Heal on post-commit apply
	// failure instead of staying degraded until reopen.
	autoHealAfterApplyFailure bool
	// maxTxWriteSetBytes mirrors Options.MaxTxWriteSetBytes (0 = unlimited).
	maxTxWriteSetBytes int64
	// recoveryLimitLSN bounds WAL replay while RecoverToLSN runs (0 =
	// full replay). Recovery requires exclusive engine access, so a plain
	// field is safe here.
	recoveryLimitLSN uint64
	// Note: per-table lock now lives in Table.mu
}

// NewProductionStorageEngine is the recommended constructor for production use.
//
// Behavior:
//  1. Requires walWriter != nil (without a WAL there is no durability).
//  2. Performs auto-recovery: idempotent WAL replay synchronizing tree+heap
//     with the committed state before returning the engine. Transactions for
//     which Put returned success are visible after a crash.
//  3. Advances lsnTracker to the WAL's max LSN automatically.
//
// Cost: opening the engine in production can take O(N) in the WAL size for
// replay. For large databases, Phase 8 (fuzzy checkpoint) reduces this.
//
// For tests/memory-only (WAL=nil), use NewStorageEngine directly.
func NewProductionStorageEngine(tableMetaData *TableMetaData, walWriter *wal.WALWriter) (*StorageEngine, error) {
	return NewProductionStorageEngineWithOptions(tableMetaData, walWriter, Options{})
}

// NewProductionStorageEngineWithOptions is the recommended constructor for
// production use. It mirrors NewProductionStorageEngine but accepts an
// Options struct to plug in a Logger and an EventListener.
func NewProductionStorageEngineWithOptions(tableMetaData *TableMetaData, walWriter *wal.WALWriter, opts Options) (*StorageEngine, error) {
	if walWriter == nil {
		return nil, fmt.Errorf("storage: NewProductionStorageEngine requires a non-nil walWriter (without WAL there is no durability)")
	}

	se, err := NewStorageEngineWithOptions(tableMetaData, walWriter, opts)
	if err != nil {
		return nil, err
	}

	ctx := opts.RecoveryContext
	if ctx == nil {
		ctx = context.Background()
	}
	// Idempotent replay. If the WAL is empty (initial setup), it is a no-op.
	if err := se.Recover(ctx, walWriter.Path()); err != nil {
		return nil, fmt.Errorf("storage: recovery failed: %w", err)
	}
	return se, nil
}

func NewStorageEngine(tableMetaData *TableMetaData, walWriter *wal.WALWriter) (*StorageEngine, error) {
	return NewStorageEngineWithOptions(tableMetaData, walWriter, Options{})
}

// NewStorageEngineWithOptions builds an engine wired with the supplied
// Options (Logger + EventListener). The zero Options value is valid and
// behaves like NewStorageEngine: logs go to io.Discard and no listener
// callbacks fire.
func NewStorageEngineWithOptions(tableMetaData *TableMetaData, walWriter *wal.WALWriter, opts Options) (*StorageEngine, error) {
	// When opening the engine with an already-populated WAL (reopen), we must
	// advance the lsnTracker to the highest recorded LSN. Without this, new
	// transactions start with SnapshotLSN=0 and do not see persisted records
	// (CreateLSN >= 1) — the record path pretends they "disappeared".
	//
	// We only SCAN the WAL here (cheap, O(entries), no replay).
	// The actual rebuild still happens in Recover().
	initialLSN := uint64(0)
	tailTruncations := uint64(0)
	if walWriter != nil {
		maxLSN, truncations, err := scanMaxWALLSN(walWriter.Path(), walWriter.Cipher())
		if err != nil {
			return nil, fmt.Errorf("storage: failed to synchronize WAL LSN: %w", err)
		}
		initialLSN = maxLSN
		tailTruncations = truncations
	}

	logger := opts.Logger
	if logger == nil {
		logger = discardLogger()
	}

	docCodec := opts.Codec
	if docCodec == nil {
		docCodec = bsoncodec.New()
	}

	se := &StorageEngine{
		TableMetaData:             tableMetaData,
		WAL:                       walWriter,
		lsnTracker:                NewLSNTracker(initialLSN),
		appliedLSN:                NewAppliedLSNTracker(),
		TxRegistry:                NewTransactionRegistry(),
		logger:                    logger,
		listener:                  opts.Listener,
		codec:                     docCodec,
		autoHealAfterApplyFailure: opts.AutoHealAfterApplyFailure,
		maxTxWriteSetBytes:        opts.MaxTxWriteSetBytes,
	}
	if tailTruncations > 0 {
		se.counters.walTailTruncations.Add(tailTruncations)
		logger.Warn("storage: WAL tail truncated on open",
			"path", walWriter.Path(),
			"truncations", tailTruncations,
			"max_lsn", initialLSN,
		)
	}
	se.LockManager = NewLockManager(LockManagerConfig{
		OnDeadlock: func(ev DeadlockEvent) {
			se.counters.deadlocksDetected.Add(1)
			se.logger.Warn("storage: deadlock victim aborted",
				"victim_tx_id", ev.VictimTxID,
				"cycle", ev.Cycle,
			)
			if se.listener.OnDeadlock != nil {
				se.listener.OnDeadlock(ev)
			}
		},
		OnLockWaitTimeout: func(ev LockWaitTimeoutEvent) {
			se.counters.lockWaitTimeouts.Add(1)
			se.logger.Warn("storage: lock wait timeout",
				"tx_id", ev.TxID,
				"resource", ev.Resource,
			)
			if se.listener.OnLockWaitTimeout != nil {
				se.listener.OnLockWaitTimeout(ev)
			}
		},
	})
	se.registerPageRedoHooks()
	if tableMetaData != nil {
		// Re-arm the hooks whenever a table or index is created on the live
		// engine (runtime DDL): registerPageRedoHooks is idempotent and the
		// callback fires outside the metadata locks.
		tableMetaData.setOnTopologyChange(se.registerPageRedoHooks)
	}
	return se, nil
}

func (se *StorageEngine) nextTxID() uint64 {
	return se.txIDCounter.Add(1)
}

// scanMaxWALLSN reads the WAL at `path` looking for the highest LSN. It is
// cheap and independent of Recover (which performs a full replay). A missing
// or empty file → returns 0 without error.
//
// It also returns the number of observed tail-truncations (partial entries
// at the end of the log caused by a crash mid-write). Only errors of type
// io.ErrUnexpectedEOF are tolerated — any other error (CRC mismatch, invalid
// magic, I/O) is treated as real corruption and propagated to the caller,
// which must not keep opening the engine: using an under-estimated maxLSN
// would cause new writes to reuse LSNs already present in the WAL.
func scanMaxWALLSN(path string, cipher crypto.Cipher) (uint64, uint64, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil
		}
		return 0, 0, err
	}

	reader, err := wal.NewWALReaderWithCipher(path, cipher)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = reader.Close() }()

	var (
		maxLSN      uint64
		truncations uint64
		count       int
	)
	for {
		entry, err := reader.ReadEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			if isExpectedWALTail(err) {
				// Crash mid-write — we stop without error, having
				// read as far as possible, and signal the event
				// for the caller to record.
				truncations++
				break
			}
			return 0, 0, fmt.Errorf("storage: scanMaxWALLSN at entry %d: %w", count, err)
		}
		advanceMaxLSN(&maxLSN, entry.Header.LSN)
		wal.ReleaseEntry(entry)
		count++
	}
	return maxLSN, truncations, nil
}

// IsolationLevel defines the transaction's isolation level
type IsolationLevel int

const (
	ReadCommitted  IsolationLevel = iota // Each read takes a new committed snapshot; allows non-repeatable read and phantom.
	RepeatableRead                       // Fixed snapshot per transaction; prevents observational dirty/non-repeatable/phantom read.
)

// Transaction represents an execution context with Snapshot Isolation
type Transaction struct {
	SnapshotLSN uint64
	Level       IsolationLevel
	engine      *StorageEngine

	// iterMu guards iterators. NewIterator registers the handle here so
	// Transaction.Close can force-close anything the caller forgot —
	// otherwise a forgotten Close would leave a B+ tree leaf pinned with
	// its read latch held, blocking writers on that leaf.
	iterMu    sync.Mutex
	iterators []*storageIterator
}

type visibleRecord struct {
	Document  string
	Found     bool
	CreateLSN uint64
}

// BeginTransaction inicia uma transação com o nível de isolamento especificado
func (se *StorageEngine) BeginTransaction(level IsolationLevel) *Transaction {
	se.opMu.RLock()
	snapshot := se.lsnTracker.Current()
	se.opMu.RUnlock()

	tx := &Transaction{
		SnapshotLSN: snapshot, // Captura o "agora" linearizável
		Level:       level,
		engine:      se,
	}
	se.TxRegistry.Register(tx)
	return tx
}

// Close marks the transaction as finished and unregisters it. It also
// force-closes any iterators the caller forgot to close so their pinned
// B+ tree leaves are released back to the buffer pool.
func (tx *Transaction) Close() {
	tx.iterMu.Lock()
	pending := tx.iterators
	tx.iterators = nil
	tx.iterMu.Unlock()
	for _, it := range pending {
		_ = it.Close()
	}
	tx.engine.TxRegistry.Unregister(tx)
}

func (tx *Transaction) trackIterator(it *storageIterator) {
	tx.iterMu.Lock()
	tx.iterators = append(tx.iterators, it)
	tx.iterMu.Unlock()
}

func (tx *Transaction) untrackIterator(it *storageIterator) {
	tx.iterMu.Lock()
	defer tx.iterMu.Unlock()
	for i, x := range tx.iterators {
		if x == it {
			tx.iterators = append(tx.iterators[:i], tx.iterators[i+1:]...)
			return
		}
	}
}

// BeginRead inicia uma transação de read (Snapshot) com o padrão Repeatable Read
func (se *StorageEngine) BeginRead() *Transaction {
	return se.BeginTransaction(RepeatableRead)
}

// IsVisible reports whether a record version is visible to this transaction
func (tx *Transaction) IsVisible(createLSN uint64) bool {
	// Basic rule: I see everything committed BEFORE my snapshot
	return createLSN <= tx.SnapshotLSN
}

func (se *StorageEngine) Close() error {
	var err error
	// TODO: Clean up TxRegistry? Not strictly needed as Engine is closing.

	// Close the page-based runtime trees.
	closedTrees := make(map[btree.Tree]bool)
	for _, tableName := range se.TableMetaData.ListTables() {
		table, _ := se.TableMetaData.GetTableByName(tableName)
		if table == nil {
			continue
		}
		for _, idx := range table.GetIndices() {
			if idx.Tree != nil && !closedTrees[idx.Tree] {
				if tErr := idx.Tree.Close(); tErr != nil {
					if err == nil {
						err = tErr
					} else {
						err = fmt.Errorf("%v; tree close error: %v", err, tErr)
					}
				}
				closedTrees[idx.Tree] = true
			}
		}
	}

	// Close heaps of all tables
	closedHeaps := make(map[heap.Heap]bool)
	for _, tableName := range se.TableMetaData.ListTables() {
		table, _ := se.TableMetaData.GetTableByName(tableName)
		if table != nil && table.Heap != nil && !closedHeaps[table.Heap] {
			if hErr := table.Heap.Close(); hErr != nil {
				if err == nil {
					err = hErr
				} else {
					err = fmt.Errorf("%v; heap close error: %v", err, hErr)
				}
			}
			closedHeaps[table.Heap] = true
		}
	}
	if se.WAL != nil {
		// A clean close just flushed every dirty page (tree/heap Close above),
		// so the on-disk image is a complete consistent cut. Record that with
		// a checkpoint entry: recovery uses it to tell a clean shutdown from a
		// crash after partial eviction flushes (which force index rebuilds).
		// Best-effort — a failure only costs a more conservative reopen.
		// Skipped when a close error means the flush guarantee does not hold,
		// and when the runtime is degraded: a failed post-commit apply left
		// committed data that exists only in the WAL, and a checkpoint here
		// would tell the next recovery to skip exactly that replay.
		if lsn := se.lsnTracker.Current(); err == nil && se.runtimeReadyError() == nil && lsn > 0 && !isPoisonedLSN(lsn) {
			payload := serializeCheckpointPayloadV2(lsn, nil, nil)
			_ = se.WAL.WriteCheckpointRecordPayload(lsn, payload)
		}
		if wErr := se.WAL.Close(); wErr != nil {
			if err == nil {
				err = wErr
			} else {
				err = fmt.Errorf("%v; wal close error: %v", err, wErr)
			}
		}
	}
	return err
}

// DropTable removes a table entirely: its heap, every index tree, and their
// backing files. It takes the engine's exclusive operation lock so no read,
// write, checkpoint, or backup can observe the table mid-removal. WAL entries
// that still reference the dropped table are safe: recovery skips entries
// whose table is no longer registered.
func (se *StorageEngine) DropTable(ctx context.Context, tableName string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	se.opMu.Lock()
	defer se.opMu.Unlock()
	if err := se.runtimeReadyError(); err != nil {
		return err
	}
	return se.TableMetaData.DropTable(tableName)
}

// visibleRecordRaw is the MVCC-resolved payload as stored on the heap, with
// no codec transformation applied. Used by the streaming Iterator and by
// the bytes-returning Get path.
type visibleRecordRaw struct {
	Raw       []byte
	Found     bool
	CreateLSN uint64
}

// readVisibleRecordRaw walks the version chain for `key` starting at
// `currentOffset` and returns the first version visible to `tx` as raw
// bytes. Missing / vacuumed / non-visible chains return Found=false.
//
// ctx is checked at every chain hop because long-lived version chains
// can take noticeable time to walk under heavy MVCC churn.
func (se *StorageEngine) readVisibleRecordRaw(ctx context.Context, tx *Transaction, table *Table, key types.Comparable, currentOffset int64) (visibleRecordRaw, error) {
	for currentOffset != -1 {
		if err := ctx.Err(); err != nil {
			return visibleRecordRaw{}, err
		}
		docBytes, header, err := table.Heap.Read(currentOffset)
		if isChainEndErr(err) {
			return visibleRecordRaw{}, nil
		}
		if err != nil {
			return visibleRecordRaw{}, fmt.Errorf("heap read failed at key %v: %w", key, err)
		}

		if tx.IsVisible(header.CreateLSN) {
			isVisibleVersion := header.Valid || (header.DeleteLSN > tx.SnapshotLSN)
			if !isVisibleVersion {
				return visibleRecordRaw{}, nil
			}
			return visibleRecordRaw{Raw: docBytes, Found: true, CreateLSN: header.CreateLSN}, nil
		}
		currentOffset = header.PrevRecordID
	}
	return visibleRecordRaw{}, nil
}

func (se *StorageEngine) visibleRecordForKey(tx *Transaction, tableName string, indexName string, key types.Comparable) (visibleRecord, error) {
	raw, err := se.visibleRecordForKeyRaw(context.Background(), tx, tableName, indexName, key)
	if err != nil || !raw.Found {
		return visibleRecord{Found: raw.Found, CreateLSN: raw.CreateLSN}, err
	}
	if se.codec != nil {
		if jsonStr, err := se.codec.DecodeToText(raw.Raw); err == nil {
			return visibleRecord{Document: jsonStr, Found: true, CreateLSN: raw.CreateLSN}, nil
		}
	}
	return visibleRecord{Document: string(raw.Raw), Found: true, CreateLSN: raw.CreateLSN}, nil
}

// visibleRecordForKeyRaw resolves the MVCC version for `key` and returns
// the raw heap bytes. Used by the bytes-returning Get path.
func (se *StorageEngine) visibleRecordForKeyRaw(ctx context.Context, tx *Transaction, tableName string, indexName string, key types.Comparable) (visibleRecordRaw, error) {
	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return visibleRecordRaw{}, err
	}
	index, err := table.GetIndex(indexName)
	if err != nil {
		return visibleRecordRaw{}, err
	}
	if !index.Primary {
		treeV2, ok := index.Tree.(*btreev2.BTreeV2)
		if !ok {
			return visibleRecordRaw{}, fmt.Errorf("storage: secondary lookup: index %s uses unsupported tree type %T", indexName, index.Tree)
		}
		cur, err := treeV2.NewCursor(secondaryLowerBound(key), secondaryUpperBound(key))
		if err != nil {
			return visibleRecordRaw{}, err
		}
		defer func() { _ = cur.Close() }()
		for cur.Next() {
			rec, err := se.readVisibleRecordRaw(ctx, tx, table, key, cur.Value())
			if err != nil {
				return visibleRecordRaw{}, err
			}
			if rec.Found {
				return rec, nil
			}
		}
		return visibleRecordRaw{}, cur.Err()
	}
	currentOffset, found, err := index.Tree.Get(key)
	if err != nil {
		return visibleRecordRaw{}, fmt.Errorf("tree get: %w", err)
	}
	if !found {
		return visibleRecordRaw{}, nil
	}
	return se.readVisibleRecordRaw(ctx, tx, table, key, currentOffset)
}

// Put writes (or replaces) `document` under `key` in `tableName.indexName`.
//
// Cancellation: ctx is honored up to and including the moment the WAL
// entry has been queued (see WriteEntry). Once WriteEntry returns, the
// heap and tree mutations that follow are part of the durability path
// and are NOT cancelled by ctx — the durable write either lands fully
// or the engine enters its degraded state.
func (se *StorageEngine) Put(ctx context.Context, tableName string, indexName string, key types.Comparable, document string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	se.opMu.RLock()
	defer se.opMu.RUnlock()
	if err := se.runtimeReadyError(); err != nil {
		return err
	}

	// Get the table first (no lock)
	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return err
	}

	// We don't need to traverse the whole table (Table RLock removed in favor of granular concurrency)
	// se.TableMetaData already protects access to the table map.

	// Get the index (we already hold the table lock)
	index, err := table.GetIndex(indexName)
	if err != nil {
		return err
	}

	// Try parse the document with the configured codec for validation and
	// canonical on-disk encoding. If the document contains every indexed
	// field, use the multi-index write path so updates keep secondary
	// indexes consistent.
	parsedDoc, parseErr := se.codec.Parse(document)
	var encodedDoc []byte
	if parseErr == nil {
		extracted, exists, kerr := parsedDoc.Key(indexName)
		if kerr != nil {
			return kerr
		}
		if !exists {
			return &errors.IndexNotFoundError{Name: indexName}
		}

		keyType := getTypeFromKey(extracted)
		if keyType != index.Type {
			return &errors.InvalidKeyTypeError{
				Name:     indexName,
				TypeName: keyType.String(),
			}
		}

		encodedDoc, _ = parsedDoc.Bytes()

		if keys, ok, err := keysFromCodecDocForAllIndexes(table, parsedDoc); err != nil {
			return err
		} else if ok {
			docKey := keys[indexName]
			if !sameComparableKey(docKey, key) {
				return fmt.Errorf("storage: key informada %v diverge do campo indexado %s=%v", key, indexName, docKey)
			}
			return se.writeRowLocked(ctx, tableName, document, keys, false)
		}
	} else {
		// Fallback to raw bytes
		encodedDoc = []byte(document)
	}

	resource, err := lockResourceForKey(tableName, indexName, key)
	if err != nil {
		return err
	}

	return se.withAutoCommitLocks(ctx, []string{resource}, func() error {
		// LSN Management
		// We generate the LSN *before* writing to the WAL or Heap to guarantee ordering
		currentLSN := se.lsnTracker.Next()
		physicalKey := singleIndexPhysicalKey(index, key)

		// 1. Write Ahead Log
		if se.WAL != nil {
			payload, err := SerializeDocumentEntry(tableName, indexName, key, encodedDoc)
			if err != nil {
				return err
			}

			entry := wal.AcquireEntry()
			entry.Header.Magic = wal.WALMagic
			entry.Header.Version = 1
			entry.Header.EntryType = wal.EntryInsert // We treat Update as Insert in the log-structured WAL

			entry.Header.LSN = currentLSN

			entry.Header.PayloadLen = uint32(len(payload)) //nolint:gosec // payload size bounded by page/record limits
			entry.Header.CRC32 = wal.CalculateCRC32(payload)
			entry.Payload = append(entry.Payload, payload...)

			if err := se.WAL.WriteEntry(entry); err != nil {
				wal.ReleaseEntry(entry)
				return fmt.Errorf("wal write failed: %w", err)
			}
			wal.ReleaseEntry(entry)
		}

		// 2 ~ 4. Atomic Upsert (Write Heap -> Update Tree)
		// We use Upsert to guarantee atomicity when accessing the previous version and updating the HEAD pointer.
		table.Lock()
		defer table.Unlock()
		upsert := func(oldOffset int64, exists bool) (int64, error) {
			var prevOffset int64 = -1
			if exists {
				prevOffset = oldOffset
			}

			// Write to Heap (inside the leaf Lock - safe but increases lock latency)
			// TODO: future optimization - if heap write is slow, refactor.
			// But since it is append-only bufio, it should be fast.
			offset, err := table.Heap.Write(encodedDoc, currentLSN, prevOffset)
			if err != nil {
				return 0, fmt.Errorf("heap write failed: %w", err)
			}

			return offset, nil
		}

		if treeV2, ok := index.Tree.(*btreev2.BTreeV2); ok {
			err = treeV2.UpsertWithLSN(physicalKey, currentLSN, upsert)
		} else {
			err = index.Tree.Upsert(physicalKey, upsert)
		}

		if err != nil {
			return err
		}

		se.appliedLSN.MarkApplied(tableName, indexName, currentLSN)

		return nil
	})
}

// GetBytes returns the raw heap bytes of the visible version of `key`,
// bypassing the codec text round-trip. Prefer this over Get for any
// consumer that does its own decoding (BSON, Protobuf, MessagePack,
// native structs, etc.).
// GetBytes returns the raw heap bytes of the visible version of `key`,
// honoring this transaction's snapshot. ctx is honored at entry only —
// MVCC chain traversal that follows is bounded by the version chain
// length on disk.
func (tx *Transaction) GetBytes(ctx context.Context, tableName string, indexName string, key types.Comparable) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	se := tx.engine
	se.opMu.RLock()
	defer se.opMu.RUnlock()
	if err := se.runtimeReadyError(); err != nil {
		return nil, false, err
	}

	tx.refreshSnapshot()

	rec, err := se.visibleRecordForKeyRaw(ctx, tx, tableName, indexName, key)
	if err != nil {
		return nil, false, err
	}
	if !rec.Found {
		return nil, false, nil
	}
	return rec.Raw, true, nil
}

// GetBytes is the auto-commit (snapshot-on-call) variant of
// Transaction.GetBytes.
func (se *StorageEngine) GetBytes(ctx context.Context, tableName string, indexName string, key types.Comparable) ([]byte, bool, error) {
	tx := se.BeginRead()
	defer tx.Close()
	return tx.GetBytes(ctx, tableName, indexName, key)
}

// InsertRow inserts a new row and updates all of the table's indexes.
// Duplicate primary keys fail while the table's exclusive lock is held,
// closing the check-then-write race.
//
// Cancellation: same contract as Put — ctx is honored up to WAL queue.
func (se *StorageEngine) InsertRow(ctx context.Context, tableName string, doc string, keys map[string]types.Comparable) error {
	return se.writeRow(ctx, tableName, doc, keys, true)
}

// UpsertRow inserts or updates an entire row keeping all indexes in sync.
// When the primary key already exists, the previous version is tombstoned in
// the heap; old secondary index entries then point to a version not visible
// to new snapshots.
//
// Cancellation: same contract as Put.
func (se *StorageEngine) UpsertRow(ctx context.Context, tableName string, doc string, keys map[string]types.Comparable) error {
	return se.writeRow(ctx, tableName, doc, keys, false)
}

// Del removes the visible version under `key`.
//
// Cancellation: same contract as Put — ctx is honored up to WAL queue.
func (se *StorageEngine) Del(ctx context.Context, tableName string, indexName string, key types.Comparable) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	se.opMu.RLock()
	defer se.opMu.RUnlock()
	if err := se.runtimeReadyError(); err != nil {
		return false, err
	}

	// Get the table first (no lock)
	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return false, err
	}

	// No Table Lock. Upsert handles it.

	// Get the index (we already hold the table lock)
	index, err := table.GetIndex(indexName)
	if err != nil {
		return false, err
	}

	resource, err := lockResourceForKey(tableName, indexName, key)
	if err != nil {
		return false, err
	}

	var wasFound bool
	err = se.withAutoCommitLocks(ctx, []string{resource}, func() error {
		// LSN Management
		currentLSN := se.lsnTracker.Next()
		physicalKey := singleIndexPhysicalKey(index, key)

		// 1. Write Ahead Log
		if se.WAL != nil {
			// For delete, we only need the key. Empty document.
			payload, err := SerializeDocumentEntry(tableName, indexName, key, nil)
			if err != nil {
				return err
			}

			entry := wal.AcquireEntry()
			entry.Header.Magic = wal.WALMagic
			entry.Header.Version = 1
			entry.Header.EntryType = wal.EntryDelete

			entry.Header.LSN = currentLSN

			entry.Header.PayloadLen = uint32(len(payload)) //nolint:gosec // payload size bounded by page/record limits
			entry.Header.CRC32 = wal.CalculateCRC32(payload)
			entry.Payload = append(entry.Payload, payload...)

			if err := se.WAL.WriteEntry(entry); err != nil {
				wal.ReleaseEntry(entry)
				return fmt.Errorf("wal write failed: %w", err)
			}
			wal.ReleaseEntry(entry)
		}

		// 2. Modify Memory and Heap
		// Use Upsert to logically remove (or keep pointing to the Tombstone)
		// We need to write the Tombstone in the Heap and update the tree to point to it.
		// The current Delete only marks in the Heap, and does NOT remove from the tree (per the commented-out notes below).
		// But we need to update the pointer in the tree to the new record in the Heap (which says "Deleted").
		upsert := func(oldOffset int64, exists bool) (int64, error) {
			if !exists {
				return 0, nil // Key not found, nothing to delete
			}
			// Write a Delete record in the Heap (Tombstone)
			// Does Heap delete require the old offset? The current Heap.Delete method takes the offset.
			// Wait, does Heap.Delete(offset) mark the OLD record as deleted?
			// Original engine.go:
			// offset := node.DataPtrs[idx]
			// se.Heap.Delete(offset, currentLSN) -> Modifies the old record header in-place?
			// If Heap.Delete modifies in-place, then we don't create a new version?
			// That violates WAL/AppendOnly immutability.
			// The comment said: "For simplified Phase 2: Update Head in-place with DeleteLSN."
			// If it is in-place, we don't need to update the tree (it points to the same offset).
			// HOWEVER, for correct concurrency, we must lock the node while reading the offset and calling heap.Delete.

			if err := table.Heap.Delete(oldOffset, currentLSN); err != nil {
				if isChainEndErr(err) {
					return oldOffset, nil
				}
				return 0, fmt.Errorf("heap delete failed: %w", err)
			}
			wasFound = true

			// Return the SAME offset, since the tree doesn't change (it points to the same place, now marked deleted)
			return oldOffset, nil
		}

		if treeV2, ok := index.Tree.(*btreev2.BTreeV2); ok {
			err = treeV2.UpsertWithLSN(physicalKey, currentLSN, upsert)
		} else {
			err = index.Tree.Upsert(physicalKey, upsert)
		}

		if err != nil {
			return err
		}

		// MVCC Phase 2: Do NOT remove from B-Tree.
		// We need to keep the key pointing to the "Deleted" record (Tombstone)
		// so that older transactions can check visibility (DeleteLSN) and potential previous versions.
		// Garbage Collection (Vacuum) will eventually remove these when safe.
		// removed := index.Tree.Root.Remove(key)
		// if index.Tree.Root.N == 0 && !index.Tree.Root.Leaf {
		// 	index.Tree.Root = index.Tree.Root.Children[0]
		// }

		if wasFound {
			se.appliedLSN.MarkApplied(tableName, indexName, currentLSN)
		}

		return nil
	})
	if err != nil {
		return false, err
	}

	return wasFound, nil
}

// CreateCheckpoint flushes the durable page-based state.
//
// Cancellation: ctx is honored up to the WAL.Sync(). Once Sync returns
// successfully, the on-disk page flushes that follow are NOT cancelled —
// aborting mid-checkpoint would leave the engine with a partially
// synced view.
func (se *StorageEngine) CreateCheckpoint(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	se.opMu.RLock()
	defer se.opMu.RUnlock()
	if err := se.runtimeReadyError(); err != nil {
		return err
	}

	if se.WAL != nil {
		if err := se.WAL.Sync(); err != nil {
			return err
		}
	}

	syncedTrees := make(map[btree.Tree]bool)
	syncedHeaps := make(map[heap.Heap]bool)

	for _, tableName := range se.TableMetaData.ListTables() {
		table, err := se.TableMetaData.GetTableByName(tableName)
		if err != nil {
			continue
		}

		for _, idx := range table.GetIndices() {
			if idx.Tree != nil && !syncedTrees[idx.Tree] {
				if treeV2, ok := idx.Tree.(*btreev2.BTreeV2); ok {
					if err := treeV2.Sync(); err != nil {
						return err
					}
				}
				syncedTrees[idx.Tree] = true
			}
		}

		if table.Heap != nil && !syncedHeaps[table.Heap] {
			if heapV2, ok := table.Heap.(*v2.HeapV2); ok {
				if err := heapV2.Sync(); err != nil {
					return err
				}
			}
			syncedHeaps[table.Heap] = true
		}
	}
	return nil
}

// Helper to refresh snapshot for ReadCommitted
func (tx *Transaction) refreshSnapshot() {
	if tx.Level == ReadCommitted {
		tx.SnapshotLSN = tx.engine.lsnTracker.Current()
	}
}

// Recover rebuilds engine state from the WAL.
//
// MUST be called before any concurrent operations; recovery assumes
// exclusive access to the engine.
//
// Cancellation: ctx is checked periodically while replaying the WAL
// (every 256 entries during physical and logical redo). Cancelling
// returns early with ctx.Err(); the engine is left in its degraded
// state and must not be used until a successful Recover happens.
func (se *StorageEngine) Recover(ctx context.Context, walPath string) error {
	return se.RecoverWithCipher(ctx, walPath, se.walCipher())
}

// RecoverToLSN is point-in-time recovery: it replays `walPath` exactly as
// Recover does, but treats the log as ending right before the first entry
// whose LSN exceeds targetLSN. Transactions whose COMMIT lies beyond the
// target are discarded (they become losers, as in crash recovery), so the
// engine lands on the committed state as of targetLSN.
//
// Intended flow: restore a backup, wire the engine over the restored data
// files with a FRESH WAL (so new commits cannot collide with LSNs from the
// discarded tail), then call RecoverToLSN against the original/archived
// WAL. The WAL is decrypted with the engine's current cipher — configure
// the fresh writer with the same DEK that encrypted the source log.
func (se *StorageEngine) RecoverToLSN(ctx context.Context, walPath string, targetLSN uint64) error {
	if targetLSN == 0 {
		return fmt.Errorf("storage: RecoverToLSN requires a non-zero target LSN")
	}
	se.recoveryLimitLSN = targetLSN
	defer func() { se.recoveryLimitLSN = 0 }()
	return se.RecoverWithCipher(ctx, walPath, se.walCipher())
}

// openRecoveryReader opens the WAL for a recovery phase, applying the
// point-in-time bound when RecoverToLSN is driving the replay.
func (se *StorageEngine) openRecoveryReader(walPath string, cipher crypto.Cipher) (*wal.WALReader, error) {
	reader, err := wal.NewWALReaderWithCipher(walPath, cipher)
	if err != nil {
		return nil, err
	}
	if se.recoveryLimitLSN > 0 {
		reader.SetLimitLSN(se.recoveryLimitLSN)
	}
	return reader, nil
}

// RecoverWithCipher is Recover with an explicit cipher. Use this only
// when the engine's WALWriter is not yet available.
func (se *StorageEngine) RecoverWithCipher(ctx context.Context, walPath string, cipher crypto.Cipher) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	recoveryStart := time.Now()
	var maxLSN uint64
	loadedLSNs := make(map[string]uint64)
	pageRedoTargets := se.pageRedoTargets()

	analysis, err := se.analyzeRecoveryWithCipher(walPath, cipher)
	if err != nil {
		return err
	}
	if se.recoveryLimitLSN > 0 {
		// Point-in-time recovery replays a source log over RESTORED files: a
		// checkpoint in that log certifies what was on the source's disk, not
		// on the restore target, so trusting it would skip replay the target
		// never received. Full replay is idempotent; only the cost grows.
		analysis.CheckpointLSN = 0
		analysis.DPT = nil
	}
	if analysis.MaxLSN > maxLSN {
		maxLSN = analysis.MaxLSN
	}

	// 1. Redo scan-only: re-reads the entire WAL, but reapplies only
	// autocommit operations or those belonging to committed transactions.
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		se.lsnTracker.Set(maxLSN)
		return nil
	}

	physicalApplied, physicalSkipped, physMaxLSN, err := se.runPhysicalRedo(ctx, walPath, cipher, analysis, pageRedoTargets)
	if err != nil {
		return err
	}
	if physMaxLSN > maxLSN {
		maxLSN = physMaxLSN
	}

	// Trees of tables that took eviction flushes after the last checkpoint
	// are a mixed-vintage cut on disk and cannot be replayed through; rebuild
	// them from the heap before logical redo probes them.
	rebuiltPaths, indexesRebuilt, err := se.rebuildEvictedIndexes(ctx, analysis)
	if err != nil {
		return err
	}

	count, skipped, logMaxLSN, err := se.runLogicalRedo(ctx, walPath, cipher, analysis, loadedLSNs)
	if err != nil {
		return err
	}
	if logMaxLSN > maxLSN {
		maxLSN = logMaxLSN
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	// 2a. Undo-lite: loser txs never reached the visible state because the
	// write path only applies heap/tree after a durable COMMIT.
	loserTxsUndone := len(analysis.LoserTxs)
	clrsApplied, err := se.undoLoserTransactionsWithLimit(walPath, cipher, analysis, 0)
	if err != nil {
		return err
	}
	// 2b. Roll back nested top actions that started but never
	// committed. Their captured before-images get written back to
	// the on-disk pages so a half-applied split / merge is reverted.
	// Rebuilt trees are excluded: their before-images describe pages of
	// the replaced file and would corrupt the fresh one.
	partialNTAsRolledBack, err := se.rollbackPartialNTAs(analysis, rebuiltPaths)
	if err != nil {
		return err
	}

	se.lsnTracker.Set(maxLSN)
	// txIDCounter is intentionally NOT restored from the WAL: txIDs are
	// process-local (reset on restart by design, like Postgres XIDs).
	// Persisting/restoring them would require a checkpoint format
	// change and is deliberately deferred.
	se.clearDegraded()
	se.fireRecoveryComplete(RecoveryEvent{
		PhysicalApplied:       physicalApplied,
		PhysicalSkipped:       physicalSkipped,
		LogicalApplied:        count,
		LogicalSkipped:        skipped,
		CLRsApplied:           clrsApplied,
		LoserTxsUndone:        loserTxsUndone,
		PartialNTAsRolledBack: partialNTAsRolledBack,
		IndexesRebuilt:        indexesRebuilt,
		CheckpointLSN:         analysis.CheckpointLSN,
		MaxLSN:                maxLSN,
		Duration:              time.Since(recoveryStart),
	})
	return nil
}

func (se *StorageEngine) walCipher() crypto.Cipher {
	if se == nil || se.WAL == nil {
		return nil
	}
	return se.WAL.Cipher()
}

// runPhysicalRedo replays page-image WAL entries up to / starting from
// the checkpoint LSN. ctx is checked every 256 entries.
func (se *StorageEngine) runPhysicalRedo(ctx context.Context, walPath string, cipher crypto.Cipher, analysis *recoveryAnalysis, pageRedoTargets map[string]pageRedoTarget) (int, int, uint64, error) {
	reader, err := se.openRecoveryReader(walPath, cipher)
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() { _ = reader.Close() }()

	applied := 0
	skipped := 0
	var maxLSN uint64

	for {
		if (applied+skipped)&0xFF == 0 {
			if err := ctx.Err(); err != nil {
				return applied, skipped, maxLSN, err
			}
		}
		entry, err := reader.ReadEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			if isExpectedWALTail(err) {
				break
			}
			return applied, skipped, maxLSN, fmt.Errorf("physical redo error at entry %d: %w", applied+skipped, err)
		}

		advanceMaxLSN(&maxLSN, entry.Header.LSN)
		// ARIES: physical redo can start from min(DPT.recLSN) instead
		// of CheckpointLSN. Pages absent from the DPT at checkpoint
		// time were flushed; entries before minRec for those pages
		// have been durably applied.
		startLSN := analysis.CheckpointLSN
		if rec := minRecLSN(analysis.DPT); rec > 0 && rec < startLSN {
			startLSN = rec
		}
		if startLSN > 0 && entry.Header.LSN < startLSN {
			skipped++
			wal.ReleaseEntry(entry)
			continue
		}
		if entry.Header.EntryType != wal.EntryPageRedo {
			skipped++
			wal.ReleaseEntry(entry)
			continue
		}
		ok, err := se.redoPageEntry(entry, pageRedoTargets)
		wal.ReleaseEntry(entry)
		if err != nil {
			return applied, skipped, maxLSN, fmt.Errorf("physical redo apply failed at entry %d: %w", applied+skipped, err)
		}
		if ok {
			applied++
		} else {
			skipped++
		}
	}
	return applied, skipped, maxLSN, nil
}

// runLogicalRedo replays the logical (document / multi-index / CLR) WAL
// entries. ctx is checked every 256 entries.
func (se *StorageEngine) runLogicalRedo(ctx context.Context, walPath string, cipher crypto.Cipher, analysis *recoveryAnalysis, loadedLSNs map[string]uint64) (int, int, uint64, error) {
	reader, err := se.openRecoveryReader(walPath, cipher)
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() { _ = reader.Close() }()

	count := 0
	skipped := 0
	var maxLSN uint64

	for {
		if count&0xFF == 0 {
			if err := ctx.Err(); err != nil {
				return count, skipped, maxLSN, err
			}
		}
		entry, err := reader.ReadEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			if isExpectedWALTail(err) {
				break
			}
			return count, skipped, maxLSN, fmt.Errorf("recovery error at entry %d: %w", count, err)
		}

		advanceMaxLSN(&maxLSN, entry.Header.LSN)

		payload, shouldRedo, err := analysis.shouldRedo(entry)
		if err != nil {
			wal.ReleaseEntry(entry)
			return count, skipped, maxLSN, fmt.Errorf("redo classification failed at entry %d: %w", count, err)
		}
		if !shouldRedo {
			skipped++
			wal.ReleaseEntry(entry)
			count++
			continue
		}

		if err := se.redoLogicalEntry(entry, payload, loadedLSNs); err != nil {
			var unknownErr *unknownEntryTypeError
			if goerrors.As(err, &unknownErr) {
				skipped++
				wal.ReleaseEntry(entry)
				count++
				continue
			}
			wal.ReleaseEntry(entry)
			return count, skipped, maxLSN, fmt.Errorf("redo failed at entry %d: %w", count, err)
		}
		wal.ReleaseEntry(entry)
		count++
	}
	return count, skipped, maxLSN, nil
}

type unknownEntryTypeError struct{ typ uint8 }

func (e *unknownEntryTypeError) Error() string {
	return fmt.Sprintf("storage: recovery skipped unknown entry type %d", e.typ)
}

func (se *StorageEngine) redoLogicalEntry(entry *wal.WALEntry, payload []byte, loadedLSNs map[string]uint64) error {
	switch entry.Header.EntryType {
	case wal.EntryInsert, wal.EntryUpdate, wal.EntryDelete:
		return se.redoDocumentEntry(entry, payload, loadedLSNs)
	case wal.EntryMultiInsert:
		return se.redoMultiInsertEntry(entry, payload, loadedLSNs)
	case wal.EntryCLR:
		return se.redoCompensationEntry(entry, payload)
	default:
		return &unknownEntryTypeError{typ: entry.Header.EntryType}
	}
}

// Vacuum performs garbage collection on `tableName`, removing dead
// tombstones (deleted records visible to no active transaction) and
// compacting the heap.
//
// Cancellation: ctx is honored at entry and inside the heap's per-page
// loop. Cancellation between pages returns ctx.Err() without leaving
// the on-disk state in an inconsistent shape (vacuum is page-local).
func (se *StorageEngine) Vacuum(ctx context.Context, tableName string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	se.opMu.RLock()
	defer se.opMu.RUnlock()
	if err := se.runtimeReadyError(); err != nil {
		return err
	}

	// 1. Acquire Table Lock (Exclusive)
	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return err
	}
	table.Lock()
	defer table.Unlock()

	// 2. Determine Minimum Visible LSN
	// Any Tombstone with DeleteLSN < minLSN is safe to remove.
	//
	// With no active transactions the registry reports MaxUint64 ("everything
	// is reclaimable"). That sentinel must never leave this function: the heap
	// stamps the horizon into compacted pages' PageLSN, from where it would
	// flow into WAL page-redo entries and, on the next open, into the LSN
	// counter — whose first increment then wraps to 0 and hides every
	// committed row. Clamping to the current LSN keeps the same reclaim
	// semantics (every committed delete is below it) while staying a real LSN.
	minLSN := se.TxRegistry.GetMinActiveLSN()
	if current := se.lsnTracker.Current(); minLSN > current {
		minLSN = current
	}

	se.logger.Info("storage: vacuum start", "table", tableName, "min_lsn", minLSN)

	// 3. Dispatch to the current implementation: in-place compaction,
	// without rewriting the B+ tree. Vacuumed slots become length=0;
	// reads hit ErrVacuumed (treated as end of chain in engine.Get).
	if heapV2, ok := table.Heap.(*v2.HeapV2); ok {
		n, err := heapV2.Vacuum(ctx, minLSN)
		if err != nil {
			return fmt.Errorf("Vacuum v2 failed for table %s: %w", tableName, err)
		}
		se.fireVacuumComplete(VacuumEvent{Table: tableName, MinLSN: minLSN, Reclaimed: n})
		return nil
	}

	return fmt.Errorf("Vacuum: legacy heap removed; table %s must use HeapV2", tableName)
}
