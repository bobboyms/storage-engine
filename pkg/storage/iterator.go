package storage

import (
	"fmt"

	btreev2 "github.com/bobboyms/storage-engine/pkg/btree/v2"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// IterOptions controls the scope of a streaming iteration.
//
// Lower / Upper are inclusive. Pass nil to leave a bound unbounded. The
// underlying B+ tree cursor walks the leaves in key order, holding the
// read latch on a single leaf at a time.
type IterOptions struct {
	Lower   types.Comparable
	Upper   types.Comparable
	Reverse bool
}

// Iterator is the streaming read API. It is created from a Transaction
// (see Transaction.NewIterator) or from the engine for auto-commit reads
// (StorageEngine.NewIterator). Visibility follows the originating
// transaction's snapshot.
//
// Lifetime contract:
//
//   - Call Close exactly once when done. Forgetting Close is tolerated —
//     the owning Transaction force-closes anything still open when it is
//     closed — but it leaves a B+ tree leaf latch held until then.
//   - Key, Value, LSN return values are valid only between a successful
//     Next call and the next call to Next or Close.
//   - Value returns raw heap bytes (e.g. BSON when the default codec is
//     in use). The caller decides how to decode them.
type Iterator interface {
	Next() bool
	Key() types.Comparable
	Value() []byte
	LSN() uint64
	Err() error
	Close() error
}

// storageIterator wraps a btreev2.Cursor with MVCC visibility resolution
// against the originating transaction's snapshot.
type storageIterator struct {
	tx     *Transaction
	table  *Table
	cursor *btreev2.Cursor

	curKey   types.Comparable
	curValue []byte
	curLSN   uint64

	err    error
	closed bool
}

// NewIterator opens a streaming iterator over `indexName` of `tableName`,
// scoped to this transaction's snapshot.
//
// For ReadCommitted, the iterator captures the snapshot at the moment it
// is created and keeps that view for its whole lifetime — otherwise a
// long-running scan could observe rows from multiple commit epochs.
func (tx *Transaction) NewIterator(tableName, indexName string, opts IterOptions) (Iterator, error) {
	se := tx.engine
	se.opMu.RLock()
	if err := se.runtimeReadyError(); err != nil {
		se.opMu.RUnlock()
		return nil, err
	}
	se.opMu.RUnlock()

	if opts.Reverse {
		return nil, fmt.Errorf("storage: reverse iteration not yet supported")
	}

	tx.refreshSnapshot()

	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return nil, err
	}
	index, err := table.GetIndex(indexName)
	if err != nil {
		return nil, err
	}
	treeV2, ok := index.Tree.(*btreev2.BTreeV2)
	if !ok {
		return nil, fmt.Errorf("storage: iterator: index %s uses unsupported tree type %T", indexName, index.Tree)
	}

	cur, err := treeV2.NewCursor(opts.Lower, opts.Upper)
	if err != nil {
		return nil, err
	}

	it := &storageIterator{
		tx:     tx,
		table:  table,
		cursor: cur,
	}
	tx.trackIterator(it)
	return it, nil
}

// NewIterator on the engine is a wrapper that opens a snapshot read
// transaction whose lifetime is tied to the returned iterator. Close on
// the iterator closes the transaction too.
func (se *StorageEngine) NewIterator(tableName, indexName string, opts IterOptions) (Iterator, error) {
	tx := se.BeginRead()
	it, err := tx.NewIterator(tableName, indexName, opts)
	if err != nil {
		tx.Close()
		return nil, err
	}
	return &autoCommitIterator{inner: it, tx: tx}, nil
}

func (it *storageIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}
	for it.cursor.Next() {
		offset := it.cursor.Value()
		rec, err := it.tx.engine.readVisibleRecordRaw(it.tx, it.table, it.cursor.Key(), offset)
		if err != nil {
			it.err = err
			return false
		}
		if !rec.Found {
			continue
		}
		it.curKey = it.cursor.Key()
		it.curValue = rec.Raw
		it.curLSN = rec.CreateLSN
		return true
	}
	if err := it.cursor.Err(); err != nil {
		it.err = err
	}
	return false
}

func (it *storageIterator) Key() types.Comparable { return it.curKey }
func (it *storageIterator) Value() []byte         { return it.curValue }
func (it *storageIterator) LSN() uint64           { return it.curLSN }
func (it *storageIterator) Err() error            { return it.err }

func (it *storageIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	cerr := it.cursor.Close()
	it.tx.untrackIterator(it)
	return cerr
}

// autoCommitIterator ties an Iterator to a transaction that the engine
// opened on the caller's behalf (see StorageEngine.NewIterator). Closing
// the iterator also closes the transaction.
type autoCommitIterator struct {
	inner Iterator
	tx    *Transaction
}

func (a *autoCommitIterator) Next() bool            { return a.inner.Next() }
func (a *autoCommitIterator) Key() types.Comparable { return a.inner.Key() }
func (a *autoCommitIterator) Value() []byte         { return a.inner.Value() }
func (a *autoCommitIterator) LSN() uint64           { return a.inner.LSN() }
func (a *autoCommitIterator) Err() error            { return a.inner.Err() }
func (a *autoCommitIterator) Close() error {
	err := a.inner.Close()
	a.tx.Close()
	return err
}
