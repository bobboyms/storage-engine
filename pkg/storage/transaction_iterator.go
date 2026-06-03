package storage

import (
	"context"
	"fmt"
	"sort"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// overlayEntry is the transaction-local effect on a single logical key for the
// index being scanned: either a buffered upsert (raw document bytes) or a
// buffered delete.
type overlayEntry struct {
	key     types.Comparable
	raw     []byte
	deleted bool
}

// NewIterator returns an iterator over indexName that reflects this
// transaction's buffered writes on top of its committed snapshot
// (read-your-writes). Buffered inserts/upserts appear with their staged
// document, buffered deletes are excluded, and results stay ordered by the
// index's logical key. Reverse iteration is not supported. The read-your-writes
// guarantee is exact for the primary index; for a secondary index a buffered
// row appears under its current secondary key, but a committed entry whose
// secondary key was moved by an in-transaction upsert may still surface (the
// same staleness the engine resolves through heap visibility at commit).
func (tx *WriteTransaction) NewIterator(ctx context.Context, tableName, indexName string, opts IterOptions) (Iterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.Reverse {
		return nil, fmt.Errorf("storage: reverse iteration not yet supported")
	}

	tx.mu.Lock()
	if err := tx.ensureWritableLocked(); err != nil {
		tx.mu.Unlock()
		return nil, err
	}
	overlayByKey, ordered, err := tx.buildOverlayLocked(indexName, opts.Lower, opts.Upper)
	tx.mu.Unlock()
	if err != nil {
		return nil, err
	}

	committed, err := tx.readView.NewIterator(ctx, tableName, indexName, opts)
	if err != nil {
		return nil, err
	}

	return &txIterator{
		committed:  committed,
		overlay:    ordered,
		overlaySet: overlayByKey,
	}, nil
}

// buildOverlayLocked snapshots the write buffer into the per-key effect map for
// indexName, plus a sorted slice of the upsert effects within [lower, upper].
func (tx *WriteTransaction) buildOverlayLocked(indexName string, lower, upper types.Comparable) (map[string]overlayEntry, []overlayEntry, error) {
	byKey := make(map[string]overlayEntry)
	for i := range tx.writeSet {
		op := &tx.writeSet[i]
		if op.tableName == "" {
			continue
		}
		key, raw, deleted, ok := tx.overlayEffectForIndex(op, indexName)
		if !ok {
			continue
		}
		byKey[overlayKeyString(key)] = overlayEntry{key: key, raw: raw, deleted: deleted}
	}

	ordered := make([]overlayEntry, 0, len(byKey))
	for _, e := range byKey {
		if e.deleted {
			continue
		}
		in, err := keyInRange(e.key, lower, upper)
		if err != nil {
			return nil, nil, err
		}
		if in {
			ordered = append(ordered, e)
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		cmp, _ := ordered[i].key.Compare(ordered[j].key)
		return cmp < 0
	})
	return byKey, ordered, nil
}

// overlayEffectForIndex extracts the effect a buffered op has on indexName, if
// any: the logical key for that index, the document bytes, and whether it is a
// delete.
func (tx *WriteTransaction) overlayEffectForIndex(op *writeOp, indexName string) (key types.Comparable, raw []byte, deleted bool, ok bool) {
	switch op.opType {
	case wal.EntryMultiInsert:
		k, present := op.keys[indexName]
		if !present {
			return nil, nil, false, false
		}
		return k, encodeDocumentOrRaw(tx.engine.codec, op.document), false, true
	case wal.EntryDelete:
		if op.indexName != indexName {
			return nil, nil, false, false
		}
		return op.key, nil, true, true
	default: // single-index put
		if op.indexName != indexName {
			return nil, nil, false, false
		}
		return op.key, encodeDocumentOrRaw(tx.engine.codec, op.document), false, true
	}
}

func overlayKeyString(key types.Comparable) string {
	return fmt.Sprintf("%T:%v", key, key)
}

// keyInRange reports whether key is within the inclusive [lower, upper] bounds.
// A nil bound is unbounded on that side.
func keyInRange(key, lower, upper types.Comparable) (bool, error) {
	if lower != nil {
		cmp, err := key.Compare(lower)
		if err != nil {
			return false, err
		}
		if cmp < 0 {
			return false, nil
		}
	}
	if upper != nil {
		cmp, err := key.Compare(upper)
		if err != nil {
			return false, err
		}
		if cmp > 0 {
			return false, nil
		}
	}
	return true, nil
}

// txIterator merges an ordered committed iterator with the transaction's
// sorted overlay of buffered upserts, applying buffered deletes and letting
// buffered writes shadow committed rows with the same key.
type txIterator struct {
	committed  Iterator
	overlay    []overlayEntry
	overlaySet map[string]overlayEntry
	oi         int

	cValid    bool // whether a committed row is currently buffered in cKey/cVal
	cKey      types.Comparable
	cVal      []byte
	cLSN      uint64
	cFinished bool

	curKey types.Comparable
	curVal []byte
	curLSN uint64
	err    error
}

func (it *txIterator) advanceCommitted() {
	for !it.cFinished {
		if !it.committed.Next() {
			it.cFinished = true
			it.cValid = false
			if err := it.committed.Err(); err != nil {
				it.err = err
			}
			return
		}
		key := it.committed.Key()
		// Skip committed rows shadowed by a buffered write on the same key;
		// the buffered version (if an upsert) is emitted from the overlay.
		if _, shadowed := it.overlaySet[overlayKeyString(key)]; shadowed {
			continue
		}
		it.cKey = key
		it.cVal = it.committed.Value()
		it.cLSN = it.committed.LSN()
		it.cValid = true
		return
	}
}

func (it *txIterator) Next() bool {
	if it.err != nil {
		return false
	}
	if !it.cValid && !it.cFinished {
		it.advanceCommitted()
		if it.err != nil {
			return false
		}
	}

	overlayNext := it.oi < len(it.overlay)

	switch {
	case !it.cValid && !overlayNext:
		return false
	case !it.cValid:
		return it.emitOverlay()
	case !overlayNext:
		return it.emitCommitted()
	default:
		cmp, err := it.cKey.Compare(it.overlay[it.oi].key)
		if err != nil {
			it.err = err
			return false
		}
		if cmp <= 0 {
			// Equal keys cannot both occur (committed shadow is skipped in
			// advanceCommitted), so cmp<0 here; committed key comes first.
			return it.emitCommitted()
		}
		return it.emitOverlay()
	}
}

func (it *txIterator) emitCommitted() bool {
	it.curKey, it.curVal, it.curLSN = it.cKey, it.cVal, it.cLSN
	it.cValid = false
	return true
}

func (it *txIterator) emitOverlay() bool {
	e := it.overlay[it.oi]
	it.oi++
	it.curKey, it.curVal, it.curLSN = e.key, e.raw, 0
	return true
}

func (it *txIterator) Key() types.Comparable { return it.curKey }
func (it *txIterator) Value() []byte         { return it.curVal }
func (it *txIterator) LSN() uint64           { return it.curLSN }
func (it *txIterator) Err() error            { return it.err }

func (it *txIterator) Close() error {
	if it.committed != nil {
		return it.committed.Close()
	}
	return nil
}
