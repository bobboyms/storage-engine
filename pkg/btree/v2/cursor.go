package v2

import (
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// Cursor walks the leaves of a BTreeV2 in key order, exposing one
// (key, value) pair per call to Next. It pins exactly one leaf page at a
// time via the buffer pool, holding that page's read latch between
// successive Next calls — concurrent writers wait on the leaf only while
// the cursor is positioned on it, and the cursor moves on as soon as the
// current leaf is exhausted (or Close is called).
//
// Lifetime contract:
//
//   - The caller MUST call Close once it stops using the cursor. Forgetting
//     Close leaves a leaf pinned with its read latch held, which would
//     block writers on that leaf until eviction or process exit.
//   - Key and Value return the current row; their values are only valid
//     between Next returning true and the next call to Next/Close.
//   - Err returns the first error encountered. After Next returns false,
//     check Err to distinguish end-of-range from a real failure.
type Cursor struct {
	tr *BTreeV2

	// Encoded bounds. For the fixed-key path we use startU/endU; for the
	// variable path we use startV/endV. Exactly one of the two pairs is
	// populated depending on tr.isVariable.
	hasStart bool
	hasEnd   bool
	startU   uint64
	endU     uint64
	startV   []byte
	endV     []byte

	handle *pagestore.PageHandle
	pageID pagestore.PageID
	idx    int
	n      int

	// Current row (decoded for the caller).
	curKey   types.Comparable
	curValue int64

	err    error
	closed bool
	primed bool // first Next() seeks to the start leaf
}

// NewCursor opens a cursor over [lower, upper] (inclusive). Pass nil for
// either bound to leave it unbounded. The returned cursor must be closed
// by the caller.
func (tr *BTreeV2) NewCursor(lower, upper types.Comparable) (*Cursor, error) {
	c := &Cursor{tr: tr}
	if lower != nil {
		c.hasStart = true
		if tr.isVariable {
			enc, err := tr.varCodec.Encode(lower)
			if err != nil {
				return nil, err
			}
			c.startV = enc
		} else {
			enc, err := tr.codec.Encode(lower)
			if err != nil {
				return nil, err
			}
			c.startU = enc
		}
	}
	if upper != nil {
		c.hasEnd = true
		if tr.isVariable {
			enc, err := tr.varCodec.Encode(upper)
			if err != nil {
				return nil, err
			}
			c.endV = enc
		} else {
			enc, err := tr.codec.Encode(upper)
			if err != nil {
				return nil, err
			}
			c.endU = enc
		}
	}
	return c, nil
}

// Next advances to the next (key, value) pair. Returns true when a row is
// available, false when the cursor reaches the end of the range or
// encounters an error (use Err to disambiguate).
func (c *Cursor) Next() bool {
	if c.closed || c.err != nil {
		return false
	}

	if !c.primed {
		if err := c.seekToStartLeaf(); err != nil {
			c.err = err
			c.releaseHandle()
			return false
		}
		c.primed = true
	}

	for c.handle != nil {
		if c.idx < c.n {
			ok, stop, err := c.readCurrentSlot()
			if err != nil {
				c.err = err
				c.releaseHandle()
				return false
			}
			c.idx++
			if stop {
				c.releaseHandle()
				return false
			}
			if ok {
				return true
			}
			continue
		}

		if err := c.advanceToNextLeaf(); err != nil {
			c.err = err
			c.releaseHandle()
			return false
		}
	}
	return false
}

// Key returns the current row's key. Only valid after Next returned true.
func (c *Cursor) Key() types.Comparable { return c.curKey }

// Value returns the current row's leaf value (heap offset for engine
// indexes). Only valid after Next returned true.
func (c *Cursor) Value() int64 { return c.curValue }

// Err returns the first error encountered, or nil.
func (c *Cursor) Err() error { return c.err }

// Close releases any pinned page and is safe to call multiple times.
func (c *Cursor) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	c.releaseHandle()
	return nil
}

func (c *Cursor) releaseHandle() {
	if c.handle != nil {
		c.handle.Release()
		c.handle = nil
	}
	c.pageID = pagestore.InvalidPageID
	c.n = 0
	c.idx = 0
}

// seekToStartLeaf positions the cursor on the leaf holding the start bound (or
// the leftmost leaf for an unbounded scan). It uses the crabbing read descent,
// which hands back the leaf with its read latch already held, so the leaf the
// cursor reads from is never re-fetched by page id (closing the descend→re-pin
// window where a concurrent split could swap the page out).
func (c *Cursor) seekToStartLeaf() error {
	var (
		h   *pagestore.PageHandle
		err error
	)
	if c.tr.isVariable {
		h, err = c.tr.descendReadLeafVar(c.startV, !c.hasStart)
	} else {
		h, err = c.tr.descendReadLeafFixed(c.startU, !c.hasStart)
	}
	if err != nil {
		return err
	}
	return c.adoptLeaf(h)
}

// adoptLeaf takes ownership of an already-pinned leaf handle and stages it as
// the cursor's current page.
func (c *Cursor) adoptLeaf(h *pagestore.PageHandle) error {
	if c.tr.isVariable {
		vp, err := OpenVariableNodePage(h.Page(), c.tr.maxBodySize, c.tr.varCodec.Compare)
		if err != nil {
			h.Release()
			return err
		}
		c.n = vp.NumKeys()
	} else {
		np, err := OpenNodePage(h.Page(), c.tr.maxBodySize, c.tr.codec.Compare)
		if err != nil {
			h.Release()
			return err
		}
		c.n = np.NumKeys()
	}
	c.handle = h
	c.pageID = h.ID()
	c.idx = 0
	return nil
}

// readCurrentSlot loads the current slot from the pinned leaf, applies
// range filters, and stages the row on c.curKey / c.curValue when the
// caller should observe it.
//
// Returns:
//
//	(true,  false, nil) — row published, caller should return from Next
//	(false, false, nil) — row skipped (below start), keep iterating
//	(false, true,  nil) — past end bound, cursor is done
//	(_,     _,     err) — fatal error
func (c *Cursor) readCurrentSlot() (bool, bool, error) {
	if c.tr.isVariable {
		vp, err := OpenVariableNodePage(c.handle.Page(), c.tr.maxBodySize, c.tr.varCodec.Compare)
		if err != nil {
			return false, false, fmt.Errorf("btree cursor: reopen variable leaf: %w", err)
		}
		key, val := vp.LeafAtVar(c.idx)
		if c.hasStart && c.tr.varCodec.Compare(key, c.startV) < 0 {
			return false, false, nil
		}
		if c.hasEnd && c.tr.varCodec.Compare(key, c.endV) > 0 {
			return false, true, nil
		}
		// Page body can mutate after Release, so copy the key bytes.
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		c.curKey = c.tr.varCodec.Decode(keyCopy)
		c.curValue = val
		return true, false, nil
	}

	np, err := OpenNodePage(c.handle.Page(), c.tr.maxBodySize, c.tr.codec.Compare)
	if err != nil {
		return false, false, fmt.Errorf("btree cursor: reopen leaf: %w", err)
	}
	key, val, err := np.LeafAt(c.idx)
	if err != nil {
		return false, false, err
	}
	if c.hasStart && c.tr.codec.Compare(key, c.startU) < 0 {
		return false, false, nil
	}
	if c.hasEnd && c.tr.codec.Compare(key, c.endU) > 0 {
		return false, true, nil
	}
	c.curKey = c.tr.codec.Decode(key)
	c.curValue = val
	return true, false, nil
}

func (c *Cursor) advanceToNextLeaf() error {
	var nextLeaf pagestore.PageID
	if c.tr.isVariable {
		vp, err := OpenVariableNodePage(c.handle.Page(), c.tr.maxBodySize, c.tr.varCodec.Compare)
		if err != nil {
			return err
		}
		nextLeaf = vp.NextLeafPageID()
	} else {
		np, err := OpenNodePage(c.handle.Page(), c.tr.maxBodySize, c.tr.codec.Compare)
		if err != nil {
			return err
		}
		nextLeaf = np.NextLeafPageID()
	}
	// The current leaf MUST be released before pinning the right sibling.
	// Crabbing here (hold current, then fetch next) would order leaf latches
	// left-to-right, while the delete paths' merge/redistribute lock the
	// underflowing child first and then FetchForWrite its LEFT sibling
	// (right-to-left; see mergeWithLeftVar and the delete_fixed.go merges) —
	// holding both would form an ABBA deadlock cycle with a concurrent merge.
	// The cost is a small window where the sibling can be restructured between
	// release and pin; the descent itself crabs (descendReadLeafFixed/Var), so
	// lookups and scan starts never land on a stale page.
	c.releaseHandle()
	if nextLeaf == pagestore.InvalidPageID {
		return nil
	}
	h, err := c.tr.bp.Fetch(nextLeaf)
	if err != nil {
		return err
	}
	return c.adoptLeaf(h)
}
