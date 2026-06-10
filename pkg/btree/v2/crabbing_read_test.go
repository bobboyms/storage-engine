package v2

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// TestCrabbingReadDescent_GetFindsKeyDuringConcurrentSplit verifies that a read
// descent does not follow a stale child pointer when a concurrent writer splits
// the target leaf.
//
// Without latch crabbing (the descent releases the parent latch before pinning
// the child), a reader paused in that window lets the writer split the target
// leaf and migrate the reader's key — always the leaf's maximum — into a new
// right sibling. The reader then lands on the now-stale leaf page and reports
// not-found for a key that still exists. This is the in-memory analogue of the
// corrupt heap read observed under TDE + concurrent vacuum, where a stale child
// pointer yields a bogus heap offset.
//
// The onDescendChild seam pauses the reader exactly at "committed to child,
// parent released". With crabbing the child is already pinned at that point, so
// the writer cannot split it out from under the reader and the lookup succeeds.
func TestCrabbingReadDescent_GetFindsKeyDuringConcurrentSplit(t *testing.T) {
	tr := newTree(t, nil)

	// Build a height-2 tree (internal root with leaf children). Keys are spaced
	// by 10 so the writer has room to insert splitting keys inside a leaf's
	// range. Stop at the first root split to keep the tree exactly two levels,
	// so the read descent takes a single internal step (one hook firing).
	const step = 64
	for n := 1; ; n++ {
		if err := tr.Insert(k(int64(n*step)), int64(n)); err != nil {
			t.Fatalf("seed insert %d: %v", n*step, err)
		}
		internal, err := rootIsInternal(tr)
		if err != nil {
			t.Fatal(err)
		}
		if internal {
			break
		}
		if n > 100000 {
			t.Fatal("root never became internal")
		}
	}

	// Target the leftmost leaf and its maximum key. The maximum always migrates
	// to the new right sibling on a leaf split, so the original leaf page stops
	// holding it the moment it splits.
	leafPID, err := firstChildLeaf(tr)
	if err != nil {
		t.Fatal(err)
	}
	lmin, kt, err := leafMinMax(tr, leafPID)
	if err != nil {
		t.Fatal(err)
	}
	if kt-lmin < 2 {
		t.Fatalf("leftmost leaf range [%d,%d] too small to split deterministically", lmin, kt)
	}

	if v, found, gerr := tr.Get(k(kt)); gerr != nil || !found {
		t.Fatalf("precondition Get(%d) = (%d,%v,%v), want found", kt, v, found, gerr)
	}

	atWindow := make(chan struct{})
	resume := make(chan struct{})
	var once sync.Once
	tr.onDescendChild = func() {
		once.Do(func() {
			close(atWindow)
			<-resume
		})
	}
	defer func() { tr.onDescendChild = nil }()

	type result struct {
		v     int64
		found bool
		err   error
	}
	readerDone := make(chan result, 1)
	go func() {
		v, found, gerr := tr.Get(k(kt))
		readerDone <- result{v, found, gerr}
	}()

	<-atWindow // reader is parked: parent released, about to use the child.

	// Writer: flood the leftmost leaf's interior with keys until it splits,
	// pushing kt (the maximum) into a new right sibling.
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for x := lmin + 1; x < kt; x++ {
			_ = tr.Insert(k(x), 0) // best effort; goal is to overflow the leaf
		}
	}()

	// Give the writer a chance to finish. Pre-fix it runs freely (the parent is
	// released); post-fix it blocks on the latch the reader still holds.
	select {
	case <-writerDone:
	case <-time.After(750 * time.Millisecond):
	}
	close(resume)

	res := <-readerDone
	<-writerDone // let the writer drain before the tree is torn down

	if res.err != nil {
		t.Fatalf("reader Get(%d) error: %v", kt, res.err)
	}
	if !res.found {
		t.Fatalf("reader Get(%d) = not found: read descent followed a stale child "+
			"pointer past a concurrent leaf split (missing latch crabbing)", kt)
	}
}

// rootIsInternal reports whether the tree root is an internal (non-leaf) node.
func rootIsInternal(tr *BTreeV2) (bool, error) {
	h, err := tr.bp.Fetch(tr.rootPage())
	if err != nil {
		return false, err
	}
	defer h.Release()
	np, err := OpenNodePage(h.Page(), tr.maxBodySize, tr.codec.Compare)
	if err != nil {
		return false, err
	}
	return !np.IsLeaf(), nil
}

// firstChildLeaf returns the leftmost child page of an internal root.
func firstChildLeaf(tr *BTreeV2) (pagestore.PageID, error) {
	h, err := tr.bp.Fetch(tr.rootPage())
	if err != nil {
		return pagestore.InvalidPageID, err
	}
	defer h.Release()
	np, err := OpenNodePage(h.Page(), tr.maxBodySize, tr.codec.Compare)
	if err != nil {
		return pagestore.InvalidPageID, err
	}
	if np.IsLeaf() {
		return pagestore.InvalidPageID, fmt.Errorf("root is a leaf")
	}
	return np.LeftmostChild(), nil
}

// leafMinMax returns the smallest and largest decoded int keys in a leaf page.
func leafMinMax(tr *BTreeV2, pid pagestore.PageID) (int64, int64, error) {
	h, err := tr.bp.Fetch(pid)
	if err != nil {
		return 0, 0, err
	}
	defer h.Release()
	np, err := OpenNodePage(h.Page(), tr.maxBodySize, tr.codec.Compare)
	if err != nil {
		return 0, 0, err
	}
	n := np.NumKeys()
	if n == 0 {
		return 0, 0, fmt.Errorf("leaf %d is empty", pid)
	}
	encMin, _, err := np.LeafAt(0)
	if err != nil {
		return 0, 0, err
	}
	encMax, _, err := np.LeafAt(n - 1)
	if err != nil {
		return 0, 0, err
	}
	return int64(tr.codec.Decode(encMin).(types.IntKey)), int64(tr.codec.Decode(encMax).(types.IntKey)), nil
}
