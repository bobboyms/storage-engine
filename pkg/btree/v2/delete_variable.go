package v2

import (
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

type varLeafEntry struct {
	key   []byte
	value int64
}

type varInternalEntry struct {
	key   []byte
	child pagestore.PageID
}

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func collectLeafEntriesVar(vp *VariableNodePage) []varLeafEntry {
	entries := make([]varLeafEntry, 0, vp.NumKeys())
	for i := 0; i < vp.NumKeys(); i++ {
		key, value := vp.LeafAtVar(i)
		entries = append(entries, varLeafEntry{key: cloneBytes(key), value: value})
	}
	return entries
}

func collectInternalEntriesVar(vp *VariableNodePage) (pagestore.PageID, []varInternalEntry) {
	entries := make([]varInternalEntry, 0, vp.NumKeys())
	for i := 0; i < vp.NumKeys(); i++ {
		key, child := vp.InternalAtVar(i)
		entries = append(entries, varInternalEntry{key: cloneBytes(key), child: child})
	}
	return vp.LeftmostChild(), entries
}

func rebuildLeafVar(vp *VariableNodePage, entries []varLeafEntry, nextLeaf pagestore.PageID) error {
	InitLeafPageVar(vp.page, vp.maxBodySize, vp.cmp)
	vp.setNextLeafPageID(nextLeaf)
	for _, entry := range entries {
		if err := vp.LeafInsertVar(entry.key, entry.value); err != nil {
			return fmt.Errorf("btree/v2: rebuild leaf insert: %w", err)
		}
	}
	return nil
}

func rebuildInternalVar(vp *VariableNodePage, leftmost pagestore.PageID, entries []varInternalEntry) error {
	InitInternalPageVar(vp.page, vp.maxBodySize, leftmost, vp.cmp)
	for _, entry := range entries {
		if err := vp.InsertSeparatorVar(entry.key, entry.child); err != nil {
			return fmt.Errorf("btree/v2: rebuild internal insert: %w", err)
		}
	}
	return nil
}

func varLeafPayload(entries []varLeafEntry) int {
	total := 0
	for _, entry := range entries {
		total += VariableSlotSize + len(entry.key)
	}
	return total
}

func varInternalPayload(entries []varInternalEntry) int {
	total := 0
	for _, entry := range entries {
		total += VariableSlotSize + len(entry.key)
	}
	return total
}

func varLeafCapacity(vp *VariableNodePage) int {
	return vp.maxBodySize - NodeHeaderSize
}

func varInternalCapacity(vp *VariableNodePage) int {
	return vp.maxBodySize - NodeHeaderSize - LeftmostChildSize
}

func canFitLeafEntriesVar(vp *VariableNodePage, entries []varLeafEntry) bool {
	return varLeafPayload(entries) <= varLeafCapacity(vp)
}

func canFitInternalEntriesVar(vp *VariableNodePage, entries []varInternalEntry) bool {
	return varInternalPayload(entries) <= varInternalCapacity(vp)
}

func varChildPageIDAt(vp *VariableNodePage, idx int) pagestore.PageID {
	if idx == 0 {
		return vp.LeftmostChild()
	}
	_, child := vp.InternalAtVar(idx - 1)
	return child
}

func varMinKeys(vp *VariableNodePage, isRoot bool) int {
	if isRoot {
		if vp.IsLeaf() {
			return 0
		}
		return 1
	}
	if vp.IsLeaf() {
		maxLeafSlots := (vp.maxBodySize - NodeHeaderSize) / VariableSlotSize
		return maxLeafSlots / 2
	}
	maxInternalSlots := (vp.maxBodySize - NodeHeaderSize - LeftmostChildSize) / VariableSlotSize
	min := (maxInternalSlots - 1) / 2
	if min < 1 {
		return 1
	}
	return min
}

func varFirstKey(vp *VariableNodePage) ([]byte, bool) {
	if vp.IsLeaf() {
		if vp.NumKeys() == 0 {
			return nil, false
		}
		key, _ := vp.LeafAtVar(0)
		return cloneBytes(key), true
	}
	if vp.NumKeys() == 0 {
		return nil, false
	}
	key, _ := vp.InternalAtVar(0)
	return cloneBytes(key), true
}

func (tr *BTreeV2) varSubtreeMin(vp *VariableNodePage, held map[pagestore.PageID]*VariableNodePage) ([]byte, bool, error) {
	if vp == nil {
		return nil, false, nil
	}

	curr := vp
	var fetched []*pagestore.PageHandle
	defer func() {
		for i := len(fetched) - 1; i >= 0; i-- {
			fetched[i].Release()
		}
	}()

	for !curr.IsLeaf() {
		childPageID := curr.LeftmostChild()
		if heldVP, ok := held[childPageID]; ok {
			curr = heldVP
			continue
		}

		h, err := tr.bp.Fetch(childPageID)
		if err != nil {
			return nil, false, err
		}
		fetched = append(fetched, h)

		next, err := OpenVariableNodePage(h.Page(), tr.maxBodySize, tr.varCodec.Compare)
		if err != nil {
			return nil, false, err
		}
		curr = next
	}

	key, ok := varFirstKey(curr)
	return key, ok, nil
}

func (tr *BTreeV2) varSubtreeMinByPageID(pageID pagestore.PageID, held map[pagestore.PageID]*VariableNodePage) ([]byte, bool, error) {
	if vp, ok := held[pageID]; ok {
		return tr.varSubtreeMin(vp, held)
	}

	h, err := tr.bp.Fetch(pageID)
	if err != nil {
		return nil, false, err
	}
	defer h.Release()

	vp, err := OpenVariableNodePage(h.Page(), tr.maxBodySize, tr.varCodec.Compare)
	if err != nil {
		return nil, false, err
	}
	return tr.varSubtreeMin(vp, held)
}

func (tr *BTreeV2) refreshInternalSeparatorsVar(parentVP *VariableNodePage, held map[pagestore.PageID]*VariableNodePage) error {
	leftmost, entries := collectInternalEntriesVar(parentVP)
	for i := range entries {
		minKey, ok, err := tr.varSubtreeMinByPageID(entries[i].child, held)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		entries[i].key = minKey
	}
	return rebuildInternalVar(parentVP, leftmost, entries)
}

func (tr *BTreeV2) fixChildUnderflowVar(
	parentH *pagestore.PageHandle,
	parentVP *VariableNodePage,
	childH *pagestore.PageHandle,
	childVP *VariableNodePage,
	childPos int,
) (*pagestore.PageHandle, *VariableNodePage, int, error) {
	if childVP.NumKeys() >= varMinKeys(childVP, false) {
		return childH, childVP, childPos, nil
	}

	if done, h, vp, pos, err := tr.borrowFromLeftVar(parentH, parentVP, childH, childVP, childPos); done || err != nil {
		return h, vp, pos, err
	}
	if done, h, vp, pos, err := tr.borrowFromRightVar(parentH, parentVP, childH, childVP, childPos); done || err != nil {
		return h, vp, pos, err
	}
	if done, h, vp, pos, err := tr.mergeWithLeftVar(parentH, parentVP, childH, childVP, childPos); done || err != nil {
		return h, vp, pos, err
	}
	return tr.mergeWithRightVar(parentH, parentVP, childH, childVP, childPos)
}

// borrowFromLeftVar tries to rebalance an underflowing child by moving
// one entry from its left sibling. Returns done=true when it rebalanced;
// done=false (err==nil) means the left sibling cannot lend, so the caller
// should try the next strategy.
func (tr *BTreeV2) borrowFromLeftVar(
	parentH *pagestore.PageHandle,
	parentVP *VariableNodePage,
	childH *pagestore.PageHandle,
	childVP *VariableNodePage,
	childPos int,
) (bool, *pagestore.PageHandle, *VariableNodePage, int, error) {
	if childPos <= 0 {
		return false, nil, nil, 0, nil
	}
	leftH, err := tr.bp.FetchForWrite(varChildPageIDAt(parentVP, childPos-1))
	if err != nil {
		return false, nil, nil, 0, err
	}
	leftVP, err := OpenVariableNodePage(leftH.Page(), tr.maxBodySize, tr.varCodec.Compare)
	if err != nil {
		leftH.Release()
		return false, nil, nil, 0, err
	}
	if leftVP.NumKeys() <= varMinKeys(leftVP, false) {
		leftH.Release()
		return false, nil, nil, 0, nil
	}

	if childVP.IsLeaf() {
		leftEntries := collectLeafEntriesVar(leftVP)
		childEntries := collectLeafEntriesVar(childVP)
		moved := leftEntries[len(leftEntries)-1]
		leftEntries = leftEntries[:len(leftEntries)-1]
		childEntries = append([]varLeafEntry{moved}, childEntries...)
		if !canFitLeafEntriesVar(childVP, childEntries) {
			leftH.Release()
			return false, nil, nil, 0, nil
		}
		if err := rebuildLeafVar(leftVP, leftEntries, leftVP.NextLeafPageID()); err != nil {
			leftH.Release()
			return false, nil, nil, 0, err
		}
		if err := rebuildLeafVar(childVP, childEntries, childVP.NextLeafPageID()); err != nil {
			leftH.Release()
			return false, nil, nil, 0, err
		}
	} else {
		leftLeftmost, leftEntries := collectInternalEntriesVar(leftVP)
		childLeftmost, childEntries := collectInternalEntriesVar(childVP)
		parentSep, _ := parentVP.InternalAtVar(childPos - 1)
		movedChild := leftEntries[len(leftEntries)-1].child
		leftEntries = leftEntries[:len(leftEntries)-1]
		childEntries = append([]varInternalEntry{{key: cloneBytes(parentSep), child: childLeftmost}}, childEntries...)
		if !canFitInternalEntriesVar(childVP, childEntries) {
			leftH.Release()
			return false, nil, nil, 0, nil
		}
		if err := rebuildInternalVar(leftVP, leftLeftmost, leftEntries); err != nil {
			leftH.Release()
			return false, nil, nil, 0, err
		}
		if err := rebuildInternalVar(childVP, movedChild, childEntries); err != nil {
			leftH.Release()
			return false, nil, nil, 0, err
		}
	}

	tr.markDirty(leftH)
	tr.markDirty(childH)
	tr.markDirty(parentH)
	leftH.Release()
	return true, childH, childVP, childPos, nil
}

// borrowFromRightVar mirrors borrowFromLeftVar for the right sibling.
func (tr *BTreeV2) borrowFromRightVar(
	parentH *pagestore.PageHandle,
	parentVP *VariableNodePage,
	childH *pagestore.PageHandle,
	childVP *VariableNodePage,
	childPos int,
) (bool, *pagestore.PageHandle, *VariableNodePage, int, error) {
	if childPos >= parentVP.NumKeys() {
		return false, nil, nil, 0, nil
	}
	rightH, err := tr.bp.FetchForWrite(varChildPageIDAt(parentVP, childPos+1))
	if err != nil {
		return false, nil, nil, 0, err
	}
	rightVP, err := OpenVariableNodePage(rightH.Page(), tr.maxBodySize, tr.varCodec.Compare)
	if err != nil {
		rightH.Release()
		return false, nil, nil, 0, err
	}
	if rightVP.NumKeys() <= varMinKeys(rightVP, false) {
		rightH.Release()
		return false, nil, nil, 0, nil
	}

	if childVP.IsLeaf() {
		childEntries := collectLeafEntriesVar(childVP)
		rightEntries := collectLeafEntriesVar(rightVP)
		childEntries = append(childEntries, rightEntries[0])
		rightEntries = rightEntries[1:]
		if !canFitLeafEntriesVar(childVP, childEntries) {
			rightH.Release()
			return false, nil, nil, 0, nil
		}
		if err := rebuildLeafVar(childVP, childEntries, childVP.NextLeafPageID()); err != nil {
			rightH.Release()
			return false, nil, nil, 0, err
		}
		if err := rebuildLeafVar(rightVP, rightEntries, rightVP.NextLeafPageID()); err != nil {
			rightH.Release()
			return false, nil, nil, 0, err
		}
	} else {
		childLeftmost, childEntries := collectInternalEntriesVar(childVP)
		rightLeftmost, rightEntries := collectInternalEntriesVar(rightVP)
		parentSep, _ := parentVP.InternalAtVar(childPos)

		childEntries = append(childEntries, varInternalEntry{key: cloneBytes(parentSep), child: rightLeftmost})
		newRightLeftmost := rightEntries[0].child
		rightEntries = rightEntries[1:]
		if !canFitInternalEntriesVar(childVP, childEntries) {
			rightH.Release()
			return false, nil, nil, 0, nil
		}
		if err := rebuildInternalVar(childVP, childLeftmost, childEntries); err != nil {
			rightH.Release()
			return false, nil, nil, 0, err
		}
		if err := rebuildInternalVar(rightVP, newRightLeftmost, rightEntries); err != nil {
			rightH.Release()
			return false, nil, nil, 0, err
		}
	}

	tr.markDirty(childH)
	tr.markDirty(rightH)
	tr.markDirty(parentH)
	rightH.Release()
	return true, childH, childVP, childPos, nil
}

// mergeWithLeftVar merges the child into its left sibling. Returns
// done=true with the surviving left page on success; done=false
// (err==nil) when there is no left sibling or the merge does not fit.
func (tr *BTreeV2) mergeWithLeftVar(
	parentH *pagestore.PageHandle,
	parentVP *VariableNodePage,
	childH *pagestore.PageHandle,
	childVP *VariableNodePage,
	childPos int,
) (bool, *pagestore.PageHandle, *VariableNodePage, int, error) {
	if childPos <= 0 {
		return false, nil, nil, 0, nil
	}
	leftH, err := tr.bp.FetchForWrite(varChildPageIDAt(parentVP, childPos-1))
	if err != nil {
		return false, nil, nil, 0, err
	}
	leftVP, err := OpenVariableNodePage(leftH.Page(), tr.maxBodySize, tr.varCodec.Compare)
	if err != nil {
		leftH.Release()
		return false, nil, nil, 0, err
	}

	if childVP.IsLeaf() {
		leftEntries := collectLeafEntriesVar(leftVP)
		childEntries := collectLeafEntriesVar(childVP)
		leftEntries = append(leftEntries, childEntries...)
		if !canFitLeafEntriesVar(leftVP, leftEntries) {
			leftH.Release()
			return false, nil, nil, 0, nil
		}
		if err := rebuildLeafVar(leftVP, leftEntries, childVP.NextLeafPageID()); err != nil {
			leftH.Release()
			return false, nil, nil, 0, err
		}
	} else {
		leftLeftmost, leftEntries := collectInternalEntriesVar(leftVP)
		childLeftmost, childEntries := collectInternalEntriesVar(childVP)
		parentSep, _ := parentVP.InternalAtVar(childPos - 1)
		leftEntries = append(leftEntries, varInternalEntry{key: cloneBytes(parentSep), child: childLeftmost})
		leftEntries = append(leftEntries, childEntries...)
		if !canFitInternalEntriesVar(leftVP, leftEntries) {
			leftH.Release()
			return false, nil, nil, 0, nil
		}
		if err := rebuildInternalVar(leftVP, leftLeftmost, leftEntries); err != nil {
			leftH.Release()
			return false, nil, nil, 0, err
		}
	}

	parentLeftmost, parentEntries := collectInternalEntriesVar(parentVP)
	parentEntries = append(parentEntries[:childPos-1], parentEntries[childPos:]...)
	if err := rebuildInternalVar(parentVP, parentLeftmost, parentEntries); err != nil {
		leftH.Release()
		return false, nil, nil, 0, err
	}

	tr.markDirty(leftH)
	tr.markDirty(parentH)
	childH.Release()
	return true, leftH, leftVP, childPos - 1, nil
}

// mergeWithRightVar is the last-resort strategy: merge the right sibling
// into the child. It always returns the resulting child handle (unchanged
// when there is no right sibling or the merge does not fit).
func (tr *BTreeV2) mergeWithRightVar(
	parentH *pagestore.PageHandle,
	parentVP *VariableNodePage,
	childH *pagestore.PageHandle,
	childVP *VariableNodePage,
	childPos int,
) (*pagestore.PageHandle, *VariableNodePage, int, error) {
	if childPos >= parentVP.NumKeys() {
		return childH, childVP, childPos, nil
	}

	rightH, err := tr.bp.FetchForWrite(varChildPageIDAt(parentVP, childPos+1))
	if err != nil {
		return nil, nil, 0, err
	}
	rightVP, err := OpenVariableNodePage(rightH.Page(), tr.maxBodySize, tr.varCodec.Compare)
	if err != nil {
		rightH.Release()
		return nil, nil, 0, err
	}

	if childVP.IsLeaf() {
		childEntries := collectLeafEntriesVar(childVP)
		rightEntries := collectLeafEntriesVar(rightVP)
		childEntries = append(childEntries, rightEntries...)
		if !canFitLeafEntriesVar(childVP, childEntries) {
			rightH.Release()
			return childH, childVP, childPos, nil
		}
		if err := rebuildLeafVar(childVP, childEntries, rightVP.NextLeafPageID()); err != nil {
			rightH.Release()
			return nil, nil, 0, err
		}
	} else {
		childLeftmost, childEntries := collectInternalEntriesVar(childVP)
		rightLeftmost, rightEntries := collectInternalEntriesVar(rightVP)
		parentSep, _ := parentVP.InternalAtVar(childPos)
		childEntries = append(childEntries, varInternalEntry{key: cloneBytes(parentSep), child: rightLeftmost})
		childEntries = append(childEntries, rightEntries...)
		if !canFitInternalEntriesVar(childVP, childEntries) {
			rightH.Release()
			return childH, childVP, childPos, nil
		}
		if err := rebuildInternalVar(childVP, childLeftmost, childEntries); err != nil {
			rightH.Release()
			return nil, nil, 0, err
		}
	}

	parentLeftmost, parentEntries := collectInternalEntriesVar(parentVP)
	parentEntries = append(parentEntries[:childPos], parentEntries[childPos+1:]...)
	if err := rebuildInternalVar(parentVP, parentLeftmost, parentEntries); err != nil {
		rightH.Release()
		return nil, nil, 0, err
	}

	tr.markDirty(childH)
	tr.markDirty(parentH)
	rightH.Release()
	return childH, childVP, childPos, nil
}
