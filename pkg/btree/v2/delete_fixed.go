package v2

import "github.com/bobboyms/storage-engine/pkg/pagestore"

type fixedLeafEntry struct {
	key   uint64
	value int64
}

type fixedInternalEntry struct {
	key   uint64
	child pagestore.PageID
}

func collectLeafEntriesFixed(np *NodePage) []fixedLeafEntry {
	entries := make([]fixedLeafEntry, 0, np.NumKeys())
	for i := 0; i < np.NumKeys(); i++ {
		key, value := np.LeafAt(i)
		entries = append(entries, fixedLeafEntry{key: key, value: value})
	}
	return entries
}

func collectInternalEntriesFixed(np *NodePage) (pagestore.PageID, []fixedInternalEntry) {
	entries := make([]fixedInternalEntry, 0, np.NumKeys())
	for i := 0; i < np.NumKeys(); i++ {
		key, child := np.InternalAt(i)
		entries = append(entries, fixedInternalEntry{key: key, child: child})
	}
	return np.LeftmostChild(), entries
}

func rebuildLeafFixed(np *NodePage, entries []fixedLeafEntry, nextLeaf pagestore.PageID) {
	InitLeafPage(np.page, np.maxBodySize, np.cmp)
	np.setNextLeafPageID(nextLeaf)
	for _, entry := range entries {
		if err := np.LeafInsert(entry.key, entry.value); err != nil {
			panic(err)
		}
	}
}

func rebuildInternalFixed(np *NodePage, leftmost pagestore.PageID, entries []fixedInternalEntry) {
	InitInternalPage(np.page, np.maxBodySize, leftmost, np.cmp)
	for _, entry := range entries {
		if err := np.InsertSeparator(entry.key, entry.child); err != nil {
			panic(err)
		}
	}
}

func fixedChildPageIDAt(np *NodePage, idx int) pagestore.PageID {
	if idx == 0 {
		return np.LeftmostChild()
	}
	_, child := np.InternalAt(idx - 1)
	return child
}

func fixedMinKeys(np *NodePage, isRoot bool) int {
	if isRoot {
		if np.IsLeaf() {
			return 0
		}
		return 1
	}
	if np.IsLeaf() {
		return np.MaxLeafSlots() / 2
	}
	min := (np.MaxInternalSlots() - 1) / 2
	if min < 1 {
		return 1
	}
	return min
}

func fixedFirstKey(np *NodePage) (uint64, bool) {
	if np.IsLeaf() {
		if np.NumKeys() == 0 {
			return 0, false
		}
		key, _ := np.LeafAt(0)
		return key, true
	}
	if np.NumKeys() == 0 {
		return 0, false
	}
	key, _ := np.InternalAt(0)
	return key, true
}

func (tr *BTreeV2) fixedSubtreeMin(np *NodePage, held map[pagestore.PageID]*NodePage) (uint64, bool, error) {
	if np == nil {
		return 0, false, nil
	}

	curr := np
	var fetched []*pagestore.PageHandle
	defer func() {
		for i := len(fetched) - 1; i >= 0; i-- {
			fetched[i].Release()
		}
	}()

	for !curr.IsLeaf() {
		childPageID := curr.LeftmostChild()
		if heldNP, ok := held[childPageID]; ok {
			curr = heldNP
			continue
		}

		h, err := tr.bp.Fetch(childPageID)
		if err != nil {
			return 0, false, err
		}
		fetched = append(fetched, h)

		next, err := OpenNodePage(h.Page(), tr.maxBodySize, tr.codec.Compare)
		if err != nil {
			return 0, false, err
		}
		curr = next
	}

	key, ok := fixedFirstKey(curr)
	return key, ok, nil
}

func (tr *BTreeV2) fixedSubtreeMinByPageID(pageID pagestore.PageID, held map[pagestore.PageID]*NodePage) (uint64, bool, error) {
	if np, ok := held[pageID]; ok {
		return tr.fixedSubtreeMin(np, held)
	}

	h, err := tr.bp.Fetch(pageID)
	if err != nil {
		return 0, false, err
	}
	defer h.Release()

	np, err := OpenNodePage(h.Page(), tr.maxBodySize, tr.codec.Compare)
	if err != nil {
		return 0, false, err
	}
	return tr.fixedSubtreeMin(np, held)
}

func (tr *BTreeV2) refreshInternalSeparatorsFixed(parentNP *NodePage, held map[pagestore.PageID]*NodePage) error {
	leftmost, entries := collectInternalEntriesFixed(parentNP)
	for i := range entries {
		minKey, ok, err := tr.fixedSubtreeMinByPageID(entries[i].child, held)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		entries[i].key = minKey
	}
	rebuildInternalFixed(parentNP, leftmost, entries)
	return nil
}

func (tr *BTreeV2) fixChildUnderflowFixed(
	parentH *pagestore.PageHandle,
	parentNP *NodePage,
	childH *pagestore.PageHandle,
	childNP *NodePage,
	childPos int,
) (*pagestore.PageHandle, *NodePage, int, error) {
	if childNP.NumKeys() >= fixedMinKeys(childNP, false) {
		return childH, childNP, childPos, nil
	}

	if childPos > 0 {
		leftH, err := tr.bp.FetchForWrite(fixedChildPageIDAt(parentNP, childPos-1))
		if err != nil {
			return nil, nil, 0, err
		}
		leftNP, err := OpenNodePage(leftH.Page(), tr.maxBodySize, tr.codec.Compare)
		if err != nil {
			leftH.Release()
			return nil, nil, 0, err
		}
		if leftNP.NumKeys() > fixedMinKeys(leftNP, false) {
			if childNP.IsLeaf() {
				leftEntries := collectLeafEntriesFixed(leftNP)
				childEntries := collectLeafEntriesFixed(childNP)
				moved := leftEntries[len(leftEntries)-1]
				leftEntries = leftEntries[:len(leftEntries)-1]
				childEntries = append([]fixedLeafEntry{moved}, childEntries...)

				rebuildLeafFixed(leftNP, leftEntries, leftNP.NextLeafPageID())
				rebuildLeafFixed(childNP, childEntries, childNP.NextLeafPageID())
			} else {
				leftLeftmost, leftEntries := collectInternalEntriesFixed(leftNP)
				childLeftmost, childEntries := collectInternalEntriesFixed(childNP)
				parentSep, _ := parentNP.InternalAt(childPos - 1)
				movedChild := leftEntries[len(leftEntries)-1].child
				leftEntries = leftEntries[:len(leftEntries)-1]
				childEntries = append([]fixedInternalEntry{{key: parentSep, child: childLeftmost}}, childEntries...)

				rebuildInternalFixed(leftNP, leftLeftmost, leftEntries)
				rebuildInternalFixed(childNP, movedChild, childEntries)
			}

			tr.markDirty(leftH)
			tr.markDirty(childH)
			tr.markDirty(parentH)
			leftH.Release()
			return childH, childNP, childPos, nil
		}
		leftH.Release()
	}

	if childPos < parentNP.NumKeys() {
		rightH, err := tr.bp.FetchForWrite(fixedChildPageIDAt(parentNP, childPos+1))
		if err != nil {
			return nil, nil, 0, err
		}
		rightNP, err := OpenNodePage(rightH.Page(), tr.maxBodySize, tr.codec.Compare)
		if err != nil {
			rightH.Release()
			return nil, nil, 0, err
		}
		if rightNP.NumKeys() > fixedMinKeys(rightNP, false) {
			if childNP.IsLeaf() {
				childEntries := collectLeafEntriesFixed(childNP)
				rightEntries := collectLeafEntriesFixed(rightNP)
				childEntries = append(childEntries, rightEntries[0])
				rightEntries = rightEntries[1:]

				rebuildLeafFixed(childNP, childEntries, childNP.NextLeafPageID())
				rebuildLeafFixed(rightNP, rightEntries, rightNP.NextLeafPageID())
			} else {
				childLeftmost, childEntries := collectInternalEntriesFixed(childNP)
				rightLeftmost, rightEntries := collectInternalEntriesFixed(rightNP)
				parentSep, _ := parentNP.InternalAt(childPos)

				childEntries = append(childEntries, fixedInternalEntry{key: parentSep, child: rightLeftmost})
				newRightLeftmost := rightEntries[0].child
				rightEntries = rightEntries[1:]

				rebuildInternalFixed(childNP, childLeftmost, childEntries)
				rebuildInternalFixed(rightNP, newRightLeftmost, rightEntries)
			}

			tr.markDirty(childH)
			tr.markDirty(rightH)
			tr.markDirty(parentH)
			rightH.Release()
			return childH, childNP, childPos, nil
		}
		rightH.Release()
	}

	if childPos > 0 {
		leftH, err := tr.bp.FetchForWrite(fixedChildPageIDAt(parentNP, childPos-1))
		if err != nil {
			return nil, nil, 0, err
		}
		leftNP, err := OpenNodePage(leftH.Page(), tr.maxBodySize, tr.codec.Compare)
		if err != nil {
			leftH.Release()
			return nil, nil, 0, err
		}

		if childNP.IsLeaf() {
			leftEntries := collectLeafEntriesFixed(leftNP)
			childEntries := collectLeafEntriesFixed(childNP)
			leftEntries = append(leftEntries, childEntries...)
			rebuildLeafFixed(leftNP, leftEntries, childNP.NextLeafPageID())
		} else {
			leftLeftmost, leftEntries := collectInternalEntriesFixed(leftNP)
			childLeftmost, childEntries := collectInternalEntriesFixed(childNP)
			parentSep, _ := parentNP.InternalAt(childPos - 1)
			leftEntries = append(leftEntries, fixedInternalEntry{key: parentSep, child: childLeftmost})
			leftEntries = append(leftEntries, childEntries...)
			rebuildInternalFixed(leftNP, leftLeftmost, leftEntries)
		}

		parentLeftmost, parentEntries := collectInternalEntriesFixed(parentNP)
		parentEntries = append(parentEntries[:childPos-1], parentEntries[childPos:]...)
		rebuildInternalFixed(parentNP, parentLeftmost, parentEntries)

		tr.markDirty(leftH)
		tr.markDirty(parentH)
		childH.Release()
		return leftH, leftNP, childPos - 1, nil
	}

	rightH, err := tr.bp.FetchForWrite(fixedChildPageIDAt(parentNP, childPos+1))
	if err != nil {
		return nil, nil, 0, err
	}
	rightNP, err := OpenNodePage(rightH.Page(), tr.maxBodySize, tr.codec.Compare)
	if err != nil {
		rightH.Release()
		return nil, nil, 0, err
	}

	if childNP.IsLeaf() {
		childEntries := collectLeafEntriesFixed(childNP)
		rightEntries := collectLeafEntriesFixed(rightNP)
		childEntries = append(childEntries, rightEntries...)
		rebuildLeafFixed(childNP, childEntries, rightNP.NextLeafPageID())
	} else {
		childLeftmost, childEntries := collectInternalEntriesFixed(childNP)
		rightLeftmost, rightEntries := collectInternalEntriesFixed(rightNP)
		parentSep, _ := parentNP.InternalAt(childPos)
		childEntries = append(childEntries, fixedInternalEntry{key: parentSep, child: rightLeftmost})
		childEntries = append(childEntries, rightEntries...)
		rebuildInternalFixed(childNP, childLeftmost, childEntries)
	}

	parentLeftmost, parentEntries := collectInternalEntriesFixed(parentNP)
	parentEntries = append(parentEntries[:childPos], parentEntries[childPos+1:]...)
	rebuildInternalFixed(parentNP, parentLeftmost, parentEntries)

	tr.markDirty(childH)
	tr.markDirty(parentH)
	rightH.Release()
	return childH, childNP, childPos, nil
}
