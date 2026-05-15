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

func collectLeafEntriesFixed(np *NodePage) ([]fixedLeafEntry, error) {
	entries := make([]fixedLeafEntry, 0, np.NumKeys())
	for i := 0; i < np.NumKeys(); i++ {
		key, value, err := np.LeafAt(i)
		if err != nil {
			return nil, err
		}
		entries = append(entries, fixedLeafEntry{key: key, value: value})
	}
	return entries, nil
}

func collectInternalEntriesFixed(np *NodePage) (pagestore.PageID, []fixedInternalEntry, error) {
	entries := make([]fixedInternalEntry, 0, np.NumKeys())
	for i := 0; i < np.NumKeys(); i++ {
		key, child, err := np.InternalAt(i)
		if err != nil {
			return pagestore.InvalidPageID, nil, err
		}
		entries = append(entries, fixedInternalEntry{key: key, child: child})
	}
	return np.LeftmostChild(), entries, nil
}

func rebuildLeafFixed(np *NodePage, entries []fixedLeafEntry, nextLeaf pagestore.PageID) error {
	InitLeafPage(np.page, np.maxBodySize, np.cmp)
	np.setNextLeafPageID(nextLeaf)
	for _, entry := range entries {
		if err := np.LeafInsert(entry.key, entry.value); err != nil {
			return err
		}
	}
	return nil
}

func rebuildInternalFixed(np *NodePage, leftmost pagestore.PageID, entries []fixedInternalEntry) error {
	InitInternalPage(np.page, np.maxBodySize, leftmost, np.cmp)
	for _, entry := range entries {
		if err := np.InsertSeparator(entry.key, entry.child); err != nil {
			return err
		}
	}
	return nil
}

func fixedChildPageIDAt(np *NodePage, idx int) (pagestore.PageID, error) {
	if idx == 0 {
		return np.LeftmostChild(), nil
	}
	_, child, err := np.InternalAt(idx - 1)
	return child, err
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

func fixedFirstKey(np *NodePage) (uint64, bool, error) {
	if np.IsLeaf() {
		if np.NumKeys() == 0 {
			return 0, false, nil
		}
		key, _, err := np.LeafAt(0)
		return key, err == nil, err
	}
	if np.NumKeys() == 0 {
		return 0, false, nil
	}
	key, _, err := np.InternalAt(0)
	return key, err == nil, err
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

	return fixedFirstKey(curr)
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
	leftmost, entries, err := collectInternalEntriesFixed(parentNP)
	if err != nil {
		return err
	}
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
	return rebuildInternalFixed(parentNP, leftmost, entries)
}

//nolint:gocyclo // Fixed-key B-tree underflow repair is a compact structural case analysis with latch ownership.
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
		leftPageID, err := fixedChildPageIDAt(parentNP, childPos-1)
		if err != nil {
			return nil, nil, 0, err
		}
		leftH, err := tr.bp.FetchForWrite(leftPageID)
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
				leftEntries, err := collectLeafEntriesFixed(leftNP)
				if err != nil {
					leftH.Release()
					return nil, nil, 0, err
				}
				childEntries, err := collectLeafEntriesFixed(childNP)
				if err != nil {
					leftH.Release()
					return nil, nil, 0, err
				}
				moved := leftEntries[len(leftEntries)-1]
				leftEntries = leftEntries[:len(leftEntries)-1]
				childEntries = append([]fixedLeafEntry{moved}, childEntries...)

				if err := rebuildLeafFixed(leftNP, leftEntries, leftNP.NextLeafPageID()); err != nil {
					leftH.Release()
					return nil, nil, 0, err
				}
				if err := rebuildLeafFixed(childNP, childEntries, childNP.NextLeafPageID()); err != nil {
					leftH.Release()
					return nil, nil, 0, err
				}
			} else {
				leftLeftmost, leftEntries, err := collectInternalEntriesFixed(leftNP)
				if err != nil {
					leftH.Release()
					return nil, nil, 0, err
				}
				childLeftmost, childEntries, err := collectInternalEntriesFixed(childNP)
				if err != nil {
					leftH.Release()
					return nil, nil, 0, err
				}
				parentSep, _, err := parentNP.InternalAt(childPos - 1)
				if err != nil {
					leftH.Release()
					return nil, nil, 0, err
				}
				movedChild := leftEntries[len(leftEntries)-1].child
				leftEntries = leftEntries[:len(leftEntries)-1]
				childEntries = append([]fixedInternalEntry{{key: parentSep, child: childLeftmost}}, childEntries...)

				if err := rebuildInternalFixed(leftNP, leftLeftmost, leftEntries); err != nil {
					leftH.Release()
					return nil, nil, 0, err
				}
				if err := rebuildInternalFixed(childNP, movedChild, childEntries); err != nil {
					leftH.Release()
					return nil, nil, 0, err
				}
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
		rightPageID, err := fixedChildPageIDAt(parentNP, childPos+1)
		if err != nil {
			return nil, nil, 0, err
		}
		rightH, err := tr.bp.FetchForWrite(rightPageID)
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
				childEntries, err := collectLeafEntriesFixed(childNP)
				if err != nil {
					rightH.Release()
					return nil, nil, 0, err
				}
				rightEntries, err := collectLeafEntriesFixed(rightNP)
				if err != nil {
					rightH.Release()
					return nil, nil, 0, err
				}
				childEntries = append(childEntries, rightEntries[0])
				rightEntries = rightEntries[1:]

				if err := rebuildLeafFixed(childNP, childEntries, childNP.NextLeafPageID()); err != nil {
					rightH.Release()
					return nil, nil, 0, err
				}
				if err := rebuildLeafFixed(rightNP, rightEntries, rightNP.NextLeafPageID()); err != nil {
					rightH.Release()
					return nil, nil, 0, err
				}
			} else {
				childLeftmost, childEntries, err := collectInternalEntriesFixed(childNP)
				if err != nil {
					rightH.Release()
					return nil, nil, 0, err
				}
				rightLeftmost, rightEntries, err := collectInternalEntriesFixed(rightNP)
				if err != nil {
					rightH.Release()
					return nil, nil, 0, err
				}
				parentSep, _, err := parentNP.InternalAt(childPos)
				if err != nil {
					rightH.Release()
					return nil, nil, 0, err
				}

				childEntries = append(childEntries, fixedInternalEntry{key: parentSep, child: rightLeftmost})
				newRightLeftmost := rightEntries[0].child
				rightEntries = rightEntries[1:]

				if err := rebuildInternalFixed(childNP, childLeftmost, childEntries); err != nil {
					rightH.Release()
					return nil, nil, 0, err
				}
				if err := rebuildInternalFixed(rightNP, newRightLeftmost, rightEntries); err != nil {
					rightH.Release()
					return nil, nil, 0, err
				}
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
		leftPageID, err := fixedChildPageIDAt(parentNP, childPos-1)
		if err != nil {
			return nil, nil, 0, err
		}
		leftH, err := tr.bp.FetchForWrite(leftPageID)
		if err != nil {
			return nil, nil, 0, err
		}
		leftNP, err := OpenNodePage(leftH.Page(), tr.maxBodySize, tr.codec.Compare)
		if err != nil {
			leftH.Release()
			return nil, nil, 0, err
		}

		if childNP.IsLeaf() {
			leftEntries, err := collectLeafEntriesFixed(leftNP)
			if err != nil {
				leftH.Release()
				return nil, nil, 0, err
			}
			childEntries, err := collectLeafEntriesFixed(childNP)
			if err != nil {
				leftH.Release()
				return nil, nil, 0, err
			}
			leftEntries = append(leftEntries, childEntries...)
			if err := rebuildLeafFixed(leftNP, leftEntries, childNP.NextLeafPageID()); err != nil {
				leftH.Release()
				return nil, nil, 0, err
			}
		} else {
			leftLeftmost, leftEntries, err := collectInternalEntriesFixed(leftNP)
			if err != nil {
				leftH.Release()
				return nil, nil, 0, err
			}
			childLeftmost, childEntries, err := collectInternalEntriesFixed(childNP)
			if err != nil {
				leftH.Release()
				return nil, nil, 0, err
			}
			parentSep, _, err := parentNP.InternalAt(childPos - 1)
			if err != nil {
				leftH.Release()
				return nil, nil, 0, err
			}
			leftEntries = append(leftEntries, fixedInternalEntry{key: parentSep, child: childLeftmost})
			leftEntries = append(leftEntries, childEntries...)
			if err := rebuildInternalFixed(leftNP, leftLeftmost, leftEntries); err != nil {
				leftH.Release()
				return nil, nil, 0, err
			}
		}

		parentLeftmost, parentEntries, err := collectInternalEntriesFixed(parentNP)
		if err != nil {
			leftH.Release()
			return nil, nil, 0, err
		}
		parentEntries = append(parentEntries[:childPos-1], parentEntries[childPos:]...)
		if err := rebuildInternalFixed(parentNP, parentLeftmost, parentEntries); err != nil {
			leftH.Release()
			return nil, nil, 0, err
		}

		tr.markDirty(leftH)
		tr.markDirty(parentH)
		childH.Release()
		return leftH, leftNP, childPos - 1, nil
	}

	rightPageID, err := fixedChildPageIDAt(parentNP, childPos+1)
	if err != nil {
		return nil, nil, 0, err
	}
	rightH, err := tr.bp.FetchForWrite(rightPageID)
	if err != nil {
		return nil, nil, 0, err
	}
	rightNP, err := OpenNodePage(rightH.Page(), tr.maxBodySize, tr.codec.Compare)
	if err != nil {
		rightH.Release()
		return nil, nil, 0, err
	}

	if childNP.IsLeaf() {
		childEntries, err := collectLeafEntriesFixed(childNP)
		if err != nil {
			rightH.Release()
			return nil, nil, 0, err
		}
		rightEntries, err := collectLeafEntriesFixed(rightNP)
		if err != nil {
			rightH.Release()
			return nil, nil, 0, err
		}
		childEntries = append(childEntries, rightEntries...)
		if err := rebuildLeafFixed(childNP, childEntries, rightNP.NextLeafPageID()); err != nil {
			rightH.Release()
			return nil, nil, 0, err
		}
	} else {
		childLeftmost, childEntries, err := collectInternalEntriesFixed(childNP)
		if err != nil {
			rightH.Release()
			return nil, nil, 0, err
		}
		rightLeftmost, rightEntries, err := collectInternalEntriesFixed(rightNP)
		if err != nil {
			rightH.Release()
			return nil, nil, 0, err
		}
		parentSep, _, err := parentNP.InternalAt(childPos)
		if err != nil {
			rightH.Release()
			return nil, nil, 0, err
		}
		childEntries = append(childEntries, fixedInternalEntry{key: parentSep, child: rightLeftmost})
		childEntries = append(childEntries, rightEntries...)
		if err := rebuildInternalFixed(childNP, childLeftmost, childEntries); err != nil {
			rightH.Release()
			return nil, nil, 0, err
		}
	}

	parentLeftmost, parentEntries, err := collectInternalEntriesFixed(parentNP)
	if err != nil {
		rightH.Release()
		return nil, nil, 0, err
	}
	parentEntries = append(parentEntries[:childPos], parentEntries[childPos+1:]...)
	if err := rebuildInternalFixed(parentNP, parentLeftmost, parentEntries); err != nil {
		rightH.Release()
		return nil, nil, 0, err
	}

	tr.markDirty(childH)
	tr.markDirty(parentH)
	rightH.Release()
	return childH, childNP, childPos, nil
}
