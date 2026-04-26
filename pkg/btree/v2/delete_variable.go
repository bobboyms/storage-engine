package v2

import "github.com/bobboyms/storage-engine/pkg/pagestore"

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

func rebuildLeafVar(vp *VariableNodePage, entries []varLeafEntry, nextLeaf pagestore.PageID) {
	InitLeafPageVar(vp.page, vp.maxBodySize, vp.cmp)
	vp.setNextLeafPageID(nextLeaf)
	for _, entry := range entries {
		if err := vp.LeafInsertVar(entry.key, entry.value); err != nil {
			panic(err)
		}
	}
}

func rebuildInternalVar(vp *VariableNodePage, leftmost pagestore.PageID, entries []varInternalEntry) {
	InitInternalPageVar(vp.page, vp.maxBodySize, leftmost, vp.cmp)
	for _, entry := range entries {
		if err := vp.InsertSeparatorVar(entry.key, entry.child); err != nil {
			panic(err)
		}
	}
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
	rebuildInternalVar(parentVP, leftmost, entries)
	return nil
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

	if childPos > 0 {
		leftH, err := tr.bp.FetchForWrite(varChildPageIDAt(parentVP, childPos-1))
		if err != nil {
			return nil, nil, 0, err
		}
		leftVP, err := OpenVariableNodePage(leftH.Page(), tr.maxBodySize, tr.varCodec.Compare)
		if err != nil {
			leftH.Release()
			return nil, nil, 0, err
		}
		if leftVP.NumKeys() > varMinKeys(leftVP, false) {
			if childVP.IsLeaf() {
				leftEntries := collectLeafEntriesVar(leftVP)
				childEntries := collectLeafEntriesVar(childVP)
				moved := leftEntries[len(leftEntries)-1]
				leftEntries = leftEntries[:len(leftEntries)-1]
				childEntries = append([]varLeafEntry{moved}, childEntries...)
				if !canFitLeafEntriesVar(childVP, childEntries) {
					leftH.Release()
					goto tryRight
				}

				rebuildLeafVar(leftVP, leftEntries, leftVP.NextLeafPageID())
				rebuildLeafVar(childVP, childEntries, childVP.NextLeafPageID())
			} else {
				leftLeftmost, leftEntries := collectInternalEntriesVar(leftVP)
				childLeftmost, childEntries := collectInternalEntriesVar(childVP)
				parentSep, _ := parentVP.InternalAtVar(childPos - 1)
				movedChild := leftEntries[len(leftEntries)-1].child
				leftEntries = leftEntries[:len(leftEntries)-1]
				childEntries = append([]varInternalEntry{{key: cloneBytes(parentSep), child: childLeftmost}}, childEntries...)
				if !canFitInternalEntriesVar(childVP, childEntries) {
					leftH.Release()
					goto tryRight
				}

				rebuildInternalVar(leftVP, leftLeftmost, leftEntries)
				rebuildInternalVar(childVP, movedChild, childEntries)
			}

			tr.markDirty(leftH)
			tr.markDirty(childH)
			tr.markDirty(parentH)
			leftH.Release()
			return childH, childVP, childPos, nil
		}
		leftH.Release()
	}

tryRight:
	if childPos < parentVP.NumKeys() {
		rightH, err := tr.bp.FetchForWrite(varChildPageIDAt(parentVP, childPos+1))
		if err != nil {
			return nil, nil, 0, err
		}
		rightVP, err := OpenVariableNodePage(rightH.Page(), tr.maxBodySize, tr.varCodec.Compare)
		if err != nil {
			rightH.Release()
			return nil, nil, 0, err
		}
		if rightVP.NumKeys() > varMinKeys(rightVP, false) {
			if childVP.IsLeaf() {
				childEntries := collectLeafEntriesVar(childVP)
				rightEntries := collectLeafEntriesVar(rightVP)
				childEntries = append(childEntries, rightEntries[0])
				rightEntries = rightEntries[1:]
				if !canFitLeafEntriesVar(childVP, childEntries) {
					rightH.Release()
					goto tryMerge
				}

				rebuildLeafVar(childVP, childEntries, childVP.NextLeafPageID())
				rebuildLeafVar(rightVP, rightEntries, rightVP.NextLeafPageID())
			} else {
				childLeftmost, childEntries := collectInternalEntriesVar(childVP)
				rightLeftmost, rightEntries := collectInternalEntriesVar(rightVP)
				parentSep, _ := parentVP.InternalAtVar(childPos)

				childEntries = append(childEntries, varInternalEntry{key: cloneBytes(parentSep), child: rightLeftmost})
				newRightLeftmost := rightEntries[0].child
				rightEntries = rightEntries[1:]
				if !canFitInternalEntriesVar(childVP, childEntries) {
					rightH.Release()
					goto tryMerge
				}

				rebuildInternalVar(childVP, childLeftmost, childEntries)
				rebuildInternalVar(rightVP, newRightLeftmost, rightEntries)
			}

			tr.markDirty(childH)
			tr.markDirty(rightH)
			tr.markDirty(parentH)
			rightH.Release()
			return childH, childVP, childPos, nil
		}
		rightH.Release()
	}

tryMerge:
	if childPos > 0 {
		leftH, err := tr.bp.FetchForWrite(varChildPageIDAt(parentVP, childPos-1))
		if err != nil {
			return nil, nil, 0, err
		}
		leftVP, err := OpenVariableNodePage(leftH.Page(), tr.maxBodySize, tr.varCodec.Compare)
		if err != nil {
			leftH.Release()
			return nil, nil, 0, err
		}

		if childVP.IsLeaf() {
			leftEntries := collectLeafEntriesVar(leftVP)
			childEntries := collectLeafEntriesVar(childVP)
			leftEntries = append(leftEntries, childEntries...)
			if !canFitLeafEntriesVar(leftVP, leftEntries) {
				leftH.Release()
				goto mergeRight
			}
			rebuildLeafVar(leftVP, leftEntries, childVP.NextLeafPageID())
		} else {
			leftLeftmost, leftEntries := collectInternalEntriesVar(leftVP)
			childLeftmost, childEntries := collectInternalEntriesVar(childVP)
			parentSep, _ := parentVP.InternalAtVar(childPos - 1)
			leftEntries = append(leftEntries, varInternalEntry{key: cloneBytes(parentSep), child: childLeftmost})
			leftEntries = append(leftEntries, childEntries...)
			if !canFitInternalEntriesVar(leftVP, leftEntries) {
				leftH.Release()
				goto mergeRight
			}
			rebuildInternalVar(leftVP, leftLeftmost, leftEntries)
		}

		parentLeftmost, parentEntries := collectInternalEntriesVar(parentVP)
		parentEntries = append(parentEntries[:childPos-1], parentEntries[childPos:]...)
		rebuildInternalVar(parentVP, parentLeftmost, parentEntries)

		tr.markDirty(leftH)
		tr.markDirty(parentH)
		childH.Release()
		return leftH, leftVP, childPos - 1, nil
	}

mergeRight:
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
		rebuildLeafVar(childVP, childEntries, rightVP.NextLeafPageID())
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
		rebuildInternalVar(childVP, childLeftmost, childEntries)
	}

	parentLeftmost, parentEntries := collectInternalEntriesVar(parentVP)
	parentEntries = append(parentEntries[:childPos], parentEntries[childPos+1:]...)
	rebuildInternalVar(parentVP, parentLeftmost, parentEntries)

	tr.markDirty(childH)
	tr.markDirty(parentH)
	rightH.Release()
	return childH, childVP, childPos, nil
}
