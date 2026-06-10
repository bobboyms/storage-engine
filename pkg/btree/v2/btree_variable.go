package v2

import (
	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// Este arquivo contém os helpers de insert/get/scan específicos pro
// formato variable-key (VarcharKey). Paralelo aos helpers do btree.go
// pra formato fixed-key. BTreeV2 detecta o tipo do codec e chama o
// helper apropriado.
//
// A estrutura geral é idêntica ao fixed, só o layout interno das
// pages muda (VariableNodePage em vez de NodePage).

func (tr *BTreeV2) splitVarNode(h *pagestore.PageHandle, vp *VariableNodePage) (*pagestore.PageHandle, []byte, error) {
	rightH, err := tr.bp.NewPage()
	if err != nil {
		return nil, nil, err
	}

	var sepKey []byte
	if vp.IsLeaf() {
		rightVP := InitLeafPageVar(rightH.Page(), tr.maxBodySize, tr.varCodec.Compare)
		sepKey, err = vp.splitLeafIntoVar(rightVP)
		if err != nil {
			rightH.Release()
			return nil, nil, err
		}
		vp.setNextLeafPageID(rightH.ID())
	} else {
		rightVP := InitInternalPageVar(rightH.Page(), tr.maxBodySize, pagestore.InvalidPageID, tr.varCodec.Compare)
		sepKey, err = vp.splitInternalIntoVar(rightVP)
		if err != nil {
			rightH.Release()
			return nil, nil, err
		}
	}

	tr.markDirty(h)
	tr.markDirty(rightH)
	return rightH, sepKey, nil
}

// predictSplitVarLocked retorna se a subtree enraizada em `vp`
// precisa splitar para concluir a inserção de `key`, e qual seria o
// comprimento exato da key promovida por esse split.
//
// Contrato: `vp` já está com latch exclusivo.
func (tr *BTreeV2) predictSplitVarLocked(vp *VariableNodePage, key []byte) (bool, int, error) {
	if vp.IsLeaf() {
		if vp.CanLeafInsertVar(key) {
			return false, 0, nil
		}
		return true, vp.SplitSeparatorLenVar(), nil
	}

	childPageID := vp.FindChildVar(key)
	childH, err := tr.bp.FetchForWrite(childPageID)
	if err != nil {
		return false, 0, err
	}
	defer childH.Release()

	childVP, err := OpenVariableNodePage(childH.Page(), tr.maxBodySize, tr.varCodec.Compare)
	if err != nil {
		return false, 0, err
	}

	childWillSplit, childPromotedLen, err := tr.predictSplitVarLocked(childVP, key)
	if err != nil {
		return false, 0, err
	}
	if !childWillSplit {
		return false, 0, nil
	}
	if vp.CanAbsorbSeparatorVar(childPromotedLen) {
		return false, 0, nil
	}
	return true, vp.SplitSeparatorLenVar(), nil
}

func (tr *BTreeV2) ensureRootSafeForInsertVar(key []byte) (*pagestore.PageHandle, *VariableNodePage, error) {
	tr.metaMu.Lock()

	rootPageID := tr.rootPageID
	rootH, err := tr.bp.FetchForWrite(rootPageID)
	if err != nil {
		tr.metaMu.Unlock()
		return nil, nil, err
	}

	rootVP, err := OpenVariableNodePage(rootH.Page(), tr.maxBodySize, tr.varCodec.Compare)
	if err != nil {
		rootH.Release()
		tr.metaMu.Unlock()
		return nil, nil, err
	}

	rootWillSplit, _, err := tr.predictSplitVarLocked(rootVP, key)
	if err != nil {
		rootH.Release()
		tr.metaMu.Unlock()
		return nil, nil, err
	}
	if !rootWillSplit {
		tr.metaMu.Unlock()
		return rootH, rootVP, nil
	}

	ntaID, err := tr.beginStructural(NTAKindBTreeSplit, rootH)
	if err != nil {
		rootH.Release()
		tr.metaMu.Unlock()
		return nil, nil, err
	}

	newRootH, err := tr.bp.NewPage()
	if err != nil {
		rootH.Release()
		tr.metaMu.Unlock()
		return nil, nil, err
	}

	rightH, sepKey, err := tr.splitVarNode(rootH, rootVP)
	if err != nil {
		newRootH.Release()
		rootH.Release()
		tr.metaMu.Unlock()
		return nil, nil, err
	}

	_ = InitInternalPageVar(newRootH.Page(), tr.maxBodySize, rootH.ID(), tr.varCodec.Compare)
	newRootVP, _ := OpenVariableNodePage(newRootH.Page(), tr.maxBodySize, tr.varCodec.Compare)
	if err := newRootVP.InsertSeparatorVar(sepKey, rightH.ID()); err != nil {
		rightH.Release()
		newRootH.Release()
		rootH.Release()
		tr.metaMu.Unlock()
		return nil, nil, err
	}

	tr.markDirty(rootH)
	tr.markDirty(rightH)
	tr.markDirty(newRootH)

	newRootPageID := newRootH.ID()
	if err := tr.updateRootLocked(newRootPageID); err != nil {
		rightH.Release()
		newRootH.Release()
		rootH.Release()
		tr.metaMu.Unlock()
		return nil, nil, err
	}

	if err := tr.commitStructural(ntaID); err != nil {
		rightH.Release()
		newRootH.Release()
		rootH.Release()
		tr.metaMu.Unlock()
		return nil, nil, err
	}

	rightH.Release()
	rootH.Release()
	tr.metaMu.Unlock()
	return newRootH, newRootVP, nil
}

func (tr *BTreeV2) splitChildAndChooseVar(
	parentH *pagestore.PageHandle,
	parentVP *VariableNodePage,
	childH *pagestore.PageHandle,
	childVP *VariableNodePage,
	key []byte,
) (*pagestore.PageHandle, *VariableNodePage, error) {
	ntaID, err := tr.beginStructural(NTAKindBTreeSplit, parentH, childH)
	if err != nil {
		childH.Release()
		return nil, nil, err
	}

	rightH, sepKey, err := tr.splitVarNode(childH, childVP)
	if err != nil {
		childH.Release()
		return nil, nil, err
	}

	if err := parentVP.InsertSeparatorVar(sepKey, rightH.ID()); err != nil {
		rightH.Release()
		childH.Release()
		return nil, nil, err
	}
	tr.markDirty(parentH)

	if err := tr.commitStructural(ntaID); err != nil {
		rightH.Release()
		childH.Release()
		return nil, nil, err
	}

	if tr.varCodec.Compare(key, sepKey) < 0 {
		rightH.Release()
		return childH, childVP, nil
	}

	rightVP, err := OpenVariableNodePage(rightH.Page(), tr.maxBodySize, tr.varCodec.Compare)
	if err != nil {
		rightH.Release()
		childH.Release()
		return nil, nil, err
	}
	childH.Release()
	return rightH, rightVP, nil
}

func (tr *BTreeV2) descendToLeafForInsertVar(key []byte) (*pagestore.PageHandle, *VariableNodePage, error) {
	currH, currVP, err := tr.ensureRootSafeForInsertVar(key)
	if err != nil {
		return nil, nil, err
	}

	for !currVP.IsLeaf() {
		childPageID := currVP.FindChildVar(key)
		childH, err := tr.bp.FetchForWrite(childPageID)
		if err != nil {
			currH.Release()
			return nil, nil, err
		}

		childVP, err := OpenVariableNodePage(childH.Page(), tr.maxBodySize, tr.varCodec.Compare)
		if err != nil {
			childH.Release()
			currH.Release()
			return nil, nil, err
		}

		childWillSplit, _, err := tr.predictSplitVarLocked(childVP, key)
		if err != nil {
			childH.Release()
			currH.Release()
			return nil, nil, err
		}
		if childWillSplit {
			nextH, nextVP, err := tr.splitChildAndChooseVar(currH, currVP, childH, childVP, key)
			currH.Release()
			if err != nil {
				return nil, nil, err
			}
			currH = nextH
			currVP = nextVP
			continue
		}

		currH.Release()
		currH = childH
		currVP = childVP
	}

	return currH, currVP, nil
}

func (tr *BTreeV2) insertCrabbingVar(key []byte, value int64) error {
	leafH, leafVP, err := tr.descendToLeafForInsertVar(key)
	if err != nil {
		return err
	}
	defer leafH.Release()

	if err := leafVP.LeafInsertVar(key, value); err != nil {
		return err
	}
	tr.markDirty(leafH)
	return nil
}

func (tr *BTreeV2) removeCrabbingVar(encKey []byte) (bool, error) {
	rootH, err := tr.bp.FetchForWrite(tr.rootPage())
	if err != nil {
		return false, err
	}
	defer rootH.Release()

	rootVP, err := OpenVariableNodePage(rootH.Page(), tr.maxBodySize, tr.varCodec.Compare)
	if err != nil {
		return false, err
	}

	handles := []*pagestore.PageHandle{rootH}
	nodes := []*VariableNodePage{rootVP}
	childPositions := []int{-1}

	currH := rootH
	currVP := rootVP
	for !currVP.IsLeaf() {
		childPos := currVP.internalBinarySearchVar(encKey)
		childH, err := tr.bp.FetchForWrite(varChildPageIDAt(currVP, childPos))
		if err != nil {
			return false, err
		}
		childVP, err := OpenVariableNodePage(childH.Page(), tr.maxBodySize, tr.varCodec.Compare)
		if err != nil {
			childH.Release()
			return false, err
		}

		handles = append(handles, childH)
		nodes = append(nodes, childVP)
		childPositions = append(childPositions, childPos)
		currH = childH
		currVP = childVP
	}

	removed, err := currVP.LeafDeleteVar(encKey)
	if err != nil || !removed {
		for i := len(handles) - 1; i >= 1; i-- {
			handles[i].Release()
		}
		return removed, err
	}
	tr.markDirty(currH)

	held := make(map[pagestore.PageID]*VariableNodePage, len(nodes))
	for i, h := range handles {
		held[h.ID()] = nodes[i]
	}

	for level := len(nodes) - 2; level >= 0; level-- {
		parentH := handles[level]
		parentVP := nodes[level]
		childH := handles[level+1]
		childVP := nodes[level+1]
		childPos := childPositions[level+1]

		childH, childVP, childPos, err = tr.fixChildUnderflowVar(parentH, parentVP, childH, childVP, childPos)
		if err != nil {
			for i := len(handles) - 1; i >= 1; i-- {
				if handles[i] != nil {
					handles[i].Release()
				}
			}
			return false, err
		}

		handles[level+1] = childH
		nodes[level+1] = childVP
		childPositions[level+1] = childPos
		held = make(map[pagestore.PageID]*VariableNodePage, level+2)
		for i := 0; i <= level+1; i++ {
			if handles[i] != nil {
				held[handles[i].ID()] = nodes[i]
			}
		}

		if !parentVP.IsLeaf() {
			if err := tr.refreshInternalSeparatorsVar(parentVP, held); err != nil {
				for i := len(handles) - 1; i >= 1; i-- {
					if handles[i] != nil {
						handles[i].Release()
					}
				}
				return false, err
			}
			tr.markDirty(parentH)
		}

		childH.Release()
		handles[level+1] = nil
		delete(held, childH.ID())
	}

	if !rootVP.IsLeaf() && rootVP.NumKeys() == 0 {
		if err := tr.updateRoot(rootVP.LeftmostChild()); err != nil {
			return false, err
		}
	}

	return true, nil
}

// getLockedVar lê a tree variável com snapshot rápido do rootPageID
// + latch crabbing de read entre pages.
func (tr *BTreeV2) getLockedVar(encKey []byte) (int64, bool, error) {
	h, err := tr.descendReadLeafVar(encKey, false)
	if err != nil {
		return 0, false, err
	}
	defer h.Release()
	vp, err := OpenVariableNodePage(h.Page(), tr.maxBodySize, tr.varCodec.Compare)
	if err != nil {
		return 0, false, err
	}
	v, found := vp.LeafGetVar(encKey)
	return v, found, nil
}

// descendReadLeafVar is the variable-key counterpart of descendReadLeafFixed:
// a crabbing (hand-over-hand) read descent that returns the target leaf with
// its read latch still held. The caller owns the handle and MUST Release it.
// See descendReadLeafFixed for why pinning the child before releasing the
// parent is required for correctness under concurrent splits/merges.
func (tr *BTreeV2) descendReadLeafVar(encKey []byte, leftmost bool) (*pagestore.PageHandle, error) {
	h, err := tr.bp.Fetch(tr.rootPage())
	if err != nil {
		return nil, err
	}
	for {
		vp, err := OpenVariableNodePage(h.Page(), tr.maxBodySize, tr.varCodec.Compare)
		if err != nil {
			h.Release()
			return nil, err
		}
		if vp.IsLeaf() {
			return h, nil
		}
		var nextPageID pagestore.PageID
		if leftmost {
			nextPageID = vp.LeftmostChild()
		} else {
			nextPageID = vp.FindChildVar(encKey)
		}
		childH, err := tr.bp.Fetch(nextPageID) // pin child BEFORE releasing parent
		if err != nil {
			h.Release()
			return nil, err
		}
		h.Release()
		if tr.onDescendChild != nil {
			tr.onDescendChild()
		}
		h = childH
	}
}
