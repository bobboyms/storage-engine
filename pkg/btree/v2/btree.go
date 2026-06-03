package v2

import (
	"fmt"
	"sync"

	"github.com/bobboyms/storage-engine/pkg/btree"
	"github.com/bobboyms/storage-engine/pkg/crypto"
	"github.com/bobboyms/storage-engine/pkg/pagestore"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// Compile-time assertion: *BTreeV2 satisfaz btree.Tree.
// Se assinaturas divergirem, o build quebra imediatamente.
var _ btree.Tree = (*BTreeV2)(nil)

// metaPageID é a page onde guardamos o header da tree.
// Reservamos pageID=1 pro meta; a primeira folha aloca a partir de 2.
const metaPageID pagestore.PageID = 1

// treeMeta é o "catálogo" da tree persistida. Grava na metaPageID.
type treeMeta struct {
	magic       uint32
	version     uint16
	rootPageID  pagestore.PageID
	numKeysHint uint64 // pista (not confiável pós-crash), informativa
}

const (
	treeMetaMagic   = 0x4254524B // ASCII "BTRK"
	treeMetaVersion = 1
)

func (m *treeMeta) encode(buf []byte) {
	binEncU32(buf[0:4], m.magic)
	binEncU16(buf[4:6], m.version)
	binEncU64(buf[6:14], uint64(m.rootPageID))
	binEncU64(buf[14:22], m.numKeysHint)
}

func (m *treeMeta) decode(buf []byte) error {
	m.magic = binDecU32(buf[0:4])
	if m.magic != treeMetaMagic {
		return fmt.Errorf("btree/v2: meta page has invalid magic %x", m.magic)
	}
	m.version = binDecU16(buf[4:6])
	if m.version != treeMetaVersion {
		return fmt.Errorf("btree/v2: meta version %d is not supported", m.version)
	}
	m.rootPageID = pagestore.PageID(binDecU64(buf[6:14]))
	m.numKeysHint = binDecU64(buf[14:22])
	return nil
}

// BTreeV2 é a B+ tree page-based tipada por um KeyCodec.
//
// Concorrência:
//   - Get/Scan usam latch crabbing de read entre pages (sem mutex
//     global por operação).
//   - Fixed-key writers usam latch crabbing top-down com split
//     preventivo: seguram parent+child apenas até garantir que o child
//     está "safe" e então soltam o parent.
//   - Variable-key writers usam o mesmo protocolo top-down, com
//     previsão recursiva do split real pra calcular o separador
//     promovido.
//   - Delete faz rebalance estrutural (borrow/merge + root collapse).
type BTreeV2 struct {
	pf          *pagestore.PageFile
	bp          *pagestore.BufferPool
	maxBodySize int

	// Exatamente UM de codec/varCodec é not-nil. `isVariable` disambigua
	// de forma rápida sem type assertion.
	codec      KeyCodec         // fixed (8-byte keys)
	varCodec   VariableKeyCodec // variable (byte-slice keys, VarcharKey)
	isVariable bool

	// metaMu protege rootPageID e updates na metaPage.
	// Leituras pegam um snapshot rápido do root; splits de root serializam aqui.
	metaMu sync.RWMutex

	rootPageID pagestore.PageID

	writeMu            sync.Mutex
	currentMutationLSN uint64

	// structural is the nested-top-action logger used to bracket
	// multi-page mutations (splits, merges) with WAL Begin/Commit
	// records and capture before-images for crash recovery. nil means
	// no structural logging (e.g. unit tests that exercise the tree
	// in isolation without a WAL).
	structural StructuralLogger
}

// StructuralLogger mirrors pkg/storage.StructuralLogger so the B+ tree
// can call into the engine without importing it.
type StructuralLogger interface {
	BeginNTA(kind uint8, pages []NTAPage) (uint64, error)
	CommitNTA(ntaID uint64) error
}

// NTAPage carries the before-image of a page involved in a structural
// op so crash recovery can restore the pre-NTA state when the Commit
// record never made it to disk.
type NTAPage struct {
	Path     string
	PageID   pagestore.PageID
	PreImage []byte
}

// Structural op kinds. Kept in sync with pkg/storage/nta.go.
const (
	NTAKindBTreeSplit uint8 = 1
	NTAKindBTreeMerge uint8 = 2
)

// SetStructuralLogger plugs in a logger that the tree uses to bracket
// structural ops. Default is no-op; the storage engine wires this
// during NewStorageEngine.
func (tr *BTreeV2) SetStructuralLogger(l StructuralLogger) {
	tr.structural = l
}

// snapshotPageImage returns a copy of the on-page bytes. Used to
// capture before-images for NTA Begin records.
func snapshotPageImage(page *pagestore.Page) []byte {
	out := make([]byte, pagestore.PageSize)
	copy(out, page[:])
	return out
}

// beginStructural emits a Begin entry capturing the before-images of
// the given handles. Returns the NTA id (0 if structural logging is
// disabled). beforeImages are snapshotted under the caller's latches.
//
//nolint:unparam // kind will diversify when merge NTAs are wired up
func (tr *BTreeV2) beginStructural(kind uint8, handles ...*pagestore.PageHandle) (uint64, error) {
	if tr.structural == nil {
		return 0, nil
	}
	pages := make([]NTAPage, 0, len(handles))
	for _, h := range handles {
		if h == nil {
			continue
		}
		pages = append(pages, NTAPage{
			Path:     tr.pf.Path(),
			PageID:   h.ID(),
			PreImage: snapshotPageImage(h.Page()),
		})
	}
	return tr.structural.BeginNTA(kind, pages)
}

func (tr *BTreeV2) commitStructural(ntaID uint64) error {
	if tr.structural == nil || ntaID == 0 {
		return nil
	}
	return tr.structural.CommitNTA(ntaID)
}

// NewBTreeV2 abre ou cria uma B+ tree page-based em `path` com IntKeyCodec
// (keys = IntKey). Compatível com a API antiga.
func NewBTreeV2(path string, bufferPoolCapacity int, cipher crypto.Cipher) (*BTreeV2, error) {
	return NewBTreeV2Typed(path, bufferPoolCapacity, cipher, IntKeyCodec{})
}

// NewBTreeV2Typed é a forma geral pra keys de tamanho fixo (IntKey,
// FloatKey, BoolKey, DateKey). Pra VarcharKey use NewBTreeV2Varchar.
func NewBTreeV2Typed(path string, bufferPoolCapacity int, cipher crypto.Cipher, codec KeyCodec) (*BTreeV2, error) {
	if codec == nil {
		return nil, fmt.Errorf("btree/v2: codec obrigatório")
	}

	pf, err := pagestore.NewPageFile(path, cipher)
	if err != nil {
		return nil, err
	}

	tr := &BTreeV2{
		pf:          pf,
		bp:          pagestore.NewBufferPool(pf, bufferPoolCapacity),
		maxBodySize: pf.UsableBodySize(),
		codec:       codec,
	}

	if err := tr.loadOrInitMeta(); err != nil {
		_ = pf.Close()
		return nil, err
	}

	return tr, nil
}

// NewBTreeV2Varchar abre/cria uma B+ tree page-based pra VarcharKey
// (ou qualquer outra key de tamanho variável). Usa layout de slots
// com indireção (keyOffset+keyLength) — ver variable_node_page.go.
//
// Trade-off: 12 bytes por slot + bytes da key, em vez dos 16 bytes
// fixos do formato de 8-byte keys. Com keys curtas (~8 bytes) é
// equivalente; keys muito longas ocupam mais.
func NewBTreeV2Varchar(path string, bufferPoolCapacity int, cipher crypto.Cipher, varCodec VariableKeyCodec) (*BTreeV2, error) {
	if varCodec == nil {
		return nil, fmt.Errorf("btree/v2: varCodec obrigatório")
	}

	pf, err := pagestore.NewPageFile(path, cipher)
	if err != nil {
		return nil, err
	}

	tr := &BTreeV2{
		pf:          pf,
		bp:          pagestore.NewBufferPool(pf, bufferPoolCapacity),
		maxBodySize: pf.UsableBodySize(),
		varCodec:    varCodec,
		isVariable:  true,
	}

	if err := tr.loadOrInitMeta(); err != nil {
		_ = pf.Close()
		return nil, err
	}

	return tr, nil
}

// Close flusha o buffer pool e fecha o page file.
func (tr *BTreeV2) Close() error {
	if err := tr.bp.Close(); err != nil {
		return err
	}
	return tr.pf.Close()
}

// Sync flusha pages sujas sem fechar a tree.
func (tr *BTreeV2) Sync() error {
	return tr.bp.FlushAll()
}

// Path devolve o caminho do arquivo.
func (tr *BTreeV2) Path() string { return tr.pf.Path() }

// WritePageBytes writes `data` (exactly PageSize bytes) to `pageID` on
// disk, bypassing the buffer pool. Used by recovery to restore NTA
// before-images of structural page mutations.
func (tr *BTreeV2) WritePageBytes(pageID pagestore.PageID, data []byte) error {
	if len(data) != pagestore.PageSize {
		return fmt.Errorf("btree/v2: WritePageBytes expects %d bytes, got %d", pagestore.PageSize, len(data))
	}
	var p pagestore.Page
	copy(p[:], data)
	return tr.pf.WritePage(pageID, &p)
}

func (tr *BTreeV2) SetBeforeFlushHook(hook func(pageID pagestore.PageID, page *pagestore.Page) error) {
	tr.bp.SetBeforeFlushHook(hook)
}

func (tr *BTreeV2) DirtyPages() []pagestore.DirtyPageInfo {
	return tr.bp.DirtyPages()
}

// BufferPoolStats exposes the cache hit/miss/eviction counters of this
// tree's buffer pool.
func (tr *BTreeV2) BufferPoolStats() pagestore.BufferPoolStats {
	return tr.bp.Stats()
}

func (tr *BTreeV2) ApplyPageRedo(pageID pagestore.PageID, page *pagestore.Page, lsn uint64) (bool, error) {
	current, err := tr.pf.ReadPage(pageID)
	if err == nil {
		hdr, hdrErr := current.GetHeader()
		if hdrErr == nil && hdr.PageLSN >= lsn {
			tr.bp.ReplacePageImage(pageID, current)
			return false, nil
		}
	}
	if err := tr.pf.WritePage(pageID, page); err != nil {
		return false, err
	}
	tr.bp.ReplacePageImage(pageID, page)
	return true, nil
}

func (tr *BTreeV2) withMutationLSN(lsn uint64, fn func() error) error {
	tr.writeMu.Lock()
	tr.currentMutationLSN = lsn
	defer func() {
		tr.currentMutationLSN = 0
		tr.writeMu.Unlock()
	}()
	return fn()
}

func (tr *BTreeV2) markDirty(h *pagestore.PageHandle) {
	if h == nil {
		return
	}
	if tr.currentMutationLSN > 0 {
		h.Page().AdvancePageLSN(tr.currentMutationLSN)
	}
	h.MarkDirty()
}

func (tr *BTreeV2) InsertWithLSN(key types.Comparable, value int64, lsn uint64) error {
	return tr.withMutationLSN(lsn, func() error {
		if tr.isVariable {
			encKey, err := tr.varCodec.Encode(key)
			if err != nil {
				return err
			}
			return tr.insertCrabbingVar(encKey, value)
		}
		encKey, err := tr.codec.Encode(key)
		if err != nil {
			return err
		}
		return tr.insertCrabbingFixed(encKey, value)
	})
}

func (tr *BTreeV2) UpsertWithLSN(key types.Comparable, lsn uint64, fn func(oldValue int64, exists bool) (int64, error)) error {
	return tr.withMutationLSN(lsn, func() error {
		if tr.isVariable {
			encKey, err := tr.varCodec.Encode(key)
			if err != nil {
				return err
			}
			leafH, leafVP, err := tr.descendToLeafForInsertVar(encKey)
			if err != nil {
				return err
			}
			defer leafH.Release()

			oldValue, exists := leafVP.LeafGetVar(encKey)
			newValue, err := fn(oldValue, exists)
			if err != nil {
				return err
			}
			if err := leafVP.LeafInsertVar(encKey, newValue); err != nil {
				return err
			}
			tr.markDirty(leafH)
			return nil
		}

		encKey, err := tr.codec.Encode(key)
		if err != nil {
			return err
		}
		leafH, leafNP, err := tr.descendToLeafForInsertFixed(encKey)
		if err != nil {
			return err
		}
		defer leafH.Release()

		oldValue, exists := leafNP.LeafGet(encKey)
		newValue, err := fn(oldValue, exists)
		if err != nil {
			return err
		}

		if err := leafNP.LeafInsert(encKey, newValue); err != nil {
			return err
		}
		tr.markDirty(leafH)
		return nil
	})
}

func (tr *BTreeV2) ReplaceWithLSN(key types.Comparable, value int64, lsn uint64) error {
	return tr.UpsertWithLSN(key, lsn, func(int64, bool) (int64, error) { return value, nil })
}

func (tr *BTreeV2) DeleteWithLSN(key types.Comparable, lsn uint64) (bool, error) {
	var (
		found bool
		err   error
	)
	err = tr.withMutationLSN(lsn, func() error {
		if tr.isVariable {
			encKey, encErr := tr.varCodec.Encode(key)
			if encErr != nil {
				return encErr
			}
			found, err = tr.removeCrabbingVar(encKey)
			return err
		}
		encKey, encErr := tr.codec.Encode(key)
		if encErr != nil {
			return encErr
		}
		found, err = tr.removeCrabbingFixed(encKey)
		return err
	})
	return found, err
}

// loadOrInitMeta lê a meta page. Se a tree é nova, cria meta + folha raiz.
func (tr *BTreeV2) loadOrInitMeta() error {
	if tr.pf.NumPages() <= 1 {
		return tr.initFreshTree()
	}

	metaHandle, err := tr.bp.Fetch(metaPageID)
	if err != nil {
		return fmt.Errorf("btree/v2: failed to ler meta: %w", err)
	}
	defer metaHandle.Release()

	var m treeMeta
	if err := m.decode(metaHandle.Page().Body()); err != nil {
		return err
	}
	tr.metaMu.Lock()
	tr.rootPageID = m.rootPageID
	tr.metaMu.Unlock()
	return nil
}

func (tr *BTreeV2) initFreshTree() error {
	metaH, err := tr.bp.NewPage()
	if err != nil {
		return err
	}
	if metaH.ID() != metaPageID {
		metaH.Release()
		return fmt.Errorf("btree/v2: expected metaPageID=%d, got %d", metaPageID, metaH.ID())
	}

	rootH, err := tr.bp.NewPage()
	if err != nil {
		metaH.Release()
		return err
	}
	if tr.isVariable {
		_ = InitLeafPageVar(rootH.Page(), tr.maxBodySize, tr.varCodec.Compare)
	} else {
		_ = InitLeafPage(rootH.Page(), tr.maxBodySize, tr.codec.Compare)
	}
	tr.markDirty(rootH)
	rootPageID := rootH.ID()
	rootH.Release()

	m := treeMeta{
		magic:      treeMetaMagic,
		version:    treeMetaVersion,
		rootPageID: rootPageID,
	}
	m.encode(metaH.Page().Body())
	tr.markDirty(metaH)
	metaH.Release()

	tr.metaMu.Lock()
	tr.rootPageID = rootPageID
	tr.metaMu.Unlock()
	return tr.bp.FlushAll()
}

// Insert coloca (key, value) na tree. Sobrescreve value se key exists.
// Pode causar splits propagando até criar novo root.
func (tr *BTreeV2) Insert(key types.Comparable, value int64) error {
	return tr.InsertWithLSN(key, value, 0)
}

// Upsert é insert + callback. Se key exists, `fn(oldValue, true)`;
// otherwise `fn(0, false)`. O value retornado por fn é gravado.
// Essencial pro engine MVCC (engine.go usa Upsert pra chain de versões).
func (tr *BTreeV2) Upsert(key types.Comparable, fn func(oldValue int64, exists bool) (int64, error)) error {
	return tr.UpsertWithLSN(key, 0, fn)
}

// Replace sobrescreve unconditionally. Equivalente a Upsert(k, func(_,_) (v, nil)).
func (tr *BTreeV2) Replace(key types.Comparable, value int64) error {
	return tr.ReplaceWithLSN(key, value, 0)
}

// Remove apaga fisicamente `key` do index com rebalance top-down:
// trata underflow com borrow/merge e colapsa a raiz quando necessário.
func (tr *BTreeV2) Remove(key types.Comparable) (bool, error) {
	return tr.DeleteWithLSN(key, 0)
}

// Delete é alias mais explícito para Remove.
func (tr *BTreeV2) Delete(key types.Comparable) (bool, error) {
	return tr.Remove(key)
}

func (tr *BTreeV2) rootPage() pagestore.PageID {
	tr.metaMu.RLock()
	defer tr.metaMu.RUnlock()
	return tr.rootPageID
}

func (tr *BTreeV2) nodeIsFull(np *NodePage) bool {
	if np.IsLeaf() {
		return np.NumKeys() >= np.MaxLeafSlots()
	}
	return np.NumKeys() >= np.MaxInternalSlots()
}

func (tr *BTreeV2) ensureRootSafeForInsertFixed() (*pagestore.PageHandle, *NodePage, error) {
	tr.metaMu.Lock()

	rootPageID := tr.rootPageID
	rootH, err := tr.bp.FetchForWrite(rootPageID)
	if err != nil {
		tr.metaMu.Unlock()
		return nil, nil, err
	}

	rootNP, err := OpenNodePage(rootH.Page(), tr.maxBodySize, tr.codec.Compare)
	if err != nil {
		rootH.Release()
		tr.metaMu.Unlock()
		return nil, nil, err
	}

	if !tr.nodeIsFull(rootNP) {
		tr.metaMu.Unlock()
		return rootH, rootNP, nil
	}

	// ARIES nested top action: capture the pre-split image of the old
	// root before any in-memory mutation. The new root and new right
	// are freshly allocated pages — their "before state" is
	// equivalent to "not yet linked", so we do not log them.
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

	rightH, sepKey, err := tr.splitFixedNode(rootH, rootNP)
	if err != nil {
		newRootH.Release()
		rootH.Release()
		tr.metaMu.Unlock()
		return nil, nil, err
	}

	_ = InitInternalPage(newRootH.Page(), tr.maxBodySize, rootH.ID(), tr.codec.Compare)
	newRootNP, _ := OpenNodePage(newRootH.Page(), tr.maxBodySize, tr.codec.Compare)
	if err := newRootNP.InsertSeparator(sepKey, rightH.ID()); err != nil {
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
	return newRootH, newRootNP, nil
}

func (tr *BTreeV2) splitFixedNode(h *pagestore.PageHandle, np *NodePage) (*pagestore.PageHandle, uint64, error) {
	rightH, err := tr.bp.NewPage()
	if err != nil {
		return nil, 0, err
	}

	var sepKey uint64
	if np.IsLeaf() {
		rightNP := InitLeafPage(rightH.Page(), tr.maxBodySize, tr.codec.Compare)
		sepKey, err = np.splitLeafInto(rightNP)
		if err != nil {
			rightH.Release()
			return nil, 0, err
		}
		np.setNextLeafPageID(rightH.ID())
	} else {
		rightNP := InitInternalPage(rightH.Page(), tr.maxBodySize, pagestore.InvalidPageID, tr.codec.Compare)
		sepKey, err = np.splitInternalInto(rightNP)
		if err != nil {
			rightH.Release()
			return nil, 0, err
		}
	}

	tr.markDirty(h)
	tr.markDirty(rightH)
	return rightH, sepKey, nil
}

func (tr *BTreeV2) splitChildAndChooseFixed(
	parentH *pagestore.PageHandle,
	parentNP *NodePage,
	childH *pagestore.PageHandle,
	childNP *NodePage,
	key uint64,
) (*pagestore.PageHandle, *NodePage, error) {
	// ARIES NTA: capture before-images of parent + child. New right
	// leaf is fresh and does not need a pre-image.
	ntaID, err := tr.beginStructural(NTAKindBTreeSplit, parentH, childH)
	if err != nil {
		childH.Release()
		return nil, nil, err
	}

	rightH, sepKey, err := tr.splitFixedNode(childH, childNP)
	if err != nil {
		childH.Release()
		return nil, nil, err
	}

	if err := parentNP.InsertSeparator(sepKey, rightH.ID()); err != nil {
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

	if tr.codec.Compare(key, sepKey) < 0 {
		rightH.Release()
		return childH, childNP, nil
	}

	rightNP, err := OpenNodePage(rightH.Page(), tr.maxBodySize, tr.codec.Compare)
	if err != nil {
		rightH.Release()
		childH.Release()
		return nil, nil, err
	}
	childH.Release()
	return rightH, rightNP, nil
}

func (tr *BTreeV2) descendToLeafForInsertFixed(encKey uint64) (*pagestore.PageHandle, *NodePage, error) {
	currH, currNP, err := tr.ensureRootSafeForInsertFixed()
	if err != nil {
		return nil, nil, err
	}

	for !currNP.IsLeaf() {
		childPageID, err := currNP.FindChild(encKey)
		if err != nil {
			currH.Release()
			return nil, nil, err
		}
		childH, err := tr.bp.FetchForWrite(childPageID)
		if err != nil {
			currH.Release()
			return nil, nil, err
		}

		childNP, err := OpenNodePage(childH.Page(), tr.maxBodySize, tr.codec.Compare)
		if err != nil {
			childH.Release()
			currH.Release()
			return nil, nil, err
		}

		if tr.nodeIsFull(childNP) {
			nextH, nextNP, err := tr.splitChildAndChooseFixed(currH, currNP, childH, childNP, encKey)
			currH.Release()
			if err != nil {
				return nil, nil, err
			}
			currH = nextH
			currNP = nextNP
			continue
		}

		currH.Release()
		currH = childH
		currNP = childNP
	}

	return currH, currNP, nil
}

func (tr *BTreeV2) insertCrabbingFixed(encKey uint64, value int64) error {
	leafH, leafNP, err := tr.descendToLeafForInsertFixed(encKey)
	if err != nil {
		return err
	}
	defer leafH.Release()

	if err := leafNP.LeafInsert(encKey, value); err != nil {
		return err
	}
	tr.markDirty(leafH)
	return nil
}

func (tr *BTreeV2) removeCrabbingFixed(encKey uint64) (bool, error) {
	rootPageID := tr.rootPage()
	rootH, err := tr.bp.FetchForWrite(rootPageID)
	if err != nil {
		return false, err
	}
	defer rootH.Release()

	rootNP, err := OpenNodePage(rootH.Page(), tr.maxBodySize, tr.codec.Compare)
	if err != nil {
		return false, err
	}

	handles := []*pagestore.PageHandle{rootH}
	nodes := []*NodePage{rootNP}
	childPositions := []int{-1}

	currH := rootH
	currNP := rootNP
	for !currNP.IsLeaf() {
		childPos := currNP.internalBinarySearch(encKey)
		childPageID, err := fixedChildPageIDAt(currNP, childPos)
		if err != nil {
			releaseDescendantHandles(handles)
			return false, err
		}
		childH, err := tr.bp.FetchForWrite(childPageID)
		if err != nil {
			releaseDescendantHandles(handles)
			return false, err
		}
		childNP, err := OpenNodePage(childH.Page(), tr.maxBodySize, tr.codec.Compare)
		if err != nil {
			childH.Release()
			releaseDescendantHandles(handles)
			return false, err
		}

		handles = append(handles, childH)
		nodes = append(nodes, childNP)
		childPositions = append(childPositions, childPos)
		currH = childH
		currNP = childNP
	}

	removed, err := currNP.LeafDelete(encKey)
	if err != nil || !removed {
		for i := len(handles) - 1; i >= 1; i-- {
			handles[i].Release()
		}
		return removed, err
	}
	tr.markDirty(currH)

	held := make(map[pagestore.PageID]*NodePage, len(nodes))
	for i, h := range handles {
		held[h.ID()] = nodes[i]
	}

	for level := len(nodes) - 2; level >= 0; level-- {
		parentH := handles[level]
		parentNP := nodes[level]
		childH := handles[level+1]
		childNP := nodes[level+1]
		childPos := childPositions[level+1]

		childH, childNP, childPos, err = tr.fixChildUnderflowFixed(parentH, parentNP, childH, childNP, childPos)
		if err != nil {
			for i := len(handles) - 1; i >= 1; i-- {
				if handles[i] != nil {
					handles[i].Release()
				}
			}
			return false, err
		}

		handles[level+1] = childH
		nodes[level+1] = childNP
		childPositions[level+1] = childPos
		held = make(map[pagestore.PageID]*NodePage, level+2)
		for i := 0; i <= level+1; i++ {
			if handles[i] != nil {
				held[handles[i].ID()] = nodes[i]
			}
		}

		if !parentNP.IsLeaf() {
			if err := tr.refreshInternalSeparatorsFixed(parentNP, held); err != nil {
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

	if !rootNP.IsLeaf() && rootNP.NumKeys() == 0 {
		if err := tr.updateRoot(rootNP.LeftmostChild()); err != nil {
			return false, err
		}
	}

	return true, nil
}

func releaseDescendantHandles(handles []*pagestore.PageHandle) {
	for i := len(handles) - 1; i >= 1; i-- {
		if handles[i] != nil {
			handles[i].Release()
		}
	}
}

func (tr *BTreeV2) updateRoot(newRootPageID pagestore.PageID) error {
	tr.metaMu.Lock()
	defer tr.metaMu.Unlock()
	return tr.updateRootLocked(newRootPageID)
}

func (tr *BTreeV2) updateRootLocked(newRootPageID pagestore.PageID) error {
	metaH, err := tr.bp.FetchForWrite(metaPageID)
	if err != nil {
		return err
	}
	defer metaH.Release()

	var m treeMeta
	if err := m.decode(metaH.Page().Body()); err != nil {
		return err
	}
	m.rootPageID = newRootPageID
	m.encode(metaH.Page().Body())
	tr.markDirty(metaH)

	tr.rootPageID = newRootPageID
	return nil
}

// ScanAll percorre todas as keys da tree em ordem crescente.
func (tr *BTreeV2) ScanAll(fn func(key types.Comparable, value int64) error) error {
	return tr.scanWithCursor(nil, nil, fn)
}

// Scan percorre [start, end] inclusive.
func (tr *BTreeV2) Scan(start, end types.Comparable, fn func(key types.Comparable, value int64) error) error {
	return tr.scanWithCursor(start, end, fn)
}

// scanWithCursor is the shared callback-style scan implementation. It is
// thin glue over the public Cursor so legacy callers keep their callback
// semantics while the new code path streams through the cursor directly.
func (tr *BTreeV2) scanWithCursor(start, end types.Comparable, fn func(key types.Comparable, value int64) error) error {
	cur, err := tr.NewCursor(start, end)
	if err != nil {
		return err
	}
	defer func() { _ = cur.Close() }()
	for cur.Next() {
		if cbErr := fn(cur.Key(), cur.Value()); cbErr != nil {
			return cbErr
		}
	}
	return cur.Err()
}

func (tr *BTreeV2) findLeafForKey(encKey uint64) (pagestore.PageID, error) {
	pageID := tr.rootPage()
	for {
		h, err := tr.bp.Fetch(pageID)
		if err != nil {
			return pagestore.InvalidPageID, err
		}
		np, err := OpenNodePage(h.Page(), tr.maxBodySize, tr.codec.Compare)
		if err != nil {
			h.Release()
			return pagestore.InvalidPageID, err
		}
		if np.IsLeaf() {
			h.Release()
			return pageID, nil
		}
		nextPageID, err := np.FindChild(encKey)
		if err != nil {
			h.Release()
			return pagestore.InvalidPageID, err
		}
		h.Release()
		pageID = nextPageID
	}
}

func (tr *BTreeV2) findLeftmostLeaf() (pagestore.PageID, error) {
	pageID := tr.rootPage()
	for {
		h, err := tr.bp.Fetch(pageID)
		if err != nil {
			return pagestore.InvalidPageID, err
		}
		np, err := OpenNodePage(h.Page(), tr.maxBodySize, tr.codec.Compare)
		if err != nil {
			h.Release()
			return pagestore.InvalidPageID, err
		}
		if np.IsLeaf() {
			h.Release()
			return pageID, nil
		}
		nextPageID := np.LeftmostChild()
		h.Release()
		pageID = nextPageID
	}
}

// Get busca `key`. RLock — múltiplos Gets em paralelo.
func (tr *BTreeV2) Get(key types.Comparable) (int64, bool, error) {
	if tr.isVariable {
		encKey, err := tr.varCodec.Encode(key)
		if err != nil {
			return 0, false, err
		}
		return tr.getLockedVar(encKey)
	}
	encKey, err := tr.codec.Encode(key)
	if err != nil {
		return 0, false, err
	}
	return tr.getLocked(encKey)
}

// getLocked lê a tree usando apenas um snapshot rápido do rootPageID
// + latch crabbing de read entre pages.
func (tr *BTreeV2) getLocked(encKey uint64) (int64, bool, error) {
	pageID := tr.rootPage()
	for {
		h, err := tr.bp.Fetch(pageID)
		if err != nil {
			return 0, false, err
		}
		np, err := OpenNodePage(h.Page(), tr.maxBodySize, tr.codec.Compare)
		if err != nil {
			h.Release()
			return 0, false, err
		}
		if np.IsLeaf() {
			v, found := np.LeafGet(encKey)
			h.Release()
			return v, found, nil
		}
		nextPageID, err := np.FindChild(encKey)
		if err != nil {
			h.Release()
			return 0, false, err
		}
		h.Release()
		pageID = nextPageID
	}
}
