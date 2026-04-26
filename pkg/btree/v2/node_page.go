// Package v2 é a implementação page-based da B+ tree (Fase 5 do plano).
//
// Escopo desta primeira entrega (sub-etapas 5.1–5.3):
//   - NodePage layout (folha apenas — sem internals)
//   - Key encoding (IntKey fixo 8 bytes, extensível)
//   - BTreeV2 com Insert/Get em uma única page
//   - SEM split, SEM concurrency, SEM sibling traversal
//
// Essas limitações são conscientes — queremos validar a fundação antes
// de subir a complexidade (split + latch crabbing são as partes difíceis
// do plano).
package v2

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// Layout do body de uma page de B+ tree (em bytes):
//
//	0..15  NodeHeader (16 bytes)
//	  byte 0:    NodeType (leaf | internal)
//	  byte 1:    Flags
//	  byte 2-3:  NumKeys (uint16)
//	  byte 4-11: NextLeafPageID (uint64 — only leaves; 0 = none)
//	  byte 12-15: Reserved
//
//	16..fim   vetor de slots (key + value), tamanho fixo por slot
//
// Slots têm tamanho fixo (= KeySize + ValueSize). Isto simplifica:
//   - Binary search O(log n)
//   - Cálculo de capacidade sem percorrer
//   - Split balanceado (metade do vetor)
//
// Trade-off: keys de tamanho variável (VarcharKey) vão precisar de
// indireção (slot aponta pra offset no body). Por ora, suportamos só
// IntKey (8 bytes). Extensão em sub-etapa 5.7.
const (
	NodeHeaderSize = 16

	// Tipos de nó
	NodeTypeLeaf     uint8 = 1
	NodeTypeInternal uint8 = 2
)

// KeySize (8) + ValueSize (8) = 16 bytes por slot. ValueSize é:
//   - Em leaf: RecordID (int64, aponta pro heap)
//   - Em internal: PageID filho (uint64, aponta pra próxima page)
const (
	IntKeySize   = 8
	ValueSize    = 8
	LeafSlotSize = IntKeySize + ValueSize // 16 bytes por par (key, recordID)

	// Internal: os primeiros 8 bytes do body after o header são o
	// "leftmost child" (pointer pra filho esquerdo do primeiro separador).
	// Depois vêm N slots de (separator, childPageID), 16 bytes cada.
	LeftmostChildSize = 8
	InternalSlotSize  = IntKeySize + ValueSize // 16
)

var (
	ErrLeafFull     = errors.New("btree/v2: leaf has no space for a new key")
	ErrInternalFull = errors.New("btree/v2: internal node has no space for a new separator")
	ErrKeyNotFound  = errors.New("btree/v2: key not found")
	ErrBadNodeType  = errors.New("btree/v2: invalid node type")
)

// nodeHeader é a visão decodificada do cabeçalho.
type nodeHeader struct {
	nodeType       uint8
	flags          uint8
	numKeys        uint16
	nextLeafPageID pagestore.PageID // 0/Invalid quando there is no sibling (último leaf)
}

func (h *nodeHeader) encode(buf []byte) {
	_ = buf[NodeHeaderSize-1]
	buf[0] = h.nodeType
	buf[1] = h.flags
	binary.LittleEndian.PutUint16(buf[2:4], h.numKeys)
	binary.LittleEndian.PutUint64(buf[4:12], uint64(h.nextLeafPageID))
	// bytes 12..15 reservados
}

func (h *nodeHeader) decode(buf []byte) {
	h.nodeType = buf[0]
	h.flags = buf[1]
	h.numKeys = binary.LittleEndian.Uint16(buf[2:4])
	h.nextLeafPageID = pagestore.PageID(binary.LittleEndian.Uint64(buf[4:12]))
}

// CompareFn define a semântica de ordem entre duas keys encoded (uint64).
// BTreeV2 injeta via KeyCodec.Compare. Crítico pra que tipos como FloatKey
// ou IntKey negativo sejam ordenados corretamente.
type CompareFn func(a, b uint64) int

// NodePage é a visão "B+ tree node" de uma pagestore.Page.
// Ver comentário inicial pra detalhes do layout.
type NodePage struct {
	page        *pagestore.Page
	body        []byte
	maxBodySize int // limite superior (considera cifragem no PageFile)
	cmp         CompareFn
}

// defaultCompareFn usa comparação int64 (semântica IntKey). Usado como
// fallback quando testes de NodePage not se importam com semântica de
// codec (só querem testar mecânica do data structure).
func defaultCompareFn(a, b uint64) int {
	ai, bi := int64(a), int64(b)
	if ai < bi {
		return -1
	}
	if ai > bi {
		return 1
	}
	return 0
}

// InitLeafPage zera a page e grava um header de folha empty.
// `maxBodySize` must ser `pagestore.PageFile.UsableBodySize()` quando
// o PageFile tem cifra, ou `pagestore.BodySize` caso contrário.
// `cmp` define a ordem dos keys — BTreeV2 passa KeyCodec.Compare.
// Se nil, usa comparação int64 padrão.
func InitLeafPage(p *pagestore.Page, maxBodySize int, cmp CompareFn) *NodePage {
	p.Reset()
	body := p.Body()
	if maxBodySize < NodeHeaderSize {
		maxBodySize = NodeHeaderSize
	}
	if maxBodySize > len(body) {
		maxBodySize = len(body)
	}
	if cmp == nil {
		cmp = defaultCompareFn
	}

	h := nodeHeader{
		nodeType:       NodeTypeLeaf,
		numKeys:        0,
		nextLeafPageID: pagestore.InvalidPageID,
	}
	h.encode(body[:NodeHeaderSize])

	return &NodePage{page: p, body: body, maxBodySize: maxBodySize, cmp: cmp}
}

// OpenNodePage conecta-se a uma page já inicializada (ex: lida do disco).
func OpenNodePage(p *pagestore.Page, maxBodySize int, cmp CompareFn) (*NodePage, error) {
	body := p.Body()
	if maxBodySize > len(body) {
		maxBodySize = len(body)
	}
	if cmp == nil {
		cmp = defaultCompareFn
	}
	np := &NodePage{page: p, body: body, maxBodySize: maxBodySize, cmp: cmp}
	h := np.header()
	if h.nodeType != NodeTypeLeaf && h.nodeType != NodeTypeInternal {
		return nil, fmt.Errorf("%w: type %d", ErrBadNodeType, h.nodeType)
	}
	return np, nil
}

func (np *NodePage) header() nodeHeader {
	var h nodeHeader
	h.decode(np.body[:NodeHeaderSize])
	return h
}

func (np *NodePage) writeHeader(h nodeHeader) {
	h.encode(np.body[:NodeHeaderSize])
}

// IsLeaf indica se é um nó folha.
func (np *NodePage) IsLeaf() bool { return np.header().nodeType == NodeTypeLeaf }

// NumKeys devolve quantas keys estão na page.
func (np *NodePage) NumKeys() int { return int(np.header().numKeys) }

// NextLeafPageID aponta pro próximo leaf na lista ligada (sibling).
// Retorna InvalidPageID quando é o último.
func (np *NodePage) NextLeafPageID() pagestore.PageID {
	return np.header().nextLeafPageID
}

// MaxLeafSlots calcula quantos slots (key+value) cabem em uma folha.
// Depende do maxBodySize (que reflete o overhead da cifra se houver).
func (np *NodePage) MaxLeafSlots() int {
	return (np.maxBodySize - NodeHeaderSize) / LeafSlotSize
}

// slotOffset devolve o byte de início do slot i (0-indexed).
func (np *NodePage) slotOffset(i int) int {
	return NodeHeaderSize + i*LeafSlotSize
}

// readLeafSlot lê (key, value) do slot i. Assume 0 <= i < numKeys.
// Key é uint64 (representação encoded da KeyCodec); value é int64.
func (np *NodePage) readLeafSlot(i int) (key uint64, value int64) {
	base := np.slotOffset(i)
	key = binary.LittleEndian.Uint64(np.body[base : base+IntKeySize])
	value = int64(binary.LittleEndian.Uint64(np.body[base+IntKeySize : base+LeafSlotSize]))
	return
}

// writeLeafSlot grava (key, value) no slot i.
func (np *NodePage) writeLeafSlot(i int, key uint64, value int64) {
	base := np.slotOffset(i)
	binary.LittleEndian.PutUint64(np.body[base:base+IntKeySize], key)
	binary.LittleEndian.PutUint64(np.body[base+IntKeySize:base+LeafSlotSize], uint64(value))
}

// binarySearch devolve (index, achou). Usa np.cmp pra respeitar
// a semântica do KeyCodec (ex: IntKey negativo, FloatKey).
//   - achou=true:  slot[index].key == key (per cmp)
//   - achou=false: index é onde `key` seria inserido pra manter ordem
func (np *NodePage) binarySearch(key uint64) (int, bool) {
	n := np.NumKeys()
	lo, hi := 0, n
	for lo < hi {
		mid := (lo + hi) / 2
		k, _ := np.readLeafSlot(mid)
		c := np.cmp(k, key)
		if c == 0 {
			return mid, true
		}
		if c < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, false
}

// LeafInsert insere (key, value) numa folha, mantendo a ordem.
// Se `key` já exists, atualiza o value in-place (sem nova alocação de slot).
// Se `key` é nova e a folha está cheia, retorna ErrLeafFull.
func (np *NodePage) LeafInsert(key uint64, value int64) error {
	if !np.IsLeaf() {
		return ErrBadNodeType
	}

	idx, found := np.binarySearch(key)

	if found {
		// Update in-place — mesmo que a folha esteja cheia
		np.writeLeafSlot(idx, key, value)
		return nil
	}

	// Chave nova: precisamos de um slot a mais
	h := np.header()
	if int(h.numKeys) >= np.MaxLeafSlots() {
		return ErrLeafFull
	}

	// Shift dos slots >= idx uma posição à direita
	// (copia de trás pra frente pra evitar sobrwrite)
	for i := int(h.numKeys) - 1; i >= idx; i-- {
		k, v := np.readLeafSlot(i)
		np.writeLeafSlot(i+1, k, v)
	}

	np.writeLeafSlot(idx, key, value)
	h.numKeys++
	np.writeHeader(h)
	return nil
}

// LeafGet busca `key` na folha. Retorna (value, true) se achou;
// (0, false) se a key does not exist.
func (np *NodePage) LeafGet(key uint64) (int64, bool) {
	idx, found := np.binarySearch(key)
	if !found {
		return 0, false
	}
	_, v := np.readLeafSlot(idx)
	return v, true
}

// LeafDelete remove `key` da folha mantendo a ordem dos slots.
// Retorna false quando a key does not exist.
func (np *NodePage) LeafDelete(key uint64) (bool, error) {
	if !np.IsLeaf() {
		return false, ErrBadNodeType
	}

	idx, found := np.binarySearch(key)
	if !found {
		return false, nil
	}

	h := np.header()
	for i := idx; i < int(h.numKeys)-1; i++ {
		k, v := np.readLeafSlot(i + 1)
		np.writeLeafSlot(i, k, v)
	}

	last := int(h.numKeys) - 1
	if last >= 0 {
		np.writeLeafSlot(last, 0, 0)
	}

	h.numKeys--
	np.writeHeader(h)
	return true, nil
}

// LeafAt devolve o par (key, value) do slot i, em ordem. Útil para
// iteração e para testes. Pânico se i fora do intervalo.
// Key é uint64 (encoded via KeyCodec); value é int64.
func (np *NodePage) LeafAt(i int) (uint64, int64) {
	if i < 0 || i >= np.NumKeys() {
		panic(fmt.Sprintf("btree/v2: LeafAt index %d fora de [0, %d)", i, np.NumKeys()))
	}
	return np.readLeafSlot(i)
}

// splitLeafInto divide esta folha em duas:
//   - self (left) fica com a metade inferior: slots[0..mid)
//   - other (right) recebe a metade superior: slots[mid..n)
//
// `other` must ser uma folha empty (recém-criada via InitLeafPage).
// Retorna a key separadora = primeira key da metade direita.
//
// O caller é responsável por:
//   - Alocar a page do `other` via BufferPool
//   - Atualizar self.nextLeafPageID = other.pageID (esta função NOT faz)
//   - Persistir ambas as pages (MarkDirty)
//
// Esta função atualiza:
//   - numKeys em ambas
//   - other.nextLeafPageID = self.nextLeafPageID (other herda o link)
func (np *NodePage) splitLeafInto(other *NodePage) uint64 {
	if !np.IsLeaf() || !other.IsLeaf() {
		panic("btree/v2: splitLeafInto requer dois leaf nodes")
	}
	if other.NumKeys() != 0 {
		panic("btree/v2: splitLeafInto requer other empty")
	}

	n := np.NumKeys()
	mid := n / 2

	// Copia slots[mid..n) pra other[0..n-mid)
	for i := mid; i < n; i++ {
		k, v := np.readLeafSlot(i)
		other.writeLeafSlot(i-mid, k, v)
	}

	// Atualiza contadores e sibling link de other
	otherHdr := other.header()
	otherHdr.numKeys = uint16(n - mid)
	otherHdr.nextLeafPageID = np.header().nextLeafPageID // herda o link
	other.writeHeader(otherHdr)

	// Trunca self
	selfHdr := np.header()
	selfHdr.numKeys = uint16(mid)
	// self.nextLeafPageID fica intacto aqui — o caller atualiza depois de
	// conhecer o pageID do `other`.
	np.writeHeader(selfHdr)

	// Separador = primeira key da metade direita
	sep, _ := other.readLeafSlot(0)
	return sep
}

// setNextLeafPageID atualiza o sibling pointer. Chamado pelo BTreeV2
// after split (quando já conhece o pageID do novo leaf).
func (np *NodePage) setNextLeafPageID(pid pagestore.PageID) {
	h := np.header()
	h.nextLeafPageID = pid
	np.writeHeader(h)
}

// ─────────────────────────────────────────────────────────────────────
// Internal node operations
// ─────────────────────────────────────────────────────────────────────
//
// Layout de internal node (dentro do body):
//
//   NodeHeader (16 bytes)
//   [leftmostChildPageID: 8 bytes]        ← c_0 (filho pra keys < sep_0)
//   slot[0]: (sep_0, c_1)  16 bytes       ← c_1 pra keys em [sep_0, sep_1)
//   slot[1]: (sep_1, c_2)
//   ...
//   slot[n-1]: (sep_{n-1}, c_n)           ← c_n pra keys >= sep_{n-1}
//
// N separadores → N+1 filhos.

// InitInternalPage zera a page e grava um header de internal node.
// `leftmost` é o PageID do filho mais à esquerda (c_0).
// `cmp` define a ordem das keys (BTreeV2 passa KeyCodec.Compare).
// Se cmp é nil, usa comparação int64 padrão.
func InitInternalPage(p *pagestore.Page, maxBodySize int, leftmost pagestore.PageID, cmp CompareFn) *NodePage {
	p.Reset()
	body := p.Body()
	if maxBodySize < NodeHeaderSize+LeftmostChildSize {
		maxBodySize = NodeHeaderSize + LeftmostChildSize
	}
	if maxBodySize > len(body) {
		maxBodySize = len(body)
	}
	if cmp == nil {
		cmp = defaultCompareFn
	}

	h := nodeHeader{
		nodeType: NodeTypeInternal,
		numKeys:  0,
	}
	h.encode(body[:NodeHeaderSize])
	binary.LittleEndian.PutUint64(body[NodeHeaderSize:NodeHeaderSize+LeftmostChildSize], uint64(leftmost))

	return &NodePage{page: p, body: body, maxBodySize: maxBodySize, cmp: cmp}
}

// LeftmostChild devolve c_0 (filho pra keys < primeiro separador).
func (np *NodePage) LeftmostChild() pagestore.PageID {
	return pagestore.PageID(
		binary.LittleEndian.Uint64(np.body[NodeHeaderSize : NodeHeaderSize+LeftmostChildSize]),
	)
}

// setLeftmostChild atualiza c_0. Uso restrito (só durante split/grow root).
func (np *NodePage) setLeftmostChild(pid pagestore.PageID) {
	binary.LittleEndian.PutUint64(
		np.body[NodeHeaderSize:NodeHeaderSize+LeftmostChildSize],
		uint64(pid),
	)
}

func (np *NodePage) internalSlotOffset(i int) int {
	return NodeHeaderSize + LeftmostChildSize + i*InternalSlotSize
}

func (np *NodePage) readInternalSlot(i int) (key uint64, child pagestore.PageID) {
	base := np.internalSlotOffset(i)
	key = binary.LittleEndian.Uint64(np.body[base : base+IntKeySize])
	child = pagestore.PageID(binary.LittleEndian.Uint64(np.body[base+IntKeySize : base+InternalSlotSize]))
	return
}

func (np *NodePage) writeInternalSlot(i int, key uint64, child pagestore.PageID) {
	base := np.internalSlotOffset(i)
	binary.LittleEndian.PutUint64(np.body[base:base+IntKeySize], key)
	binary.LittleEndian.PutUint64(np.body[base+IntKeySize:base+InternalSlotSize], uint64(child))
}

// MaxInternalSlots calcula quantos separadores cabem em um internal.
func (np *NodePage) MaxInternalSlots() int {
	return (np.maxBodySize - NodeHeaderSize - LeftmostChildSize) / InternalSlotSize
}

// internalBinarySearch busca pelo primeiro separador > key (per cmp).
// Retorna esse index. Se todos são <= key, retorna numKeys.
func (np *NodePage) internalBinarySearch(key uint64) int {
	n := np.NumKeys()
	lo, hi := 0, n
	for lo < hi {
		mid := (lo + hi) / 2
		k, _ := np.readInternalSlot(mid)
		if np.cmp(k, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// FindChild devolve o PageID do filho cuja sub-tree contém `key`.
func (np *NodePage) FindChild(key uint64) pagestore.PageID {
	if !np.isInternal() {
		panic("btree/v2: FindChild chamado em not-internal")
	}
	firstGT := np.internalBinarySearch(key)
	if firstGT == 0 {
		return np.LeftmostChild()
	}
	_, child := np.readInternalSlot(firstGT - 1)
	return child
}

// isInternal é helper interno (IsLeaf pública já exists).
func (np *NodePage) isInternal() bool {
	return np.header().nodeType == NodeTypeInternal
}

// InsertSeparator insere (key, childPageID) numa posição ordenada.
// Retorna ErrInternalFull se not houver espaço.
//
// Contrato: o chamador garante que `key` does not exist ainda entre os
// separadores (B+ tree not tem duplicatas em internals). Se exist,
// este método NOT detecta — e produz estrutura invalid. Em uso real,
// isso nunca acontece porque separadores vêm de keys de leaves que
// ainda not eram separadores.
func (np *NodePage) InsertSeparator(key uint64, child pagestore.PageID) error {
	if !np.isInternal() {
		return ErrBadNodeType
	}
	h := np.header()
	if int(h.numKeys) >= np.MaxInternalSlots() {
		return ErrInternalFull
	}

	// Posição de inserção: primeiro index onde sep > key
	idx := np.internalBinarySearch(key)

	// Shift dos slots [idx..n) uma posição à direita
	for i := int(h.numKeys) - 1; i >= idx; i-- {
		k, c := np.readInternalSlot(i)
		np.writeInternalSlot(i+1, k, c)
	}
	np.writeInternalSlot(idx, key, child)
	h.numKeys++
	np.writeHeader(h)
	return nil
}

// InternalAt devolve (sep_i, child_{i+1}) do slot i. Útil pra testes
// e pra split. Pânico fora do intervalo.
func (np *NodePage) InternalAt(i int) (uint64, pagestore.PageID) {
	if i < 0 || i >= np.NumKeys() {
		panic(fmt.Sprintf("btree/v2: InternalAt index %d fora de [0, %d)", i, np.NumKeys()))
	}
	return np.readInternalSlot(i)
}

// splitInternalInto divide este internal em dois, promovendo a key
// do meio pro parent. Diferente do leaf split:
//
//	Leaf:     k_mid vai PRO RIGHT (separador duplicado)
//	Internal: k_mid é PROMOVIDA — not fica em left nem em right
//
// Layout antes (n separadores, n+1 filhos):
//
//	left_original: leftmost=c_0, seps=[k_0..k_{n-1}], childs=[c_1..c_n]
//
// Layout depois (mid = n/2):
//
//	left (self): leftmost=c_0, seps=[k_0..k_{mid-1}], childs=[c_1..c_mid]
//	  → mid separadores, mid+1 filhos
//	right (other): leftmost=c_{mid+1}, seps=[k_{mid+1}..k_{n-1}], childs=[c_{mid+2}..c_n]
//	  → n-mid-1 separadores, n-mid filhos
//	promoted: k_mid
//
// Retorna a key promovida. O caller é responsável por:
//   - Alocar o `other` via BufferPool
//   - Inserir `promoted` no parent (com pointer pra `other`)
//   - MarkDirty em ambos
func (np *NodePage) splitInternalInto(other *NodePage) uint64 {
	if !np.isInternal() || !other.isInternal() {
		panic("btree/v2: splitInternalInto requer dois internal nodes")
	}
	if other.NumKeys() != 0 {
		panic("btree/v2: splitInternalInto requer other empty")
	}

	n := np.NumKeys()
	mid := n / 2

	// k_mid é a key promovida; seu child c_{mid+1} (= slot[mid].child)
	// vira o leftmost de other.
	promoted, rightLeftmost := np.readInternalSlot(mid)
	other.setLeftmostChild(rightLeftmost)

	// Copia slots[mid+1..n) pra other[0..n-mid-1)
	for i := mid + 1; i < n; i++ {
		k, c := np.readInternalSlot(i)
		other.writeInternalSlot(i-(mid+1), k, c)
	}

	otherHdr := other.header()
	otherHdr.numKeys = uint16(n - mid - 1)
	other.writeHeader(otherHdr)

	// Trunca self
	selfHdr := np.header()
	selfHdr.numKeys = uint16(mid)
	np.writeHeader(selfHdr)

	return promoted
}
