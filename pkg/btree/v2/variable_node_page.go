package v2

import (
	"encoding/binary"
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// Layout de variable-key NodePage (leaf ou internal):
//
//   [NodeHeader: 16 bytes]
//   [leftmostChild: 8 bytes]              ← só internal
//   [slot_dir]: 12 bytes por slot, crescendo a partir do header:
//     slot[i]: keyOffset uint16 | keyLength uint16 | value int64
//     (em internal, `value` é interpretado como childPageID uint64)
//   [free space]
//   [key bytes]: crescem de trás pra frente (fim do body → centro)
//
// O `value` do slot segue a mesma convenção do formato fixo:
//   - Leaf: RecordID (int64)
//   - Internal: childPageID (uint64 cast p/ int64)
//
// Fragmentação: inserts crescem key bytes pra trás; updates in-place
// no value (sem realocar key). DELETES (quando implementados) criariam
// holes — precisariam de compaction pro reaproveitamento ser eficiente.
// Por ora, sem delete na BTreeV2, fragmentação não é problema.

const (
	// VariableSlotSize: keyOffset(2) + keyLength(2) + value(8)
	VariableSlotSize = 12

	// keyFormatFixed é o header.format pro layout original (keys em 8 bytes fixos).
	keyFormatFixed uint8 = 0

	// keyFormatVariable é o header.format pro layout slotted.
	keyFormatVariable uint8 = 1
)

// VariableCompareFn compara dois byte-slices semanticamente.
type VariableCompareFn func(a, b []byte) int

// VariableNodePage é a visão "variable-key B+ tree node" de uma pagestore.Page.
// Paralelo ao NodePage (fixed); ambos coexistem porque o layout é diferente
// o suficiente pra justificar código separado.
type VariableNodePage struct {
	page        *pagestore.Page
	body        []byte
	maxBodySize int
	cmp         VariableCompareFn
}

// InitLeafPageVar zera a página e grava header de folha VARIABLE-key.
func InitLeafPageVar(p *pagestore.Page, maxBodySize int, cmp VariableCompareFn) *VariableNodePage {
	p.Reset()
	body := p.Body()
	if maxBodySize < NodeHeaderSize {
		maxBodySize = NodeHeaderSize
	}
	if maxBodySize > len(body) {
		maxBodySize = len(body)
	}

	// header sem nenhuma entrada; no byte 12 gravamos format=variable.
	// (byte 12 é o primeiro dos 4 bytes "reservados" do layout original.)
	// freeSpaceEnd = maxBodySize (key bytes começam lá pra trás)
	h := nodeHeader{
		nodeType:       NodeTypeLeaf,
		numKeys:        0,
		nextLeafPageID: pagestore.InvalidPageID,
	}
	h.encode(body[:NodeHeaderSize])
	body[12] = keyFormatVariable

	// Grava freeSpaceEnd no byte 13..14 (reserved antes)
	binary.LittleEndian.PutUint16(body[13:15], uint16(maxBodySize))

	return &VariableNodePage{page: p, body: body, maxBodySize: maxBodySize, cmp: cmp}
}

// InitInternalPageVar zera e grava header de internal VARIABLE-key.
func InitInternalPageVar(p *pagestore.Page, maxBodySize int, leftmost pagestore.PageID, cmp VariableCompareFn) *VariableNodePage {
	p.Reset()
	body := p.Body()
	if maxBodySize < NodeHeaderSize+LeftmostChildSize {
		maxBodySize = NodeHeaderSize + LeftmostChildSize
	}
	if maxBodySize > len(body) {
		maxBodySize = len(body)
	}

	h := nodeHeader{
		nodeType: NodeTypeInternal,
		numKeys:  0,
	}
	h.encode(body[:NodeHeaderSize])
	body[12] = keyFormatVariable
	binary.LittleEndian.PutUint16(body[13:15], uint16(maxBodySize))

	binary.LittleEndian.PutUint64(
		body[NodeHeaderSize:NodeHeaderSize+LeftmostChildSize],
		uint64(leftmost),
	)

	return &VariableNodePage{page: p, body: body, maxBodySize: maxBodySize, cmp: cmp}
}

// OpenVariableNodePage abre uma página já inicializada com formato variável.
func OpenVariableNodePage(p *pagestore.Page, maxBodySize int, cmp VariableCompareFn) (*VariableNodePage, error) {
	body := p.Body()
	if maxBodySize > len(body) {
		maxBodySize = len(body)
	}
	vp := &VariableNodePage{page: p, body: body, maxBodySize: maxBodySize, cmp: cmp}
	h := vp.header()
	if h.nodeType != NodeTypeLeaf && h.nodeType != NodeTypeInternal {
		return nil, fmt.Errorf("%w: tipo %d", ErrBadNodeType, h.nodeType)
	}
	if body[12] != keyFormatVariable {
		return nil, fmt.Errorf("btree/v2: página não é formato variable (format=%d)", body[12])
	}
	return vp, nil
}

// IsVariableKeyPage detecta pelo byte 12 do body se a página é variável.
func IsVariableKeyPage(p *pagestore.Page) bool {
	return p.Body()[12] == keyFormatVariable
}

func (vp *VariableNodePage) header() nodeHeader {
	var h nodeHeader
	h.decode(vp.body[:NodeHeaderSize])
	return h
}

func (vp *VariableNodePage) writeHeader(h nodeHeader) {
	h.encode(vp.body[:NodeHeaderSize])
}

// freeSpaceEnd fica em bytes 13..14 (reaproveitado do espaço reservado
// do header original). Cresce pra trás (começa em maxBodySize).
func (vp *VariableNodePage) freeSpaceEnd() uint16 {
	return binary.LittleEndian.Uint16(vp.body[13:15])
}

func (vp *VariableNodePage) setFreeSpaceEnd(v uint16) {
	binary.LittleEndian.PutUint16(vp.body[13:15], v)
}

// IsLeaf retorna true pra folhas.
func (vp *VariableNodePage) IsLeaf() bool {
	return vp.header().nodeType == NodeTypeLeaf
}

// NumKeys retorna a quantidade de slots.
func (vp *VariableNodePage) NumKeys() int {
	return int(vp.header().numKeys)
}

// NextLeafPageID retorna o sibling da folha.
func (vp *VariableNodePage) NextLeafPageID() pagestore.PageID {
	return vp.header().nextLeafPageID
}

func (vp *VariableNodePage) setNextLeafPageID(pid pagestore.PageID) {
	h := vp.header()
	h.nextLeafPageID = pid
	vp.writeHeader(h)
}

// LeftmostChild retorna c_0 em um internal.
func (vp *VariableNodePage) LeftmostChild() pagestore.PageID {
	return pagestore.PageID(
		binary.LittleEndian.Uint64(vp.body[NodeHeaderSize : NodeHeaderSize+LeftmostChildSize]),
	)
}

func (vp *VariableNodePage) setLeftmostChild(pid pagestore.PageID) {
	binary.LittleEndian.PutUint64(
		vp.body[NodeHeaderSize:NodeHeaderSize+LeftmostChildSize],
		uint64(pid),
	)
}

// isInternal: helper pra saber se é internal node.
func (vp *VariableNodePage) isInternal() bool {
	return vp.header().nodeType == NodeTypeInternal
}

// slotDirStart é onde o slot_dir começa. Depende se é leaf (após header)
// ou internal (após header + leftmostChild).
func (vp *VariableNodePage) slotDirStart() int {
	if vp.isInternal() {
		return NodeHeaderSize + LeftmostChildSize
	}
	return NodeHeaderSize
}

func (vp *VariableNodePage) slotOffset(i int) int {
	return vp.slotDirStart() + i*VariableSlotSize
}

func (vp *VariableNodePage) readSlot(i int) (keyOffset, keyLength uint16, value int64) {
	base := vp.slotOffset(i)
	keyOffset = binary.LittleEndian.Uint16(vp.body[base : base+2])
	keyLength = binary.LittleEndian.Uint16(vp.body[base+2 : base+4])
	value = int64(binary.LittleEndian.Uint64(vp.body[base+4 : base+12]))
	return
}

func (vp *VariableNodePage) writeSlot(i int, keyOffset, keyLength uint16, value int64) {
	base := vp.slotOffset(i)
	binary.LittleEndian.PutUint16(vp.body[base:base+2], keyOffset)
	binary.LittleEndian.PutUint16(vp.body[base+2:base+4], keyLength)
	binary.LittleEndian.PutUint64(vp.body[base+4:base+12], uint64(value))
}

// keyBytesAt devolve o byte-slice da key do slot i.
func (vp *VariableNodePage) keyBytesAt(i int) []byte {
	off, length, _ := vp.readSlot(i)
	return vp.body[off : off+length]
}

// FreeSpace devolve quantos bytes livres há entre o fim do slot_dir e
// o início da região de key bytes.
func (vp *VariableNodePage) FreeSpace() int {
	n := vp.NumKeys()
	slotEnd := vp.slotDirStart() + n*VariableSlotSize
	return int(vp.freeSpaceEnd()) - slotEnd
}

// CanLeafInsertVar retorna true quando a operação de insert/update da
// `key` cabe na folha sem split.
func (vp *VariableNodePage) CanLeafInsertVar(key []byte) bool {
	if _, found := vp.binarySearchVar(key); found {
		return true
	}
	return vp.FreeSpace() >= VariableSlotSize+len(key)
}

// CanAbsorbSeparatorVar retorna true quando a página internal ainda
// comporta um novo separador de `keyLen` bytes sem split.
func (vp *VariableNodePage) CanAbsorbSeparatorVar(keyLen int) bool {
	return vp.FreeSpace() >= VariableSlotSize+keyLen
}

// SplitSeparatorLenVar devolve o comprimento exato da chave que esta
// página promoveria num split do layout atual.
//
// Para folha: é a primeira chave da metade direita (índice n/2).
// Para internal: é a chave do meio promovida ao parent (índice n/2).
func (vp *VariableNodePage) SplitSeparatorLenVar() int {
	n := vp.NumKeys()
	if n == 0 {
		return 0
	}

	mid := n / 2
	_, keyLen, _ := vp.readSlot(mid)
	return int(keyLen)
}

// binarySearchVar procura key no slot_dir. Se achou, (idx, true).
// Senão, (idx = posição onde inserir pra manter ordem, false).
func (vp *VariableNodePage) binarySearchVar(key []byte) (int, bool) {
	n := vp.NumKeys()
	lo, hi := 0, n
	for lo < hi {
		mid := (lo + hi) / 2
		k := vp.keyBytesAt(mid)
		c := vp.cmp(k, key)
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

// LeafInsertVar insere (key, value) na folha variable. Se a key já
// existe, atualiza o value in-place (sem realocar bytes). Senão aloca
// key bytes + slot novo.
func (vp *VariableNodePage) LeafInsertVar(key []byte, value int64) error {
	if !vp.IsLeaf() {
		return ErrBadNodeType
	}
	if len(key) > int(^uint16(0)) {
		return fmt.Errorf("btree/v2: key de %d bytes excede limite uint16", len(key))
	}

	idx, found := vp.binarySearchVar(key)

	if found {
		off, length, _ := vp.readSlot(idx)
		vp.writeSlot(idx, off, length, value)
		return nil
	}

	needed := VariableSlotSize + len(key)
	if vp.FreeSpace() < needed {
		return ErrLeafFull
	}

	// Aloca key bytes no fim da região livre.
	newKeyOffset := vp.freeSpaceEnd() - uint16(len(key))
	copy(vp.body[newKeyOffset:newKeyOffset+uint16(len(key))], key)

	// Shift slots [idx..n) pra direita.
	h := vp.header()
	for i := int(h.numKeys) - 1; i >= idx; i-- {
		k, l, v := vp.readSlot(i)
		vp.writeSlot(i+1, k, l, v)
	}
	vp.writeSlot(idx, newKeyOffset, uint16(len(key)), value)

	h.numKeys++
	vp.writeHeader(h)
	vp.setFreeSpaceEnd(newKeyOffset)
	return nil
}

// LeafGetVar busca key na folha. Retorna (value, true) se achou.
func (vp *VariableNodePage) LeafGetVar(key []byte) (int64, bool) {
	idx, found := vp.binarySearchVar(key)
	if !found {
		return 0, false
	}
	_, _, v := vp.readSlot(idx)
	return v, true
}

// LeafDeleteVar remove uma key da folha e recompata a página inteira.
// Isto evita holes no body e reaproveita o espaço imediatamente.
func (vp *VariableNodePage) LeafDeleteVar(key []byte) (bool, error) {
	if !vp.IsLeaf() {
		return false, ErrBadNodeType
	}

	idx, found := vp.binarySearchVar(key)
	if !found {
		return false, nil
	}

	nextLeaf := vp.NextLeafPageID()
	type kv struct {
		key []byte
		val int64
	}

	entries := make([]kv, 0, vp.NumKeys()-1)
	for i := 0; i < vp.NumKeys(); i++ {
		if i == idx {
			continue
		}
		k, v := vp.LeafAtVar(i)
		keyCopy := make([]byte, len(k))
		copy(keyCopy, k)
		entries = append(entries, kv{key: keyCopy, val: v})
	}

	InitLeafPageVar(vp.page, vp.maxBodySize, vp.cmp)
	vp.setNextLeafPageID(nextLeaf)
	for _, entry := range entries {
		if err := vp.LeafInsertVar(entry.key, entry.val); err != nil {
			return false, fmt.Errorf("btree/v2: compactação pós-delete falhou: %w", err)
		}
	}

	return true, nil
}

// LeafAtVar devolve (keyBytes, value) do slot i.
func (vp *VariableNodePage) LeafAtVar(i int) ([]byte, int64) {
	if i < 0 || i >= vp.NumKeys() {
		panic(fmt.Sprintf("btree/v2: LeafAtVar índice %d fora de [0, %d)", i, vp.NumKeys()))
	}
	off, length, v := vp.readSlot(i)
	return vp.body[off : off+length], v
}

// internalBinarySearchVar busca o primeiro sep > key.
// Retorna esse índice (ou numKeys se todos <= key).
func (vp *VariableNodePage) internalBinarySearchVar(key []byte) int {
	n := vp.NumKeys()
	lo, hi := 0, n
	for lo < hi {
		mid := (lo + hi) / 2
		k := vp.keyBytesAt(mid)
		if vp.cmp(k, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// FindChildVar retorna o child pra `key` num internal variable.
func (vp *VariableNodePage) FindChildVar(key []byte) pagestore.PageID {
	if !vp.isInternal() {
		panic("btree/v2: FindChildVar em não-internal")
	}
	firstGT := vp.internalBinarySearchVar(key)
	if firstGT == 0 {
		return vp.LeftmostChild()
	}
	_, _, child := vp.readSlot(firstGT - 1)
	return pagestore.PageID(child)
}

// InsertSeparatorVar insere (sepKey, childPageID) num internal variable.
func (vp *VariableNodePage) InsertSeparatorVar(sepKey []byte, child pagestore.PageID) error {
	if !vp.isInternal() {
		return ErrBadNodeType
	}
	if len(sepKey) > int(^uint16(0)) {
		return fmt.Errorf("btree/v2: sepKey de %d bytes excede uint16", len(sepKey))
	}

	needed := VariableSlotSize + len(sepKey)
	if vp.FreeSpace() < needed {
		return ErrInternalFull
	}

	idx := vp.internalBinarySearchVar(sepKey)

	newKeyOffset := vp.freeSpaceEnd() - uint16(len(sepKey))
	copy(vp.body[newKeyOffset:newKeyOffset+uint16(len(sepKey))], sepKey)

	h := vp.header()
	for i := int(h.numKeys) - 1; i >= idx; i-- {
		k, l, v := vp.readSlot(i)
		vp.writeSlot(i+1, k, l, v)
	}
	vp.writeSlot(idx, newKeyOffset, uint16(len(sepKey)), int64(child))

	h.numKeys++
	vp.writeHeader(h)
	vp.setFreeSpaceEnd(newKeyOffset)
	return nil
}

// InternalAtVar devolve (keyBytes, childPageID) do slot i.
func (vp *VariableNodePage) InternalAtVar(i int) ([]byte, pagestore.PageID) {
	if i < 0 || i >= vp.NumKeys() {
		panic(fmt.Sprintf("btree/v2: InternalAtVar índice %d fora de [0, %d)", i, vp.NumKeys()))
	}
	off, length, v := vp.readSlot(i)
	return vp.body[off : off+length], pagestore.PageID(v)
}

// splitLeafIntoVar: move metade das chaves (pela metade superior dos
// slots) pra `other`. Retorna as bytes da chave separadora (primeira
// da metade direita) — cópia independente, caller não precisa se
// preocupar com aliasing no body original.
func (vp *VariableNodePage) splitLeafIntoVar(other *VariableNodePage) []byte {
	if !vp.IsLeaf() || !other.IsLeaf() {
		panic("btree/v2: splitLeafIntoVar requer leaves variable")
	}
	if other.NumKeys() != 0 {
		panic("btree/v2: splitLeafIntoVar requer other vazio")
	}

	n := vp.NumKeys()
	mid := n / 2

	// Copia slots[mid..n) pra other, realocando key bytes em other.
	for i := mid; i < n; i++ {
		keyBytes, value := vp.LeafAtVar(i)
		if err := other.LeafInsertVar(keyBytes, value); err != nil {
			panic(fmt.Sprintf("btree/v2: inserção no right pós-split falhou: %v", err))
		}
	}

	// Separador = primeira chave do right half. Cópia segura: LeafAtVar
	// em self ainda funciona pra ler, mas after truncation precisa
	// retornar cópia.
	sepSrc, _ := vp.LeafAtVar(mid)
	sep := make([]byte, len(sepSrc))
	copy(sep, sepSrc)

	// Other herda nextLeafPageID original do self.
	otherHdr := other.header()
	otherHdr.nextLeafPageID = vp.header().nextLeafPageID
	other.writeHeader(otherHdr)

	// Trunca self: slots[0..mid) mantidos; seus key bytes continuam
	// espalhados no body (fragmentação). Pra "resetar" esses bytes,
	// faríamos compaction — mas mantê-los é correto (só usa espaço
	// que o insert futuro pode reaproveitar via update-in-place, ou
	// num delete futuro).
	//
	// Simplicidade primeiro: só atualiza numKeys.
	selfHdr := vp.header()
	selfHdr.numKeys = uint16(mid)
	vp.writeHeader(selfHdr)

	return sep
}

// splitInternalIntoVar: promove chave do meio; move slots[mid+1..n) pra other.
// other.leftmost = slot[mid].child. Retorna a key promovida (cópia).
func (vp *VariableNodePage) splitInternalIntoVar(other *VariableNodePage) []byte {
	if !vp.isInternal() || !other.isInternal() {
		panic("btree/v2: splitInternalIntoVar requer internals variable")
	}
	if other.NumKeys() != 0 {
		panic("btree/v2: splitInternalIntoVar requer other vazio")
	}

	n := vp.NumKeys()
	mid := n / 2

	// Key promovida = slot[mid]
	promotedSrc, promotedChild := vp.InternalAtVar(mid)
	promoted := make([]byte, len(promotedSrc))
	copy(promoted, promotedSrc)

	// other.leftmostChild = child do slot[mid]
	other.setLeftmostChild(promotedChild)

	// Copia slots[mid+1..n) pra other.
	for i := mid + 1; i < n; i++ {
		keyBytes, child := vp.InternalAtVar(i)
		if err := other.InsertSeparatorVar(keyBytes, child); err != nil {
			panic(fmt.Sprintf("btree/v2: InsertSeparatorVar no right pós-split falhou: %v", err))
		}
	}

	// Trunca self.
	selfHdr := vp.header()
	selfHdr.numKeys = uint16(mid)
	vp.writeHeader(selfHdr)

	return promoted
}
