// Package v2 é a implementação page-based do heap (Fase 3 do plano em
// docs/page_based_migration_plan.md).
package v2

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/heap"
	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// SlottedHeader ocupa os primeiros 12 bytes do body de uma page.
// Mantido em claro dentro do body (que pode ser cifrado pela camada
// de PageFile, então not precisa de proteção adicional aqui).
const (
	SlottedHeaderSize = 12
	SlotSize          = 4 // uint16 offset + uint16 length

	// RecordHeaderSize: Valid(1) + CreateLSN(8) + DeleteLSN(8) + PrevRecordID(8)
	RecordHeaderSize = 25
)

// NoRecordID é o sentinela para "sem versão anterior" (análogo ao -1 do v1).
const NoRecordID int64 = -1

var (
	ErrPageFull     = errors.New("heap/v2: no free space on page")
	ErrSlotNotFound = errors.New("heap/v2: invalid slot or slot out of range")
	ErrBadRecord    = errors.New("heap/v2: record corrupted (length inconsistente)")
	// ErrVacuumed sinaliza que o slot exists no dir mas seu record
	// foi reclaimdo por Compact. SlotID permanece estável (referências
	// externas continuam válidas), mas a read not devolve content.
	// Chain walks mustm tratar como fim de cadeia.
	ErrVacuumed = errors.New("heap/v2: slot vacuumado (record reclaimdo)")
)

// RecordHeader é alias pro tipo compartilhado em pkg/heap. Isso permite
// que a interface heap.Heap trate v1 e v2 intercambiavelmente sem
// conversões. Os métodos de encoding ficam como funções de pacote
// (abaixo) porque Go not permite métodos fora do pacote de origem.
type RecordHeader = heap.RecordHeader

func encodeRecordHeader(h *RecordHeader, buf []byte) {
	_ = buf[RecordHeaderSize-1]
	if h.Valid {
		buf[0] = 1
	} else {
		buf[0] = 0
	}
	binary.LittleEndian.PutUint64(buf[1:9], h.CreateLSN)
	binary.LittleEndian.PutUint64(buf[9:17], h.DeleteLSN)
	binary.LittleEndian.PutUint64(buf[17:25], uint64(h.PrevRecordID))
}

func decodeRecordHeader(h *RecordHeader, buf []byte) {
	h.Valid = buf[0] == 1
	h.CreateLSN = binary.LittleEndian.Uint64(buf[1:9])
	h.DeleteLSN = binary.LittleEndian.Uint64(buf[9:17])
	h.PrevRecordID = int64(binary.LittleEndian.Uint64(buf[17:25]))
}

// slottedHeader é a visão decodificada do cabeçalho no body da page.
type slottedHeader struct {
	numSlots       uint16
	numValid       uint16
	freeSpaceStart uint16 // onde o próximo record vai começar
	freeSpaceEnd   uint16 // final do slot directory (slots crescem pra cima daqui)
	flags          uint8
	// reserved[3]
}

func (h *slottedHeader) encode(buf []byte) {
	_ = buf[SlottedHeaderSize-1]
	binary.LittleEndian.PutUint16(buf[0:2], h.numSlots)
	binary.LittleEndian.PutUint16(buf[2:4], h.numValid)
	binary.LittleEndian.PutUint16(buf[4:6], h.freeSpaceStart)
	binary.LittleEndian.PutUint16(buf[6:8], h.freeSpaceEnd)
	buf[8] = h.flags
	// bytes 9..11 reservados
}

func (h *slottedHeader) decode(buf []byte) {
	h.numSlots = binary.LittleEndian.Uint16(buf[0:2])
	h.numValid = binary.LittleEndian.Uint16(buf[2:4])
	h.freeSpaceStart = binary.LittleEndian.Uint16(buf[4:6])
	h.freeSpaceEnd = binary.LittleEndian.Uint16(buf[6:8])
	h.flags = buf[8]
}

// SlottedPage é a visão slotted de uma pagestore.Page. Os bytes ficam
// no body da page (atrás do PageHeader de 32 bytes do pagestore).
//
// Layout do body (8160 bytes sem cifra, 8132 com cifra na camada PageFile):
//
//	0          SlottedHeader (12 bytes)
//	12         slot 0   (4 bytes)
//	16         slot 1   (4 bytes)
//	...        slot N-1
//	freeStart  [espaço livre]
//	freeEnd    record N-1 bytes
//	           ...
//	           record 0 bytes
//	bodySize   fim do body
//
// Slots crescem pra cima (baixo endereço → alto), records crescem
// pra baixo (alto endereço → baixo). Quando freeStart >= freeEnd, cheio.
type SlottedPage struct {
	page *pagestore.Page
	body []byte // alias para page.Body()
}

// InitSlottedPage zera uma page e inicializa o slotted header com
// `maxBodySize` como limite superior para o espaço de records.
//
// Quando a page será cifrada pela camada PageFile, `maxBodySize` must
// ser `pagestore.PageFile.UsableBodySize()` (= BodySize - 28 com AES-GCM).
// Sem cifra, passe `pagestore.BodySize`.
func InitSlottedPage(p *pagestore.Page, maxBodySize int) *SlottedPage {
	p.Reset()
	body := p.Body()
	if maxBodySize < SlottedHeaderSize {
		maxBodySize = SlottedHeaderSize
	}
	if maxBodySize > len(body) {
		maxBodySize = len(body)
	}
	h := slottedHeader{
		freeSpaceStart: SlottedHeaderSize,
		freeSpaceEnd:   uint16(maxBodySize),
	}
	h.encode(body[:SlottedHeaderSize])
	return &SlottedPage{page: p, body: body}
}

// OpenSlottedPage conecta-se a uma page já inicializada (ex: lida do disco).
// NOT zera a page.
func OpenSlottedPage(p *pagestore.Page) *SlottedPage {
	return &SlottedPage{page: p, body: p.Body()}
}

func (sp *SlottedPage) header() slottedHeader {
	var h slottedHeader
	h.decode(sp.body[:SlottedHeaderSize])
	return h
}

func (sp *SlottedPage) writeHeader(h slottedHeader) {
	h.encode(sp.body[:SlottedHeaderSize])
}

// NumSlots devolve a quantidade total de slots (válidos ou not).
func (sp *SlottedPage) NumSlots() int { return int(sp.header().numSlots) }

// NumValid devolve a quantidade de slots com Valid=1.
func (sp *SlottedPage) NumValid() int { return int(sp.header().numValid) }

// FreeSpace devolve quantos bytes livres há entre slot dir e o primeiro record.
// Nota: inserir um novo record consome 4 bytes de slot + tamanho do record,
// então é preciso FreeSpace() >= 4 + recordSize.
func (sp *SlottedPage) FreeSpace() int {
	h := sp.header()
	return int(h.freeSpaceEnd) - int(h.freeSpaceStart)
}

// readSlot lê o (offset, length) do slot i. Assume 0 <= i < numSlots.
func (sp *SlottedPage) readSlot(i uint16) (offset, length uint16) {
	base := SlottedHeaderSize + int(i)*SlotSize
	offset = binary.LittleEndian.Uint16(sp.body[base : base+2])
	length = binary.LittleEndian.Uint16(sp.body[base+2 : base+4])
	return
}

// writeSlot grava (offset, length) no slot i.
func (sp *SlottedPage) writeSlot(i uint16, offset, length uint16) {
	base := SlottedHeaderSize + int(i)*SlotSize
	binary.LittleEndian.PutUint16(sp.body[base:base+2], offset)
	binary.LittleEndian.PutUint16(sp.body[base+2:base+4], length)
}

// Insert grava um record (header + doc) na page. Retorna o SlotID
// alocado. SlotIDs são monotonicamente crescentes — o engine nunca
// reusa um SlotID enquanto o slot exist no dir.
func (sp *SlottedPage) Insert(rh RecordHeader, doc []byte) (uint16, error) {
	recordSize := RecordHeaderSize + len(doc)
	needed := SlotSize + recordSize

	h := sp.header()
	if sp.FreeSpace() < needed {
		return 0, fmt.Errorf("%w: needs %d bytes, has %d", ErrPageFull, needed, sp.FreeSpace())
	}
	if recordSize > 0xFFFF {
		return 0, fmt.Errorf("heap/v2: record of %d bytes exceeds uint16 limit", recordSize)
	}

	// Novo record vai em freeSpaceEnd - recordSize, crescendo pra trás.
	newRecordOffset := h.freeSpaceEnd - uint16(recordSize)

	// Grava o header do record e o doc.
	encodeRecordHeader(&rh, sp.body[newRecordOffset:newRecordOffset+RecordHeaderSize])
	copy(sp.body[newRecordOffset+RecordHeaderSize:newRecordOffset+uint16(recordSize)], doc)

	// Adiciona o slot no dir.
	slotID := h.numSlots
	sp.writeSlot(slotID, newRecordOffset, uint16(recordSize))

	// Atualiza header.
	h.numSlots++
	if rh.Valid {
		h.numValid++
	}
	h.freeSpaceStart += SlotSize
	h.freeSpaceEnd = newRecordOffset
	sp.writeHeader(h)

	return slotID, nil
}

// Compact reclaim o espaço dos slots cujo record foi deleted
// (Valid=false) e cujo DeleteLSN <= minLSN. O SlotID dos vacuumados
// permanece no dir (marcado com length=0, Read devolve ErrVacuumed) —
// assim referências externas (ex: indexs B+ tree) continuam válidas,
// só que agora apontam pra "gone". Chain walks mustm tratar como fim.
//
// Retorna o número de slots vacuumados nesta operação.
//
// A região de records é rewrite por atrás: sobreviventes são empacotados
// do fim pro início, preservando a convenção "newer=deeper". O slot dir
// é atualizado com os novos offsets — SlotIDs NOT mudam.
func (sp *SlottedPage) Compact(minLSN uint64) (int, error) {
	h := sp.header()
	if h.numSlots == 0 {
		return 0, nil
	}

	// Primeira passada: classifica cada slot.
	type survivor struct {
		slotID         uint16
		offset, length uint16
	}
	survivors := make([]survivor, 0, h.numSlots)
	vacuumed := 0

	for i := uint16(0); i < h.numSlots; i++ {
		offset, length := sp.readSlot(i)
		if length == 0 {
			// Já foi vacuumado numa chamada anterior — ignora.
			continue
		}
		if length < RecordHeaderSize {
			return 0, ErrBadRecord
		}

		var rh RecordHeader
		decodeRecordHeader(&rh, sp.body[offset:offset+RecordHeaderSize])

		safeToVacuum := !rh.Valid && rh.DeleteLSN > 0 && rh.DeleteLSN <= minLSN
		if safeToVacuum {
			sp.writeSlot(i, 0, 0)
			vacuumed++
			continue
		}
		survivors = append(survivors, survivor{slotID: i, offset: offset, length: length})
	}

	if vacuumed == 0 {
		return 0, nil
	}

	// Reescreve a região de records num buffer temporário, depois copia
	// de volta. Evita copias sobrepostas (que corromperiam dados).
	tmp := make([]byte, len(sp.body))
	currentPos := uint16(len(sp.body))
	// Empacota do maior slotID pro menor — preserva "newer=deeper".
	for i := len(survivors) - 1; i >= 0; i-- {
		s := survivors[i]
		currentPos -= s.length
		copy(tmp[currentPos:currentPos+s.length], sp.body[s.offset:s.offset+s.length])
		sp.writeSlot(s.slotID, currentPos, s.length)
	}

	// Zera a antiga região de records e copia a compactada.
	for i := int(h.freeSpaceEnd); i < len(sp.body); i++ {
		sp.body[i] = 0
	}
	copy(sp.body[currentPos:], tmp[currentPos:])

	h.freeSpaceEnd = currentPos
	// numValid not muda — tombstones vacuumados já estavam em "inválido".
	sp.writeHeader(h)

	return vacuumed, nil
}

// Iterate percorre TODOS os slots (válidos e invalids) na ordem do
// SlotID. Invariante crítico pro vacuum: o iterador precisa ver
// tombstones pra decidir se reclaim.
//
// Se `fn` retorna erro, a iteração para e o erro é propagado.
func (sp *SlottedPage) Iterate(fn func(slotID uint16, rh RecordHeader, doc []byte) error) error {
	n := sp.header().numSlots
	for i := uint16(0); i < n; i++ {
		doc, rh, err := sp.Read(i)
		if err != nil {
			return err
		}
		if err := fn(i, rh, doc); err != nil {
			return err
		}
	}
	return nil
}

// MarkDeleted é o delete lazy do MVCC: altera o header do record
// in-place (Valid=false, DeleteLSN=deleteLSN). Os bytes do doc e o
// resto do header (CreateLSN, PrevRecordID) permanecem — essencial
// para transações antigas continuarem lendo a versão.
func (sp *SlottedPage) MarkDeleted(slotID uint16, deleteLSN uint64) error {
	h := sp.header()
	if slotID >= h.numSlots {
		return fmt.Errorf("%w: slotID %d >= numSlots %d", ErrSlotNotFound, slotID, h.numSlots)
	}

	offset, length := sp.readSlot(slotID)
	if length == 0 {
		return ErrVacuumed
	}
	if length < RecordHeaderSize {
		return ErrBadRecord
	}

	var rh RecordHeader
	decodeRecordHeader(&rh, sp.body[offset:offset+RecordHeaderSize])

	// Se já está invalid, noop — not decrementa numValid duas vezes.
	if !rh.Valid {
		return nil
	}

	rh.Valid = false
	rh.DeleteLSN = deleteLSN
	encodeRecordHeader(&rh, sp.body[offset:offset+RecordHeaderSize])

	h.numValid--
	sp.writeHeader(h)
	return nil
}

func (sp *SlottedPage) MarkUndeleted(slotID uint16) error {
	h := sp.header()
	if slotID >= h.numSlots {
		return fmt.Errorf("%w: slotID %d >= numSlots %d", ErrSlotNotFound, slotID, h.numSlots)
	}

	offset, length := sp.readSlot(slotID)
	if length == 0 {
		return ErrVacuumed
	}
	if length < RecordHeaderSize {
		return ErrBadRecord
	}

	var rh RecordHeader
	decodeRecordHeader(&rh, sp.body[offset:offset+RecordHeaderSize])
	rh.Valid = true
	rh.DeleteLSN = 0
	encodeRecordHeader(&rh, sp.body[offset:offset+RecordHeaderSize])
	return nil
}

// Read devolve o doc e o header do slot indicado.
func (sp *SlottedPage) Read(slotID uint16) ([]byte, RecordHeader, error) {
	h := sp.header()
	if slotID >= h.numSlots {
		return nil, RecordHeader{}, fmt.Errorf("%w: slotID %d >= numSlots %d", ErrSlotNotFound, slotID, h.numSlots)
	}

	offset, length := sp.readSlot(slotID)
	if length == 0 {
		return nil, RecordHeader{}, ErrVacuumed
	}
	if length < RecordHeaderSize {
		return nil, RecordHeader{}, ErrBadRecord
	}

	var rh RecordHeader
	decodeRecordHeader(&rh, sp.body[offset:offset+RecordHeaderSize])

	docLen := int(length) - RecordHeaderSize
	doc := make([]byte, docLen)
	copy(doc, sp.body[offset+RecordHeaderSize:offset+length])

	return doc, rh, nil
}
