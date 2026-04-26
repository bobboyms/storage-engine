package v2

import "github.com/bobboyms/storage-engine/pkg/pagestore"

// RecordID é o identificador externo de um record no heap v2.
// Externamente é um `int64` (compatível com o tipo usado pelo B+ tree
// como `dataPtr`). Internamente, empacota (PageID, SlotID):
//
//   bits 63:    0 (sinal — sempre positivo pra RecordIDs válidos)
//   bits 62-16: PageID (47 bits — max 2^47 pages = 1 exabyte com 8KB)
//   bits 15-0:  SlotID (16 bits — max 65535 slots por page)
//
// Valores especiais:
//   - NoRecordID (-1): sentinela "sem versão anterior" (compatível com v1)
//   - 0: invalid (PageID 0 é reservado pelo PageFile)
const (
	slotIDBits uint64 = 16
	slotIDMask uint64 = (1 << slotIDBits) - 1

	// MaxSlotID é o maior SlotID possível. Páginas de 8KB raramente
	// chegam perto disso — são limitadas pelo espaço, not pelo número.
	MaxSlotID uint16 = uint16(slotIDMask)

	// MaxPageID é o maior PageID possível. 2^47 - 1 pages de 8KB
	// = ~1 exabyte — bem além do que qualquer disco comporta.
	MaxPageID pagestore.PageID = (1 << 47) - 1
)

// EncodeRecordID empacota (PageID, SlotID) em um int64.
// O chamador é responsável por garantir PageID > 0 e SlotID <= MaxSlotID.
func EncodeRecordID(pageID pagestore.PageID, slotID uint16) int64 {
	return int64(uint64(pageID)<<slotIDBits | uint64(slotID))
}

// DecodeRecordID extrai (PageID, SlotID) de um RecordID produzido por
// EncodeRecordID. Not valida — passar NoRecordID ou 0 produz resultados
// sem sentido, é responsabilidade do chamador testar antes.
func DecodeRecordID(rid int64) (pagestore.PageID, uint16) {
	u := uint64(rid)
	return pagestore.PageID(u >> slotIDBits), uint16(u & slotIDMask)
}
