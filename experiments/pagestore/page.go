// Package pagestore é um PROTÓTIPO DESCARTÁVEL (Fase 0 do plano de migração
// page-based). Objetivo: validar decisões de design e medir overhead de
// criptografia page-a-page before de iniciar o refactor real em pkg/.
//
// NÃO IMPORTE deste pacote a partir de pkg/ ou cmd/.
package pagestore

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// Tamanhos (constbefore de design a serem confirmadas pela ADR).
const (
	PageSize   = 8192
	HeaderSize = 32
	BodySize   = PageSize - HeaderSize // 8160

	MagicV1    = 0x50414745 // ASCII "PAGE"
	VersionV1  = 1
	PageInvalid = ^uint64(0)
)

// PageType classifica o conteúdo da page — permite evoluir o formato
// (heap, index, meta) compartilhando a mesma infra de I/O/encryption.
type PageType uint8

const (
	PageTypeFree PageType = iota
	PageTypeMeta
	PageTypeHeap
	PageTypeBTreeInternal
	PageTypeBTreeLeaf
)

// PageHeader ocupa os primeiros 32 bytes da page — sempre em plaintext.
// O checksum é sobre o CONTEÚDO EM DISCO (ciphertext quando há encryption),
// permitindo detectar corrupção before de tentar deencryptionr (fast fail).
type PageHeader struct {
	Magic    uint32   // 4
	Version  uint16   // 2
	Type     PageType // 1
	Flags    uint8    // 1
	PageID   uint64   // 8
	PageLSN  uint64   // 8
	Checksum uint32   // 4  (CRC32-Castagnoli sobre o body em disco)
	Reserved uint32   // 4
}

func (h *PageHeader) Encode(buf []byte) {
	_ = buf[31] // bounds hint
	binary.LittleEndian.PutUint32(buf[0:4], h.Magic)
	binary.LittleEndian.PutUint16(buf[4:6], h.Version)
	buf[6] = byte(h.Type)
	buf[7] = h.Flags
	binary.LittleEndian.PutUint64(buf[8:16], h.PageID)
	binary.LittleEndian.PutUint64(buf[16:24], h.PageLSN)
	binary.LittleEndian.PutUint32(buf[24:28], h.Checksum)
	binary.LittleEndian.PutUint32(buf[28:32], h.Reserved)
}

func (h *PageHeader) Decode(buf []byte) error {
	if len(buf) < HeaderSize {
		return fmt.Errorf("pagestore: buffer de %d bytes < HeaderSize %d", len(buf), HeaderSize)
	}
	h.Magic = binary.LittleEndian.Uint32(buf[0:4])
	h.Version = binary.LittleEndian.Uint16(buf[4:6])
	h.Type = PageType(buf[6])
	h.Flags = buf[7]
	h.PageID = binary.LittleEndian.Uint64(buf[8:16])
	h.PageLSN = binary.LittleEndian.Uint64(buf[16:24])
	h.Checksum = binary.LittleEndian.Uint32(buf[24:28])
	h.Reserved = binary.LittleEndian.Uint32(buf[28:32])
	return nil
}

// Page é o buffer físico de uma page em memória (8KB).
type Page [PageSize]byte

// Body aponta para os bytes after o header (8160 bytes).
func (p *Page) Body() []byte { return p[HeaderSize:] }

// HeaderBytes aponta para os 32 bytes iniciais.
func (p *Page) HeaderBytes() []byte { return p[:HeaderSize] }

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ComputeChecksum calcula o CRC32 sobre o body atual.
func (p *Page) ComputeChecksum() uint32 {
	return crc32.Checksum(p.Body(), crcTable)
}
