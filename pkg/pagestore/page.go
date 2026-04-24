// Package pagestore implementa a primitiva de I/O page-based (Fase 1
// do plano em docs/page_based_migration_plan.md).
//
// Unidade fundamental: página de 8KB com header em claro + body
// (opcionalmente cifrado). Não há buffer pool nesta camada — isso é
// responsabilidade da Fase 2 (pkg/pagestore/bufferpool.go).
//
// Decisões de formato estão em docs/adr/001-page-format.md.
package pagestore

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// Constantes de formato. Mudar qualquer uma implica bump de Version.
const (
	PageSize   = 8192
	HeaderSize = 32
	BodySize   = PageSize - HeaderSize // 8160

	MagicV1   = 0x50414745 // ASCII "PAGE"
	VersionV1 = 1
)

// PageID identifica uma página dentro de um PageFile.
// 0 é inválido (reservado para facilitar "zero value = não alocado").
type PageID uint64

const InvalidPageID PageID = 0

// PageType classifica o conteúdo, permitindo que o mesmo PageFile
// hospede páginas heterogêneas (heap, índice, meta).
type PageType uint8

const (
	PageTypeFree PageType = iota
	PageTypeMeta
	PageTypeHeap
	PageTypeBTreeInternal
	PageTypeBTreeLeaf
)

// PageHeader ocupa os primeiros 32 bytes. Sempre em claro — recovery
// e diagnóstico precisam ler PageID/LSN/Type sem a chave de TDE.
type PageHeader struct {
	Magic    uint32   // 4
	Version  uint16   // 2
	Type     PageType // 1
	Flags    uint8    // 1
	PageID   PageID   // 8
	PageLSN  uint64   // 8
	Checksum uint32   // 4  CRC32-Castagnoli sobre o body EM DISCO
	Reserved uint32   // 4
}

func (h *PageHeader) Encode(buf []byte) {
	_ = buf[HeaderSize-1]
	binary.LittleEndian.PutUint32(buf[0:4], h.Magic)
	binary.LittleEndian.PutUint16(buf[4:6], h.Version)
	buf[6] = byte(h.Type)
	buf[7] = h.Flags
	binary.LittleEndian.PutUint64(buf[8:16], uint64(h.PageID))
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
	h.PageID = PageID(binary.LittleEndian.Uint64(buf[8:16]))
	h.PageLSN = binary.LittleEndian.Uint64(buf[16:24])
	h.Checksum = binary.LittleEndian.Uint32(buf[24:28])
	h.Reserved = binary.LittleEndian.Uint32(buf[28:32])
	return nil
}

// Page é o buffer em memória de uma página (8KB fixos).
type Page [PageSize]byte

func (p *Page) HeaderBytes() []byte { return p[:HeaderSize] }
func (p *Page) Body() []byte        { return p[HeaderSize:] }

// Reset zera a página inteira (útil quando um buffer do pool é reaproveitado).
func (p *Page) Reset() {
	for i := range p {
		p[i] = 0
	}
}

// AdvancePageLSN atualiza o campo PageLSN do header — apenas se `lsn` for
// MAIOR que o valor atual (LSN é monotonicamente crescente).
//
// Crítico pra recovery/ARIES: recovery usa pageLSN pra saber se uma mudança
// já foi aplicada à página (skip se pageLSN >= entry.LSN).
//
// Chamadores típicos: heap.v2 e btree.v2 ao modificar uma página.
func (p *Page) AdvancePageLSN(lsn uint64) {
	hdr, err := p.GetHeader()
	if err != nil {
		return
	}
	if lsn > hdr.PageLSN {
		hdr.PageLSN = lsn
		p.SetHeader(hdr)
	}
}

// SetHeader preenche os campos do header e grava. NÃO calcula checksum —
// isso é responsabilidade do PageFile quando vai escrever em disco.
func (p *Page) SetHeader(h PageHeader) {
	h.Encode(p.HeaderBytes())
}

// GetHeader decodifica os bytes do header atual.
func (p *Page) GetHeader() (PageHeader, error) {
	var h PageHeader
	err := h.Decode(p.HeaderBytes())
	return h, err
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// checksum calcula o CRC32-Castagnoli sobre um slice arbitrário.
func checksum(b []byte) uint32 {
	return crc32.Checksum(b, crcTable)
}
