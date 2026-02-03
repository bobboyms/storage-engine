package wal

import (
	"encoding/binary"
	"io"
)

// Constantes para Header e Tipos
const (
	HeaderSize = 24 // Tamanho fixo do Header em bytes
	WALVersion = 1  // Versão atual do formato WAL

	// Magic Number para validação rápida (0xDEADBEEF)
	WALMagic = 0xDEADBEEF
)

// Tipos de Operação (EntryType)
const (
	EntryInsert uint8 = iota + 1 // 1: Insert
	EntryUpdate                  // 2: Update
	EntryDelete                  // 3: Delete
	EntryBegin                   // 4: Begin Transaction
	EntryCommit                  // 5: Commit
	EntryAbort                   // 6: Rollback
)

// WALHeader cabeçalho de 24 bytes para cada entrada
type WALHeader struct {
	Magic      uint32 // 4 bytes
	Version    uint8  // 1 byte
	EntryType  uint8  // 1 byte
	Reserved   uint16 // 2 bytes (padding/alinhamento)
	LSN        uint64 // 8 bytes (Log Sequence Number)
	PayloadLen uint32 // 4 bytes
	CRC32      uint32 // 4 bytes
}

// WALEntry representa uma entrada completa no log
type WALEntry struct {
	Header  WALHeader
	Payload []byte
}

// EncodeHeader serializa o header para um byte slice
func (h *WALHeader) Encode(buf []byte) {
	binary.LittleEndian.PutUint32(buf[0:4], h.Magic)
	buf[4] = h.Version
	buf[5] = h.EntryType
	binary.LittleEndian.PutUint16(buf[6:8], h.Reserved)
	binary.LittleEndian.PutUint64(buf[8:16], h.LSN)
	binary.LittleEndian.PutUint32(buf[16:20], h.PayloadLen)
	binary.LittleEndian.PutUint32(buf[20:24], h.CRC32)
}

// DecodeHeader deserializa bytes para a struct Header
func (h *WALHeader) Decode(buf []byte) {
	h.Magic = binary.LittleEndian.Uint32(buf[0:4])
	h.Version = buf[4]
	h.EntryType = buf[5]
	h.Reserved = binary.LittleEndian.Uint16(buf[6:8])
	h.LSN = binary.LittleEndian.Uint64(buf[8:16])
	h.PayloadLen = binary.LittleEndian.Uint32(buf[16:20])
	h.CRC32 = binary.LittleEndian.Uint32(buf[20:24])
}

// WriteTo escreve a entrada (header + payload) para um writer
func (e *WALEntry) WriteTo(w io.Writer) (int64, error) {
	var headerBuf [HeaderSize]byte
	e.Header.Encode(headerBuf[:])

	// Escreve Header
	n, err := w.Write(headerBuf[:])
	if err != nil {
		return int64(n), err
	}

	// Escreve Payload
	m, err := w.Write(e.Payload)
	return int64(n + m), err
}
