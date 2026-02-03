package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
)

var (
	ErrInvalidMagic      = errors.New("arquivo WAL inválido: magic number incorreto")
	ErrChecksumMismatch  = errors.New("corrupção de dados: checksum CRC32 inválido")
	ErrInvalidPayloadLen = errors.New("tamanho de payload inválido ou excessivo")
)

// WALReader lê entradas do log sequencialmente
type WALReader struct {
	file   *os.File
	offset int64
}

// NewWALReader cria um leitor para um arquivo de log existente
func NewWALReader(path string) (*WALReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &WALReader{
		file: f,
	}, nil
}

// ReadEntry lê a próxima entrada do log.
// Retorna io.EOF quando não há mais dados.
func (r *WALReader) ReadEntry() (*WALEntry, error) {
	// 1. Ler Header (24 bytes)
	headerBuf := make([]byte, HeaderSize)
	n, err := io.ReadFull(r.file, headerBuf)
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("erro ao ler header: %w", err)
	}
	if n != HeaderSize {
		return nil, io.ErrUnexpectedEOF
	}

	// 2. Decodificar e Validar Header
	var header WALHeader
	header.Decode(headerBuf)

	if header.Magic != WALMagic {
		return nil, ErrInvalidMagic
	}

	if header.PayloadLen == 0 {
		// Entrada vazia? Apenas retornamos (mas verificamos checksum 0)
		return &WALEntry{Header: header}, nil
	}

	// Proteção contra alocação absurda (ex: leitura de lixo como tamanho)
	if header.PayloadLen > 1024*1024*1024 { // 1GB limit
		return nil, ErrInvalidPayloadLen
	}

	// 3. Ler Payload
	// Usamos o pool, mas precisamos garantir que o chamador vai liberar (ReleaseEntry)
	entry := AcquireEntry()
	entry.Header = header

	// Garante capacidade
	if uint32(cap(entry.Payload)) < header.PayloadLen {
		entry.Payload = make([]byte, header.PayloadLen)
	} else {
		entry.Payload = entry.Payload[:header.PayloadLen]
	}

	n, err = io.ReadFull(r.file, entry.Payload)
	if err != nil {
		// Se falhar aqui, devolvemos ao pool antes de retornar erro para evitar leak
		ReleaseEntry(entry)
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF // Payload truncado
		}
		return nil, err
	}

	// 4. Validar Checksum
	if !ValidateCRC32(entry.Payload, header.CRC32) {
		ReleaseEntry(entry)
		return nil, ErrChecksumMismatch
	}

	r.offset += int64(HeaderSize + header.PayloadLen)
	return entry, nil
}

// Close fecha o arquivo
func (r *WALReader) Close() error {
	return r.file.Close()
}
