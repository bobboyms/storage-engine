package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/bobboyms/storage-engine/pkg/crypto"
	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// osStat é um alias pra facilitar mocks; por ora é só os.Stat.
var osStat = os.Stat

var (
	ErrInvalidMagic      = errors.New("arquivo WAL inválido: magic number incorreto")
	ErrChecksumMismatch  = errors.New("corrupção de dados: checksum CRC32 inválido")
	ErrInvalidPayloadLen = errors.New("tamanho de payload inválido ou excessivo")
	ErrDecryptFailed     = errors.New("falha ao decifrar payload do WAL (chave inválida ou dado adulterado)")
)

// WALReader lê entradas sequenciais do log. Backend: pagestore.PageFile.
// Acumula bytes das páginas num buffer interno e parseia entries conforme
// aparecem completos; entries que cruzam páginas são remontados sem problema.
type WALReader struct {
	pf             *pagestore.PageFile
	nextPageID     pagestore.PageID // próxima página a carregar no buffer
	buffer         []byte           // bytes carregados mas ainda não consumidos
	usableBodySize int
	exhausted      bool // true quando já lemos todas as páginas de todos os segmentos
	paths          []string
	pathIndex      int
	cipher         crypto.Cipher
}

// NewWALReader cria um leitor sem TDE.
func NewWALReader(path string) (*WALReader, error) {
	return NewWALReaderWithCipher(path, nil)
}

// NewWALReaderWithCipher cria um leitor que decifra páginas via pagestore.
// Passe nil para `cipher` pra ler logs em claro.
//
// Falha com erro se o arquivo não existe (ao contrário do writer, que
// cria). Semântica esperada: leitor só trabalha com WAL existente.
func NewWALReaderWithCipher(path string, cipher crypto.Cipher) (*WALReader, error) {
	paths, err := SegmentPaths(path)
	if err != nil {
		if _, statErr := osStat(path); statErr != nil {
			return nil, statErr
		}
		return nil, err
	}
	if len(paths) == 0 {
		if _, err := osStat(path); err != nil {
			return nil, err
		}
		paths = []string{path}
	}
	return newWALReaderForPaths(paths, cipher)
}

func newSinglePathReader(path string, cipher crypto.Cipher) (*WALReader, error) {
	return newWALReaderForPaths([]string{path}, cipher)
}

func newWALReaderForPaths(paths []string, cipher crypto.Cipher) (*WALReader, error) {
	if len(paths) == 0 {
		return nil, os.ErrNotExist
	}
	pf, err := pagestore.NewPageFile(paths[0], cipher)
	if err != nil {
		return nil, fmt.Errorf("wal: abrir page file: %w", err)
	}

	return &WALReader{
		pf:             pf,
		nextPageID:     1, // pageID 0 é reservado pelo pagestore
		usableBodySize: pf.UsableBodySize(),
		paths:          paths,
		cipher:         cipher,
	}, nil
}

// ReadEntry lê a próxima entrada. Retorna io.EOF quando esgotou.
func (r *WALReader) ReadEntry() (*WALEntry, error) {
	// 1. Garante que temos bytes suficientes pra um header (24 bytes)
	for len(r.buffer) < HeaderSize {
		loaded, err := r.loadNextPage()
		if err != nil {
			return nil, err
		}
		if !loaded {
			// Sem mais páginas
			if len(r.buffer) == 0 {
				return nil, io.EOF
			}
			return nil, io.ErrUnexpectedEOF
		}
	}

	// 2. Parseia e valida header
	var header WALHeader
	header.Decode(r.buffer[:HeaderSize])

	if header.Magic != WALMagic {
		return nil, ErrInvalidMagic
	}

	// Proteção contra alocação absurda
	if header.PayloadLen > 1024*1024*1024 {
		return nil, ErrInvalidPayloadLen
	}

	if header.PayloadLen == 0 {
		r.buffer = r.buffer[HeaderSize:]
		return &WALEntry{Header: header}, nil
	}

	total := HeaderSize + int(header.PayloadLen)

	// 3. Garante que temos o payload completo no buffer
	for len(r.buffer) < total {
		loaded, err := r.loadNextPage()
		if err != nil {
			return nil, err
		}
		if !loaded {
			return nil, io.ErrUnexpectedEOF // payload truncado
		}
	}

	// 4. Valida checksum do payload
	payload := r.buffer[HeaderSize:total]
	if !ValidateCRC32(payload, header.CRC32) {
		return nil, ErrChecksumMismatch
	}

	// 5. Constrói entry (copia payload pra não compartilhar buffer interno)
	entry := AcquireEntry()
	entry.Header = header
	if uint32(cap(entry.Payload)) < header.PayloadLen {
		entry.Payload = make([]byte, header.PayloadLen)
	} else {
		entry.Payload = entry.Payload[:header.PayloadLen]
	}
	copy(entry.Payload, payload)

	// 6. Consome bytes do buffer
	r.buffer = r.buffer[total:]
	return entry, nil
}

// loadNextPage carrega a próxima página no buffer. Retorna (true, nil)
// se carregou; (false, nil) se não há mais páginas; (false, err) em erro.
//
// Semântica de erros:
//   - PageOutOfRange (i.e., passamos do fim) → EOF limpo, sem erro
//   - Checksum mismatch → ErrChecksumMismatch (possível tamper/bit-flip)
//   - Decrypt failed → ErrDecryptFailed (chave errada ou tamper autenticado)
//   - Magic inválido → ErrInvalidMagic (não é um WAL ou corrompido cedo)
func (r *WALReader) loadNextPage() (bool, error) {
	if r.exhausted {
		return false, nil
	}

	numPages := r.pf.NumPages()
	if uint64(r.nextPageID) >= numPages {
		if err := r.openNextSegment(); err != nil {
			return false, err
		}
		if r.exhausted {
			return false, nil
		}
		return r.loadNextPage()
	}

	page, err := r.pf.ReadPage(r.nextPageID)
	if err != nil {
		r.exhausted = true
		// Mapeia erros de pagestore pra erros do WAL, preservando a
		// categoria semântica (importante pra monitoring/alerting).
		switch {
		case errors.Is(err, pagestore.ErrPageOutOfRange):
			return false, nil // EOF limpo
		case errors.Is(err, pagestore.ErrChecksumMismatch):
			return false, ErrChecksumMismatch
		case errors.Is(err, pagestore.ErrDecryptFailed):
			return false, fmt.Errorf("%w: %v", ErrDecryptFailed, err)
		case errors.Is(err, pagestore.ErrInvalidMagic):
			return false, ErrInvalidMagic
		default:
			return false, fmt.Errorf("wal: read page %d: %w", r.nextPageID, err)
		}
	}

	bytesUsed := binary.LittleEndian.Uint16(page.Body()[0:2])
	if int(bytesUsed) > r.usableBodySize-walPageHeaderSize {
		r.exhausted = true
		return false, fmt.Errorf("wal: bytesUsed %d na página %d excede limite", bytesUsed, r.nextPageID)
	}

	if bytesUsed > 0 {
		start := walPageHeaderSize
		r.buffer = append(r.buffer, page.Body()[start:start+int(bytesUsed)]...)
	}
	r.nextPageID++
	return true, nil
}

func (r *WALReader) openNextSegment() error {
	if r.pathIndex+1 >= len(r.paths) {
		r.exhausted = true
		return nil
	}
	if r.pf != nil {
		if err := r.pf.Close(); err != nil {
			return err
		}
	}
	r.pathIndex++
	pf, err := pagestore.NewPageFile(r.paths[r.pathIndex], r.cipher)
	if err != nil {
		r.exhausted = true
		return fmt.Errorf("wal: abrir segmento %s: %w", r.paths[r.pathIndex], err)
	}
	r.pf = pf
	r.nextPageID = 1
	r.usableBodySize = pf.UsableBodySize()
	return nil
}

// Close fecha o page file.
func (r *WALReader) Close() error {
	if r.pf == nil {
		return nil
	}
	return r.pf.Close()
}
