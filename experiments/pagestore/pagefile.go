package pagestore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/bobboyms/storage-engine/pkg/crypto"
)

var (
	ErrInvalidMagic     = errors.New("pagestore: magic inválido — arquivo corrompido ou não é page file")
	ErrChecksumMismatch = errors.New("pagestore: checksum inválido — página corrompida")
	ErrDecryptFailed    = errors.New("pagestore: falha ao decifrar (chave errada ou tamper)")
	ErrPageOutOfRange   = errors.New("pagestore: pageID fora do intervalo alocado")
)

// UsableBodySize devolve quantos bytes de payload cabem no body após
// descontar o overhead da cifra (se houver). Chamadores devem consultar
// esse valor em vez de assumir BodySize.
func UsableBodySize(cipher crypto.Cipher) int {
	if cipher == nil {
		return BodySize
	}
	return BodySize - cipher.Overhead()
}

// PageFile é a primitiva de I/O de páginas fixas (8KB) com cifra opcional.
// Nesta fase não há buffer pool — cada Read/Write vai direto ao disco.
// Medidas de throughput nesta fase refletem o caso SEM cache.
type PageFile struct {
	path   string
	file   *os.File
	cipher crypto.Cipher

	mu      sync.Mutex // serializa writes; reads usam pread via ReadAt
	nextID  atomic.Uint64
	numPages atomic.Uint64
}

// OpenPageFile abre ou cria um arquivo de páginas. Passe nil para `cipher`
// para desligar TDE.
func OpenPageFile(path string, cipher crypto.Cipher) (*PageFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	pf := &PageFile{
		path:   path,
		file:   f,
		cipher: cipher,
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	n := uint64(stat.Size() / PageSize)
	pf.nextID.Store(n)
	pf.numPages.Store(n)

	return pf, nil
}

// Allocate reserva um novo pageID sem escrever no disco — a gravação
// só acontece no primeiro WritePage.
func (pf *PageFile) Allocate() uint64 {
	id := pf.nextID.Add(1) - 1
	return id
}

// NumPages devolve quantas páginas foram gravadas até agora.
func (pf *PageFile) NumPages() uint64 { return pf.numPages.Load() }

// WritePage grava a página `p` no offset correspondente a `pageID`.
// O header é escrito em claro; o body é cifrado quando cipher != nil.
// O Checksum do header é recalculado sobre o body EM DISCO (ciphertext).
func (pf *PageFile) WritePage(pageID uint64, p *Page) error {
	var disk [PageSize]byte
	copy(disk[:], p[:])

	// Se TDE: cifra o body, mantém o header em claro.
	if pf.cipher != nil {
		var aad [8]byte
		binary.LittleEndian.PutUint64(aad[:], pageID)

		// Cabe (BodySize - overhead) bytes de plaintext
		usable := UsableBodySize(pf.cipher)
		plaintext := p.Body()[:usable]

		enc, err := pf.cipher.Encrypt(plaintext, aad[:])
		if err != nil {
			return fmt.Errorf("pagestore: encrypt page %d: %w", pageID, err)
		}
		if len(enc) != BodySize {
			return fmt.Errorf("pagestore: ciphertext tem %d bytes, esperado %d", len(enc), BodySize)
		}
		copy(disk[HeaderSize:], enc)
	}

	// Header: preenche campos deriváveis antes de gravar.
	var hdr PageHeader
	if err := hdr.Decode(p.HeaderBytes()); err != nil {
		return err
	}
	hdr.Magic = MagicV1
	hdr.Version = VersionV1
	hdr.PageID = pageID
	// Checksum sobre o body que de fato vai pro disco.
	hdr.Checksum = checksumBytes(disk[HeaderSize:])
	hdr.Encode(disk[:HeaderSize])

	offset := int64(pageID) * PageSize

	pf.mu.Lock()
	_, err := pf.file.WriteAt(disk[:], offset)
	pf.mu.Unlock()
	if err != nil {
		return err
	}

	// Atualiza contador de páginas (se gravamos além do fim)
	for {
		cur := pf.numPages.Load()
		want := pageID + 1
		if want <= cur || pf.numPages.CompareAndSwap(cur, want) {
			break
		}
	}
	return nil
}

// ReadPage lê a página `pageID` do disco. Valida magic, checksum e decifra
// (se cipher != nil). Retorna a página com o body EM CLARO.
func (pf *PageFile) ReadPage(pageID uint64) (*Page, error) {
	if pageID >= pf.numPages.Load() {
		return nil, ErrPageOutOfRange
	}

	var page Page
	offset := int64(pageID) * PageSize
	if _, err := pf.file.ReadAt(page[:], offset); err != nil {
		return nil, err
	}

	var hdr PageHeader
	if err := hdr.Decode(page.HeaderBytes()); err != nil {
		return nil, err
	}
	if hdr.Magic != MagicV1 {
		return nil, ErrInvalidMagic
	}

	// 1. Valida checksum ANTES de tentar decifrar — fast fail em corrupção.
	if checksumBytes(page.Body()) != hdr.Checksum {
		return nil, ErrChecksumMismatch
	}

	// 2. Decifra se TDE ligado.
	if pf.cipher != nil {
		var aad [8]byte
		binary.LittleEndian.PutUint64(aad[:], pageID)

		plaintext, err := pf.cipher.Decrypt(page.Body(), aad[:])
		if err != nil {
			return nil, fmt.Errorf("%w (page %d): %v", ErrDecryptFailed, pageID, err)
		}

		// Coloca o plaintext de volta no body, zerando o tail não usado.
		copy(page.Body(), plaintext)
		for i := len(plaintext); i < BodySize; i++ {
			page[HeaderSize+i] = 0
		}
	}

	return &page, nil
}

// Sync força fsync no arquivo subjacente.
func (pf *PageFile) Sync() error {
	pf.mu.Lock()
	defer pf.mu.Unlock()
	return pf.file.Sync()
}

// Close fecha o arquivo.
func (pf *PageFile) Close() error {
	pf.mu.Lock()
	defer pf.mu.Unlock()
	return pf.file.Close()
}

func checksumBytes(b []byte) uint32 {
	// Reusa a mesma tabela de page.go
	return crc32Sum(b)
}
