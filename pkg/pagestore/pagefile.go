package pagestore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/bobboyms/storage-engine/pkg/crypto"
)

var (
	ErrInvalidMagic     = errors.New("pagestore: magic inválido — arquivo corrompido ou não é page file")
	ErrChecksumMismatch = errors.New("pagestore: checksum inválido — página corrompida")
	ErrDecryptFailed    = errors.New("pagestore: falha ao decifrar (chave errada ou tamper)")
	ErrPageOutOfRange   = errors.New("pagestore: pageID fora do intervalo alocado")
	ErrClosed           = errors.New("pagestore: PageFile fechado")
)

// PageFile é a primitiva de I/O de páginas fixas de 8KB.
//
// Concorrência: leituras e escritas usam pread/pwrite (ReadAt/WriteAt) —
// seguras para goroutines múltiplas em offsets disjuntos. Allocate é
// atômico. Close usa um flag atômico; operações depois de Close falham
// com ErrClosed em vez de tocar um descritor de arquivo liberado.
//
// Não há buffer pool aqui — cada Read/Write vai direto ao disco (via
// page cache do SO). Cache é responsabilidade do BufferPool (Fase 2).
type PageFile struct {
	path   string
	file   *os.File
	cipher *PageCipher

	// nextID é incrementado atomicamente por Allocate.
	// numPages reflete o maior pageID + 1 que já foi escrito.
	nextID   atomic.Uint64
	numPages atomic.Uint64

	// syncMu serializa apenas Sync() — ReadAt/WriteAt em offsets distintos
	// não precisam de lock.
	syncMu sync.Mutex

	closed atomic.Bool
}

// NewPageFile abre ou cria um page file em `path`. Passe nil para
// `cipher` para desligar TDE (o arquivo fica com o body em claro).
//
// Durability: se o arquivo é CRIADO (não existia antes), faz fsync do
// diretório pai — sem isso a criação pode ser "esquecida" pelo FS em
// caso de crash mesmo após a função retornar.
func NewPageFile(path string, cipher crypto.Cipher) (*PageFile, error) {
	// Detecta se vamos criar o arquivo pela primeira vez
	_, statErr := os.Stat(path)
	creating := os.IsNotExist(statErr)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if stat.Size()%PageSize != 0 {
		f.Close()
		return nil, fmt.Errorf("pagestore: tamanho do arquivo %d não é múltiplo de PageSize %d", stat.Size(), PageSize)
	}

	// fsync do diretório pai quando o arquivo foi criado agora. Garante
	// que a entry no diretório persiste além de crash imediato após
	// NewPageFile retornar.
	if creating {
		if err := fsyncDir(filepath.Dir(path)); err != nil {
			f.Close()
			return nil, fmt.Errorf("pagestore: fsync dir após create: %w", err)
		}
	}

	pf := &PageFile{
		path:   path,
		file:   f,
		cipher: NewPageCipher(cipher),
	}

	// PageID 0 é reservado (InvalidPageID). O próximo a alocar é o que
	// corresponde ao fim do arquivo (ou 1 se estiver vazio).
	n := uint64(stat.Size() / PageSize)
	if n == 0 {
		n = 1 // reserva o slot 0
	}
	pf.nextID.Store(n)
	pf.numPages.Store(n)

	return pf, nil
}

// AllocatePage reserva um novo pageID. Não grava nada em disco — a
// primeira gravação de verdade acontece em WritePage.
func (pf *PageFile) AllocatePage() (PageID, error) {
	if pf.closed.Load() {
		return InvalidPageID, ErrClosed
	}
	id := PageID(pf.nextID.Add(1) - 1)
	return id, nil
}

// NumPages devolve quantas páginas já foram persistidas (inclui o slot 0).
func (pf *PageFile) NumPages() uint64 { return pf.numPages.Load() }

// UsableBodySize devolve quantos bytes do body de cada página ficam
// disponíveis para payload depois de descontado o overhead da cifra.
// Sem cifra = BodySize (8160). Com AES-GCM = BodySize - 28.
// Quem monta payloads dentro da página (ex: SlottedPage) deve respeitar
// esse limite para não perder bytes na cifragem.
func (pf *PageFile) UsableBodySize() int { return pf.cipher.UsableBodySize() }

// Path devolve o caminho do arquivo.
func (pf *PageFile) Path() string { return pf.path }

// WritePage grava a página `p` no offset correspondente a `pageID`.
// O header é escrito em claro (com Magic, Version, PageID e Checksum
// recalculados). O body é cifrado se TDE estiver ligado.
func (pf *PageFile) WritePage(pageID PageID, p *Page) error {
	if pf.closed.Load() {
		return ErrClosed
	}
	if pageID == InvalidPageID {
		return fmt.Errorf("pagestore: pageID 0 é reservado")
	}

	// Monta o buffer on-disk num array local de 8KB.
	// No caminho NoOp isso é uma cópia; no caminho cifrado a cópia do
	// body é descartada e substituída pelo ciphertext.
	var disk [PageSize]byte
	copy(disk[:], p[:])

	if !pf.cipher.IsNoOp() {
		plaintext := p.Body()[:pf.cipher.UsableBodySize()]
		enc, err := pf.cipher.EncryptBody(plaintext, pageID)
		if err != nil {
			return fmt.Errorf("pagestore: encrypt page %d: %w", pageID, err)
		}
		if len(enc) != BodySize {
			return fmt.Errorf("pagestore: ciphertext tem %d bytes, esperado %d", len(enc), BodySize)
		}
		copy(disk[HeaderSize:], enc)
	}

	// Preenche header com campos obrigatórios (preservando Type/Flags/LSN
	// que o chamador possa ter definido) e calcula o checksum sobre o
	// body exatamente como vai pro disco.
	var hdr PageHeader
	if err := hdr.Decode(p.HeaderBytes()); err != nil {
		return err
	}
	hdr.Magic = MagicV1
	hdr.Version = VersionV1
	hdr.PageID = pageID
	hdr.Checksum = checksum(disk[HeaderSize:])
	hdr.Encode(disk[:HeaderSize])

	offset := int64(pageID) * PageSize
	if _, err := pf.file.WriteAt(disk[:], offset); err != nil {
		return err
	}

	// Atualiza numPages se gravamos além do limite atual.
	for {
		cur := pf.numPages.Load()
		want := uint64(pageID) + 1
		if want <= cur || pf.numPages.CompareAndSwap(cur, want) {
			break
		}
	}
	return nil
}

// ReadPage lê e valida a página `pageID`. Valida magic + checksum
// antes de tentar decifrar (fast fail em corrupção). Retorna a página
// com o body EM CLARO.
func (pf *PageFile) ReadPage(pageID PageID) (*Page, error) {
	if pf.closed.Load() {
		return nil, ErrClosed
	}
	if pageID == InvalidPageID {
		return nil, fmt.Errorf("pagestore: pageID 0 é reservado")
	}
	if uint64(pageID) >= pf.numPages.Load() {
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
	if checksum(page.Body()) != hdr.Checksum {
		return nil, ErrChecksumMismatch
	}
	if hdr.PageID != pageID {
		// Defesa contra swap de páginas inteiras (header + body).
		// AAD pega swap apenas de body; aqui pegamos swap completo.
		return nil, fmt.Errorf("pagestore: pageID %d no header não bate com offset %d", hdr.PageID, pageID)
	}

	if !pf.cipher.IsNoOp() {
		plaintext, err := pf.cipher.DecryptBody(page.Body(), pageID)
		if err != nil {
			return nil, fmt.Errorf("%w (page %d): %v", ErrDecryptFailed, pageID, err)
		}
		// Coloca plaintext de volta no body. Tail fica zerado.
		copy(page.Body(), plaintext)
		for i := len(plaintext); i < BodySize; i++ {
			page[HeaderSize+i] = 0
		}
	}

	return &page, nil
}

// Sync força fsync no arquivo.
func (pf *PageFile) Sync() error {
	if pf.closed.Load() {
		return ErrClosed
	}
	pf.syncMu.Lock()
	defer pf.syncMu.Unlock()
	return syncFile(pf.file)
}

// Close fecha o arquivo. Operações subsequentes falham com ErrClosed.
// É idempotente — Close() duas vezes não é erro.
//
// Durability: antes de fechar, chama fsync pra garantir que os writes
// pendentes foram persistidos. Sem isso, um crash imediatamente após
// o processo terminar "limpo" pode perder as últimas escritas.
func (pf *PageFile) Close() error {
	if !pf.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Tenta fsync — se falhar (ex: disk full), ainda tentamos fechar
	// pra não vazar descritor, mas propagamos o erro do fsync.
	syncErr := syncFile(pf.file)
	closeErr := pf.file.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
