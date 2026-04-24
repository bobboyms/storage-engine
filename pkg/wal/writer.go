package wal

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bobboyms/storage-engine/pkg/crypto"
	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// Layout de uma página WAL (dentro do body de uma pagestore.Page):
//
//	offset 0..1  — bytesUsed (uint16): bytes preenchidos após o header
//	offset 2..3  — reservado pra futuro (flags, etc)
//	offset 4+    — bytes brutos de entries; entries podem cruzar páginas
//
// O que cifra a página, o checksum CRC32 sobre o body, a validação de
// magic — tudo vem do pagestore.PageFile. O WAL só gerencia o stream
// lógico de entries dentro do body útil.
const (
	walPageHeaderSize = 4
)

// WALWriter gerencia a escrita no log.
//
// Backend: pagestore.PageFile (pages de 8KB). Cada WriteEntry pode
// encher a página atual e alocar outra. O buffer em memória é a página
// atual sendo preenchida — fsyncada quando:
//   - SyncEveryWrite: a cada WriteEntry
//   - SyncInterval:   background ticker
//   - SyncBatch:      quando N bytes de entries foram escritos
type WALWriter struct {
	mu      sync.Mutex
	pf      *pagestore.PageFile
	options Options

	// Página atualmente sendo preenchida (em memória).
	// currentOffset aponta pro próximo byte livre dentro do body da
	// currentPage, relativo a body[0]. Sempre >= walPageHeaderSize.
	currentPage      pagestore.Page
	currentPageID    pagestore.PageID
	currentOffset    uint16
	currentPageDirty bool // true se há bytes não flushados na currentPage

	// Limite de bytes úteis por página (depende da cifra do pagestore)
	usableBodySize int

	// Estado pra SyncBatch
	batchBytes int64

	// Controle de threads
	done   chan struct{}
	ticker *time.Ticker
	closed atomic.Bool
}

// NewWALWriter cria um novo Writer. Abre o arquivo via pagestore
// (aplicando cipher se configurado em `opts.Cipher`).
func NewWALWriter(path string, opts Options) (*WALWriter, error) {
	pf, err := pagestore.NewPageFile(path, opts.Cipher)
	if err != nil {
		return nil, fmt.Errorf("wal: abrir page file: %w", err)
	}

	w := &WALWriter{
		pf:             pf,
		options:        opts,
		usableBodySize: pf.UsableBodySize(),
		done:           make(chan struct{}),
	}

	// Detecta se estamos reabrindo arquivo existente ou criando novo.
	// pf.NumPages() == 1 significa só o slot 0 reservado (arquivo vazio).
	if pf.NumPages() > 1 {
		// Reabrir: busca última página e continua preenchendo onde parou.
		if err := w.adoptLastPage(); err != nil {
			pf.Close()
			return nil, err
		}
	} else {
		// Novo: aloca primeira página.
		if err := w.allocateNewPage(); err != nil {
			pf.Close()
			return nil, err
		}
	}

	// Background sync pra política Interval
	if opts.SyncPolicy == SyncInterval {
		w.ticker = time.NewTicker(opts.SyncIntervalDuration)
		go w.backgroundSync()
	}

	return w, nil
}

// Path devolve o caminho do arquivo WAL.
func (w *WALWriter) Path() string {
	return w.pf.Path()
}

// Cipher devolve o cipher usado para cifrar/decifrar as páginas do WAL.
// Pode ser nil quando TDE está desligado. Storage/recovery usa isso para
// abrir WALReader compatível com o writer configurado.
func (w *WALWriter) Cipher() crypto.Cipher {
	return w.options.Cipher
}

// WriteEntry serializa `entry` e escreve na página atual, alocando
// novas páginas quando necessário. Aplica a política de sync.
func (w *WALWriter) WriteEntry(entry *WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed.Load() {
		return fmt.Errorf("wal: writer fechado")
	}

	// Serializa header + payload num buffer (headerSize + payloadLen bytes)
	buf := make([]byte, HeaderSize+len(entry.Payload))
	entry.Header.Encode(buf[:HeaderSize])
	copy(buf[HeaderSize:], entry.Payload)

	// Escreve byte-a-byte, cruzando páginas se preciso.
	if err := w.appendBytes(buf); err != nil {
		return err
	}

	w.batchBytes += int64(len(buf))

	// Política de sync
	switch w.options.SyncPolicy {
	case SyncEveryWrite:
		return w.syncLocked()
	case SyncBatch:
		if w.batchBytes >= w.options.SyncBatchBytes {
			return w.syncLocked()
		}
	}
	return nil
}

// appendBytes escreve `data` na stream lógica, alocando páginas conforme
// necessário. Caller deve segurar w.mu.
func (w *WALWriter) appendBytes(data []byte) error {
	for len(data) > 0 {
		spaceInPage := uint16(w.usableBodySize) - w.currentOffset
		if spaceInPage == 0 {
			// Página cheia: flush, aloca nova.
			if err := w.flushCurrentPageLocked(); err != nil {
				return err
			}
			if err := w.allocateNewPage(); err != nil {
				return err
			}
			spaceInPage = uint16(w.usableBodySize) - w.currentOffset
		}

		take := uint16(len(data))
		if take > spaceInPage {
			take = spaceInPage
		}
		copy(w.currentPage.Body()[w.currentOffset:w.currentOffset+take], data[:take])
		w.currentOffset += take
		data = data[take:]

		// Atualiza bytesUsed no header da página
		bytesUsed := w.currentOffset - walPageHeaderSize
		binary.LittleEndian.PutUint16(w.currentPage.Body()[0:2], bytesUsed)
		w.currentPageDirty = true
	}
	return nil
}

// allocateNewPage aloca uma nova página do pagestore e inicializa o
// header WAL (bytesUsed = 0). Caller deve segurar w.mu.
func (w *WALWriter) allocateNewPage() error {
	pid, err := w.pf.AllocatePage()
	if err != nil {
		return fmt.Errorf("wal: alocar página: %w", err)
	}
	w.currentPageID = pid
	w.currentPage = pagestore.Page{}
	w.currentPage.Reset()
	// Grava walPageHeader zerado (bytesUsed=0) no body
	binary.LittleEndian.PutUint16(w.currentPage.Body()[0:2], 0)
	w.currentOffset = walPageHeaderSize
	w.currentPageDirty = true // precisa ser escrita ao menos uma vez
	return nil
}

// adoptLastPage carrega a última página do pf como `currentPage` e
// posiciona currentOffset após os bytes já escritos. Permite continuar
// appending num arquivo reaberto.
func (w *WALWriter) adoptLastPage() error {
	lastPageID := pagestore.PageID(w.pf.NumPages() - 1)
	page, err := w.pf.ReadPage(lastPageID)
	if err != nil {
		return fmt.Errorf("wal: ler última página: %w", err)
	}
	w.currentPage = *page
	w.currentPageID = lastPageID
	bytesUsed := binary.LittleEndian.Uint16(page.Body()[0:2])
	w.currentOffset = walPageHeaderSize + bytesUsed
	if int(w.currentOffset) > w.usableBodySize {
		return fmt.Errorf("wal: bytesUsed %d excede usableBody %d", bytesUsed, w.usableBodySize-walPageHeaderSize)
	}
	w.currentPageDirty = false
	return nil
}

// flushCurrentPageLocked escreve a página atual no pagestore (se dirty).
// NÃO chama fsync — isso é só pra garantir que o pagestore recebeu os
// bytes. Caller deve segurar w.mu.
func (w *WALWriter) flushCurrentPageLocked() error {
	if !w.currentPageDirty {
		return nil
	}
	if err := w.pf.WritePage(w.currentPageID, &w.currentPage); err != nil {
		return fmt.Errorf("wal: escrever página %d: %w", w.currentPageID, err)
	}
	w.currentPageDirty = false
	return nil
}

// Sync força a persistência em disco: escreve a página atual + fsync.
func (w *WALWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncLocked()
}

func (w *WALWriter) syncLocked() error {
	if err := w.flushCurrentPageLocked(); err != nil {
		return err
	}
	if err := w.pf.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	w.batchBytes = 0
	return nil
}

// Close fecha o writer: flush final + fsync + fecha page file.
func (w *WALWriter) Close() error {
	if !w.closed.CompareAndSwap(false, true) {
		return nil
	}

	if w.ticker != nil {
		w.ticker.Stop()
		close(w.done)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush final (pode falhar se disk full; tentamos fechar mesmo assim)
	syncErr := w.syncLocked()
	closeErr := w.pf.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// WriteCheckpointRecord grava um registro de checkpoint fuzzy no WAL.
// `beginLSN` é o LSN capturado no início do checkpoint — recovery pode
// pular entradas com LSN < beginLSN porque as páginas sujas naquele
// momento foram garantidamente flushadas ao disco antes desta chamada.
func (w *WALWriter) WriteCheckpointRecord(beginLSN uint64) error {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, beginLSN)

	entry := AcquireEntry()
	entry.Header.Magic = WALMagic
	entry.Header.Version = WALVersion
	entry.Header.EntryType = EntryCheckpoint
	entry.Header.LSN = beginLSN
	entry.Header.PayloadLen = 8
	entry.Header.CRC32 = CalculateCRC32(payload)
	entry.Payload = append(entry.Payload[:0], payload...)

	err := w.WriteEntry(entry)
	ReleaseEntry(entry)

	if err != nil {
		return fmt.Errorf("wal: escrever checkpoint record: %w", err)
	}
	return w.Sync()
}

func (w *WALWriter) backgroundSync() {
	for {
		select {
		case <-w.ticker.C:
			// Thread-safe; Sync adquire lock internamente
			_ = w.Sync()
		case <-w.done:
			return
		}
	}
}
