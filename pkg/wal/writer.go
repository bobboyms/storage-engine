package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bobboyms/storage-engine/pkg/crypto"
	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// Layout de uma page WAL (dentro do body de uma pagestore.Page):
//
//	offset 0..1  — bytesUsed (uint16): bytes preenchidos after o header
//	offset 2..3  — reservado pra futuro (flags, etc)
//	offset 4+    — bytes brutos de entries; entries podem cruzar pages
//
// O que cifra a page, o checksum CRC32 sobre o body, a validação de
// magic — tudo vem do pagestore.PageFile. O WAL só gerencia o stream
// lógico de entries dentro do body útil.
const (
	walPageHeaderSize = 4
)

// WALWriter gerencia a write no log.
//
// Backend: pagestore.PageFile (pages de 8KB). Cada WriteEntry pode
// encher a page atual e alocar outra. O buffer em memória é a page
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
	currentPageDirty bool // true se há bytes not flushados na currentPage

	// Limite de bytes úteis por page (depende da cifra do pagestore)
	usableBodySize int

	// Estado pra SyncBatch
	batchBytes int64
	// Indica se o segmento ativo contém pelo menos uma entrada completa.
	segmentHasEntries bool

	// Group commit state. Concurrent SyncEveryWrite committers elect a
	// leader that issues one fsync covering every entry appended (and
	// page-flushed) before it started; followers wait on syncCond.
	//
	//   appendSeq — sequence assigned to each entry after its bytes were
	//               flushed to the OS (incremented under mu, read atomically)
	//   syncedSeq — highest sequence covered by a completed fsync
	//   syncInFlight — gate: one fsync (or pf swap/close) at a time
	//
	// syncedSeq, syncInFlight and bgSyncErr are guarded by syncMu.
	appendSeq    atomic.Uint64
	syncMu       sync.Mutex
	syncCond     *sync.Cond
	syncedSeq    uint64
	syncInFlight bool

	// bgSyncErr poisons the writer after any fsync failure (background
	// ticker or group-commit leader). Once the kernel reports an fsync
	// error, dirty pages may have been dropped — continuing to buffer
	// writes would silently lose them, so every subsequent WriteEntry/Sync
	// fails with this error until the writer is reopened. Guarded by syncMu.
	bgSyncErr error

	// Controle de threads
	done   chan struct{}
	ticker *time.Ticker
	closed atomic.Bool

	// syncCount counts real fsyncs issued to the page file
	// (observability and group-commit tests).
	syncCount atomic.Uint64

	// lockFile holds the advisory exclusive lock (<path>.lock) that
	// guarantees a single writer per WAL path across processes. Held for
	// the writer's lifetime; rotation renames the active segment but the
	// lock file name stays constant.
	lockFile *os.File
}

// NewWALWriter cria um novo Writer. Abre o arquivo via pagestore
// (aplicando cipher se configurado em `opts.Cipher`).
func NewWALWriter(path string, opts Options) (*WALWriter, error) {
	// Exclusive writer lock first: the tolerant open below mutates the file
	// (tail truncation), so a second writer must be rejected before it can
	// touch anything.
	lock, err := acquireFileLock(path + ".lock")
	if err != nil {
		return nil, err
	}

	// Tolerant open repairs a torn trailing page left by a crash mid-append:
	// the incomplete tail is truncated back to the last fully written page so
	// the writer resumes from a consistent, page-aligned boundary.
	pf, err := pagestore.NewPageFileTolerant(path, opts.Cipher)
	if err != nil {
		releaseFileLock(lock)
		return nil, fmt.Errorf("wal: open page file: %w", err)
	}

	w := &WALWriter{
		pf:             pf,
		options:        opts,
		usableBodySize: pf.UsableBodySize(),
		done:           make(chan struct{}),
		lockFile:       lock,
	}
	w.syncCond = sync.NewCond(&w.syncMu)

	// Detecta se estamos reabrindo arquivo existsnte ou criando novo.
	// pf.NumPages() == 1 significa só o slot 0 reservado (arquivo empty).
	if pf.NumPages() > 1 {
		// Reabrir: busca última page e continua preenchendo onde parou.
		if err := w.adoptLastPage(); err != nil {
			_ = pf.Close()
			releaseFileLock(lock)
			return nil, err
		}
	} else {
		// Novo: aloca primeira page.
		if err := w.allocateNewPage(); err != nil {
			_ = pf.Close()
			releaseFileLock(lock)
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

// Cipher devolve o cipher usado para cifrar/decifrar as pages do WAL.
// Pode ser nil quando TDE está desligado. Storage/recovery usa isso para
// abrir WALReader compatível com o writer configurado.
func (w *WALWriter) Cipher() crypto.Cipher {
	return w.options.Cipher
}

// WriteEntry serializa `entry` e escreve na page atual, alocando
// novas pages quando necessário. Aplica a política de sync.
//
// SyncEveryWrite uses group commit: the entry is appended and its page is
// flushed under mu, then the fsync happens outside mu through groupSync,
// so committers that arrive during an in-flight fsync are all covered by
// the next one instead of each paying their own.
func (w *WALWriter) WriteEntry(entry *WALEntry) error {
	w.mu.Lock()

	if w.closed.Load() {
		w.mu.Unlock()
		return fmt.Errorf("wal: writer closed")
	}
	if err := w.poisonedErr(); err != nil {
		w.mu.Unlock()
		return err
	}

	// Serializa header + payload num buffer (headerSize + payloadLen bytes)
	buf := make([]byte, HeaderSize+len(entry.Payload))
	entry.Header.Encode(buf[:HeaderSize])
	copy(buf[HeaderSize:], entry.Payload)

	// Escreve byte-a-byte, cruzando pages se preciso.
	if err := w.appendBytes(buf); err != nil {
		w.mu.Unlock()
		return err
	}
	w.segmentHasEntries = true

	w.batchBytes += int64(len(buf))

	// Política de sync
	switch w.options.SyncPolicy {
	case SyncEveryWrite:
		// Flush to the OS under mu so the group fsync covers these bytes,
		// take a sequence number, then sync outside mu (group commit).
		if err := w.flushCurrentPageLocked(); err != nil {
			w.mu.Unlock()
			return err
		}
		seq := w.appendSeq.Add(1)
		w.mu.Unlock()
		if err := w.groupSync(seq); err != nil {
			return err
		}
		return w.maybeRotate()
	case SyncBatch:
		if w.batchBytes >= w.options.SyncBatchBytes {
			if err := w.syncLocked(); err != nil {
				w.mu.Unlock()
				return err
			}
		}
	}
	err := w.maybeRotateLocked()
	w.mu.Unlock()
	return err
}

// groupSync blocks until an fsync covering `mySeq` has completed. If no
// fsync is in flight, the caller becomes the leader and issues one fsync
// for every sequence flushed so far; otherwise it waits and is usually
// absorbed by the leader's (or the next leader's) fsync. A leader fsync
// failure poisons the writer (fail-stop). Caller must NOT hold w.mu.
func (w *WALWriter) groupSync(mySeq uint64) error {
	w.syncMu.Lock()
	for {
		if w.bgSyncErr != nil {
			err := w.bgSyncErr
			w.syncMu.Unlock()
			return fmt.Errorf("wal: writer poisoned by fsync failure: %w", err)
		}
		if w.syncedSeq >= mySeq {
			w.syncMu.Unlock()
			return nil
		}
		if !w.syncInFlight {
			break
		}
		w.syncCond.Wait()
	}
	w.syncInFlight = true
	// pf is stable while the gate is held: rotation/Close swap or close it
	// only after acquiring this same gate.
	pf := w.pf
	target := w.appendSeq.Load()
	w.syncMu.Unlock()

	w.syncCount.Add(1)
	err := pf.Sync()

	w.syncMu.Lock()
	w.syncInFlight = false
	if err != nil {
		if w.bgSyncErr == nil {
			w.bgSyncErr = err
		}
	} else if w.syncedSeq < target {
		w.syncedSeq = target
	}
	w.syncCond.Broadcast()
	w.syncMu.Unlock()

	if err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	return nil
}

// acquireSyncGate blocks until no group fsync is in flight and claims the
// gate, making it safe to close or swap w.pf. Caller must not hold syncMu.
func (w *WALWriter) acquireSyncGate() {
	w.syncMu.Lock()
	for w.syncInFlight {
		w.syncCond.Wait()
	}
	w.syncInFlight = true
	w.syncMu.Unlock()
}

func (w *WALWriter) releaseSyncGate() {
	w.syncMu.Lock()
	w.syncInFlight = false
	w.syncCond.Broadcast()
	w.syncMu.Unlock()
}

// appendBytes escreve `data` na stream lógica, alocando pages conforme
// necessário. Caller must segurar w.mu.
func (w *WALWriter) appendBytes(data []byte) error {
	for len(data) > 0 {
		spaceInPage := uint16(w.usableBodySize) - w.currentOffset //nolint:gosec // usableBodySize <= PageSize, fits uint16
		if spaceInPage == 0 {
			// Página cheia: flush, aloca nova.
			if err := w.flushCurrentPageLocked(); err != nil {
				return err
			}
			if err := w.allocateNewPage(); err != nil {
				return err
			}
			spaceInPage = uint16(w.usableBodySize) - w.currentOffset //nolint:gosec // usableBodySize <= PageSize
		}

		take := uint16(len(data)) //nolint:gosec // clamped to spaceInPage below
		if take > spaceInPage {
			take = spaceInPage
		}
		copy(w.currentPage.Body()[w.currentOffset:w.currentOffset+take], data[:take])
		w.currentOffset += take
		data = data[take:]

		// Atualiza bytesUsed no header da page
		bytesUsed := w.currentOffset - walPageHeaderSize
		binary.LittleEndian.PutUint16(w.currentPage.Body()[0:2], bytesUsed)
		w.currentPageDirty = true
	}
	return nil
}

// allocateNewPage aloca uma nova page do pagestore e inicializa o
// header WAL (bytesUsed = 0). Caller must segurar w.mu.
func (w *WALWriter) allocateNewPage() error {
	pid, err := w.pf.AllocatePage()
	if err != nil {
		return fmt.Errorf("wal: allocate page: %w", err)
	}
	w.currentPageID = pid
	w.currentPage = pagestore.Page{}
	w.currentPage.Reset()
	// Grava walPageHeader zerado (bytesUsed=0) no body
	binary.LittleEndian.PutUint16(w.currentPage.Body()[0:2], 0)
	w.currentOffset = walPageHeaderSize
	w.currentPageDirty = true // precisa ser write ao menos uma vez
	return nil
}

// adoptLastPage carrega a última page do pf como `currentPage` e
// posiciona currentOffset after os bytes já escritos. Permite continuar
// appending num arquivo reaberto.
func (w *WALWriter) adoptLastPage() error {
	lastPageID := pagestore.PageID(w.pf.NumPages() - 1)
	page, err := w.pf.ReadPage(lastPageID)
	if err != nil {
		return fmt.Errorf("wal: read last page: %w", err)
	}
	w.currentPage = *page
	w.currentPageID = lastPageID
	bytesUsed := binary.LittleEndian.Uint16(page.Body()[0:2])
	w.currentOffset = walPageHeaderSize + bytesUsed
	if int(w.currentOffset) > w.usableBodySize {
		return fmt.Errorf("wal: bytesUsed %d excede usableBody %d", bytesUsed, w.usableBodySize-walPageHeaderSize)
	}
	w.segmentHasEntries = bytesUsed > 0 || w.pf.NumPages() > 2
	w.currentPageDirty = false
	return nil
}

// flushCurrentPageLocked escreve a page atual no pagestore (se dirty).
// NOT chama fsync — isso é só pra garantir que o pagestore recebeu os
// bytes. Caller must segurar w.mu.
func (w *WALWriter) flushCurrentPageLocked() error {
	if !w.currentPageDirty {
		return nil
	}
	if err := w.pf.WritePage(w.currentPageID, &w.currentPage); err != nil {
		return fmt.Errorf("wal: write page %d: %w", w.currentPageID, err)
	}
	w.currentPageDirty = false
	return nil
}

func (w *WALWriter) maybeRotateLocked() error {
	if w.options.MaxSegmentBytes <= 0 || !w.segmentHasEntries {
		return nil
	}
	if err := w.flushCurrentPageLocked(); err != nil {
		return err
	}
	info, err := os.Stat(w.pf.Path())
	if err != nil {
		return err
	}
	if info.Size() < w.options.MaxSegmentBytes {
		return nil
	}
	return w.rotateActiveLocked()
}

func (w *WALWriter) rotateActiveLocked() error {
	if !w.segmentHasEntries {
		return nil
	}

	// Exclude any in-flight group fsync before closing/swapping w.pf: a
	// leader must never fsync a file that rotation is about to close.
	w.acquireSyncGate()
	defer w.releaseSyncGate()

	if err := w.syncLocked(); err != nil {
		return err
	}

	base := w.pf.Path()
	nextPath, err := nextSegmentPath(base)
	if err != nil {
		return err
	}
	if err := w.pf.Close(); err != nil {
		return err
	}
	if err := os.Rename(base, nextPath); err != nil {
		return fmt.Errorf("wal: rotate rename: %w", err)
	}
	if err := fsyncDir(filepath.Dir(base)); err != nil {
		return err
	}

	pf, err := pagestore.NewPageFileTolerant(base, w.options.Cipher)
	if err != nil {
		return fmt.Errorf("wal: open new active segment: %w", err)
	}
	// Swap under syncMu so a future group-commit leader (which reads w.pf
	// under syncMu) observes the new file.
	w.syncMu.Lock()
	w.pf = pf
	w.syncMu.Unlock()
	w.usableBodySize = pf.UsableBodySize()
	w.segmentHasEntries = false
	w.batchBytes = 0
	return w.allocateNewPage()
}

// Sync força a persistência em disco: escreve a page atual + fsync.
func (w *WALWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncLocked()
}

// maybeRotate é maybeRotateLocked para callers que não seguram w.mu.
func (w *WALWriter) maybeRotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.maybeRotateLocked()
}

// poisonedErr reports the sticky fsync failure, if any. Takes syncMu.
func (w *WALWriter) poisonedErr() error {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	if w.bgSyncErr != nil {
		return fmt.Errorf("wal: writer poisoned by fsync failure: %w", w.bgSyncErr)
	}
	return nil
}

// poison records the first fsync failure (fail-stop) and wakes any group
// committers so they observe it instead of waiting forever.
func (w *WALWriter) poison(err error) {
	w.syncMu.Lock()
	if w.bgSyncErr == nil {
		w.bgSyncErr = err
	}
	w.syncCond.Broadcast()
	w.syncMu.Unlock()
}

func (w *WALWriter) syncLocked() error {
	if err := w.poisonedErr(); err != nil {
		return err
	}
	if err := w.flushCurrentPageLocked(); err != nil {
		return err
	}
	w.syncCount.Add(1)
	if err := w.pf.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	w.batchBytes = 0
	// Everything appended so far was just flushed and fsynced: release any
	// group committers waiting on these sequences. (appendSeq is stable
	// here — appends require w.mu, which the caller holds.)
	w.advanceSyncedSeq(w.appendSeq.Load())
	return nil
}

// advanceSyncedSeq marks every sequence up to `target` as durable and
// wakes waiting group committers.
func (w *WALWriter) advanceSyncedSeq(target uint64) {
	w.syncMu.Lock()
	if w.syncedSeq < target {
		w.syncedSeq = target
	}
	w.syncCond.Broadcast()
	w.syncMu.Unlock()
}

// SyncCount returns the number of fsyncs issued to the page file since the
// writer was opened. Group commit makes this grow slower than the number
// of entries under concurrent SyncEveryWrite load.
func (w *WALWriter) SyncCount() uint64 {
	return w.syncCount.Load()
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

	// Exclude any in-flight group fsync before closing the page file.
	w.acquireSyncGate()
	defer w.releaseSyncGate()

	// Flush final (pode fail se disk full; tentamos fechar mesmo assim)
	syncErr := w.syncLocked()
	closeErr := w.pf.Close()
	releaseFileLock(w.lockFile)
	w.lockFile = nil
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// WriteCheckpointRecord grava um record de checkpoint fuzzy no WAL com
// apenas o beginLSN (legacy v1 payload). Prefira WriteCheckpointRecordPayload
// quando quiser carregar DPT/ATT no record.
func (w *WALWriter) WriteCheckpointRecord(beginLSN uint64) error {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, beginLSN)
	return w.WriteCheckpointRecordPayload(beginLSN, payload)
}

// WriteCheckpointRecordPayload grava um checkpoint com um payload
// arbitrário. Os primeiros 8 bytes DEVEM ser o beginLSN little-endian
// (para compatibilidade com leitores antigos).
func (w *WALWriter) WriteCheckpointRecordPayload(beginLSN uint64, payload []byte) error {
	if len(payload) < 8 {
		return fmt.Errorf("wal: checkpoint payload too short (%d)", len(payload))
	}
	if binary.LittleEndian.Uint64(payload[:8]) != beginLSN {
		return fmt.Errorf("wal: checkpoint payload header beginLSN mismatch")
	}

	entry := AcquireEntry()
	entry.Header.Magic = WALMagic
	entry.Header.Version = WALVersion
	entry.Header.EntryType = EntryCheckpoint
	entry.Header.LSN = beginLSN
	entry.Header.PayloadLen = uint32(len(payload)) //nolint:gosec // payload size bounded by DPT/ATT cardinality
	entry.Header.CRC32 = CalculateCRC32(payload)
	entry.Payload = append(entry.Payload[:0], payload...)

	err := w.WriteEntry(entry)
	ReleaseEntry(entry)

	if err != nil {
		return fmt.Errorf("wal: write checkpoint record: %w", err)
	}
	return w.Sync()
}

// CheckpointLifecycle rotaciona o WAL after um checkpoint e remove segmentos
// antigos já cobertos por checkpointLSN, respeitando archive/retention.
func (w *WALWriter) CheckpointLifecycle(checkpointLSN uint64) error {
	w.mu.Lock()
	base := w.pf.Path()
	err := w.rotateActiveLocked()
	archiveDir := w.options.ArchiveDir
	retentionSegments := w.options.RetentionSegments
	cipher := w.options.Cipher
	w.mu.Unlock()
	if err != nil {
		return err
	}
	return ArchiveAndTruncate(base, cipher, archiveDir, checkpointLSN, retentionSegments)
}

func (w *WALWriter) backgroundSync() {
	for {
		select {
		case <-w.ticker.C:
			// Thread-safe; Sync acquires the lock internally. An fsync
			// failure here must not be dropped: poison the writer so the
			// next WriteEntry surfaces it (fail-stop) instead of buffering
			// entries that may never become durable.
			if err := w.Sync(); err != nil && !w.closed.Load() {
				w.poison(err)
			}
		case <-w.done:
			return
		}
	}
}
