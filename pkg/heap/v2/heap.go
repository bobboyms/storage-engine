package v2

import (
	"errors"
	"fmt"
	"sync"

	"github.com/bobboyms/storage-engine/pkg/crypto"
	"github.com/bobboyms/storage-engine/pkg/heap"
	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// ErrRecordTooLarge é devolvido quando um documento não cabe nem numa
// página vazia. Bancos reais tratam isso com overflow pages / TOAST;
// esta implementação de Fase 3 rejeita.
var ErrRecordTooLarge = errors.New("heap/v2: registro maior que uma página")

// Compile-time assertion: *HeapV2 implementa heap.Heap.
// Se a interface evoluir e v2 divergir, isto quebra o build imediatamente.
var _ heap.Heap = (*HeapV2)(nil)

// HeapV2 é a implementação page-based do heap usada pelo runtime atual.
//
// A API preserva a semântica histórica do heap, mas os offsets
// retornados por Write/Read/Delete
// são RecordIDs (PageID|SlotID empacotados em int64), não offsets
// de arquivo. O B+ tree guarda esses int64 como dataPtr — a diferença
// é transparente.
type HeapV2 struct {
	pf          *pagestore.PageFile
	bp          *pagestore.BufferPool
	maxBodySize int

	// writeMu serializa o caminho de Write (para proteger activePageID
	// e o ponto de rotação de página). Read/Delete não passam por aqui.
	writeMu      sync.Mutex
	activePageID pagestore.PageID // InvalidPageID quando ainda não houve write

	// fsm rastreia páginas com espaço livre (hint structure).
	// Permite reutilizar espaço liberado por Vacuum sem scan linear.
	fsm *FreeSpaceMap
}

// NewHeapV2 abre ou cria um heap page-based em `path`. `bufferPoolCapacity`
// define quantas páginas ficam em cache RAM simultaneamente. Passe nil
// para `cipher` para desligar TDE.
func NewHeapV2(path string, bufferPoolCapacity int, cipher crypto.Cipher) (*HeapV2, error) {
	pf, err := pagestore.NewPageFile(path, cipher)
	if err != nil {
		return nil, err
	}

	h := &HeapV2{
		pf:          pf,
		bp:          pagestore.NewBufferPool(pf, bufferPoolCapacity),
		maxBodySize: pf.UsableBodySize(),
		fsm:         newFreeSpaceMap(),
	}

	// Ao reabrir, adota a última página existente como "ativa".
	// NumPages inclui o slot 0 reservado. Se só existe slot 0, não há
	// página ativa (próximo Write aloca).
	if n := pf.NumPages(); n > 1 {
		h.activePageID = pagestore.PageID(n - 1)
	}

	return h, nil
}

// Path devolve o caminho do page file subjacente.
func (h *HeapV2) Path() string { return h.pf.Path() }

func (h *HeapV2) SetBeforeFlushHook(hook func(pageID pagestore.PageID, page *pagestore.Page) error) {
	h.bp.SetBeforeFlushHook(hook)
}

func (h *HeapV2) DirtyPages() []pagestore.DirtyPageInfo {
	return h.bp.DirtyPages()
}

func (h *HeapV2) ApplyPageRedo(pageID pagestore.PageID, page *pagestore.Page, lsn uint64) (bool, error) {
	current, err := h.pf.ReadPage(pageID)
	if err == nil {
		hdr, hdrErr := current.GetHeader()
		if hdrErr == nil && hdr.PageLSN >= lsn {
			h.bp.ReplacePageImage(pageID, current)
			return false, nil
		}
	}
	if err := h.pf.WritePage(pageID, page); err != nil {
		return false, err
	}
	h.bp.ReplacePageImage(pageID, page)
	return true, nil
}

// Close flusha o buffer pool e fecha o page file.
func (h *HeapV2) Close() error {
	if err := h.bp.Close(); err != nil {
		return err
	}
	return h.pf.Close()
}

// Write grava um documento. Retorna o RecordID (int64) estável.
// Semântica idêntica ao v1: o registro NUNCA se move depois de gravado.
func (h *HeapV2) Write(doc []byte, createLSN uint64, prevRecordID int64) (int64, error) {
	// Valida tamanho: registro precisa caber com folga (slot dir + record header).
	recordNeeded := SlotSize + RecordHeaderSize + len(doc)
	maxPayload := h.maxBodySize - SlottedHeaderSize
	if recordNeeded > maxPayload {
		return 0, fmt.Errorf("%w: precisa %d bytes, página tem %d", ErrRecordTooLarge, recordNeeded, maxPayload)
	}

	rh := RecordHeader{
		Valid:        true,
		CreateLSN:    createLSN,
		DeleteLSN:    0,
		PrevRecordID: prevRecordID,
	}

	h.writeMu.Lock()
	defer h.writeMu.Unlock()

	needed := SlotSize + RecordHeaderSize + len(doc)

	// 1. Tenta reutilizar página do FSM (espaço liberado por Vacuum).
	//    O FSM pode estar desatualizado — ErrPageFull é tratado como
	//    "remover candidata e tentar activePageID".
	if candidate, ok := h.fsm.FindPage(needed); ok && candidate != h.activePageID {
		rid, ok, err := h.tryInsert(candidate, rh, doc)
		if err != nil {
			return 0, err
		}
		if ok {
			// Atualiza FSM com espaço restante após insert.
			h.updateFSMAfterInsert(candidate, needed)
			return rid, nil
		}
		// Candidata cheia (dado desatualizado no FSM) — remove do mapa.
		h.fsm.Remove(candidate)
	}

	// 2. Tenta inserir na página ativa (se houver).
	if h.activePageID != pagestore.InvalidPageID {
		rid, ok, err := h.tryInsert(h.activePageID, rh, doc)
		if err != nil {
			return 0, err
		}
		if ok {
			h.updateFSMAfterInsert(h.activePageID, needed)
			return rid, nil
		}
		// Página ativa cheia — remove do FSM e cai pro caminho de alocar nova.
		h.fsm.Remove(h.activePageID)
	}

	// 3. Aloca uma nova página via BufferPool.NewPage (que já retorna com
	// latch exclusivo e marca suja pra forçar escrita inicial).
	handle, err := h.bp.NewPage()
	if err != nil {
		return 0, err
	}
	defer handle.Release()

	sp := InitSlottedPage(handle.Page(), h.maxBodySize)
	slotID, err := sp.Insert(rh, doc)
	if err != nil {
		// Não deveria acontecer — o check de ErrRecordTooLarge acima já
		// garante que cabe em página vazia.
		return 0, fmt.Errorf("heap/v2: insert em página recém-alocada falhou: %w", err)
	}
	// Avança pageLSN pra suportar recovery idempotente (infraestrutura
	// pra futuro redo page-level; hoje não é usado no replay mas grava
	// o LSN correto pra quando for).
	handle.Page().AdvancePageLSN(createLSN)
	handle.MarkDirty()

	newPageID := handle.ID()
	// Registra espaço residual no FSM se houver folga.
	if free := sp.FreeSpace(); free > SlotSize {
		h.fsm.Register(newPageID, free)
	}

	h.activePageID = newPageID
	return EncodeRecordID(newPageID, slotID), nil
}

// updateFSMAfterInsert atualiza o FSM subtraindo o espaço consumido pelo insert.
// Como não re-lemos a página aqui (seria caro), subtrai de forma conservadora:
// se o FSM não tem entrada para a página, não faz nada (será populado no próximo Vacuum).
func (h *HeapV2) updateFSMAfterInsert(pageID pagestore.PageID, consumedBytes int) {
	h.fsm.mu.Lock()
	defer h.fsm.mu.Unlock()
	if current, ok := h.fsm.pages[pageID]; ok {
		remaining := current - consumedBytes
		if remaining <= SlotSize {
			delete(h.fsm.pages, pageID)
		} else {
			h.fsm.pages[pageID] = remaining
		}
	}
}

// tryInsert tenta inserir rh+doc na página pid. Retorna (rid, ok, err):
//   - ok=true: inserido, rid válido
//   - ok=false, err=nil: página cheia, chamador deve tentar outra
//   - err != nil: erro real de I/O
func (h *HeapV2) tryInsert(pid pagestore.PageID, rh RecordHeader, doc []byte) (int64, bool, error) {
	handle, err := h.bp.FetchForWrite(pid)
	if err != nil {
		return 0, false, err
	}
	defer handle.Release()

	sp := OpenSlottedPage(handle.Page())
	slotID, err := sp.Insert(rh, doc)
	if errors.Is(err, ErrPageFull) {
		return 0, false, nil
	}
	if err == nil {
		handle.Page().AdvancePageLSN(rh.CreateLSN)
	}
	if err != nil {
		return 0, false, err
	}
	handle.MarkDirty()
	return EncodeRecordID(pid, slotID), true, nil
}

// Read devolve o documento e o header do registro identificado por rid.
// Retorna o header mesmo se o registro está marcado inválido (MVCC) —
// chamadores de visibilidade (transação antiga) precisam ver isso.
func (h *HeapV2) Read(rid int64) ([]byte, *RecordHeader, error) {
	pid, slotID := DecodeRecordID(rid)
	if pid == pagestore.InvalidPageID {
		return nil, nil, fmt.Errorf("heap/v2: RecordID %d inválido (pageID=0)", rid)
	}

	handle, err := h.bp.Fetch(pid)
	if err != nil {
		return nil, nil, err
	}
	defer handle.Release()

	sp := OpenSlottedPage(handle.Page())
	doc, rh, err := sp.Read(slotID)
	if err != nil {
		return nil, nil, err
	}
	return doc, &rh, nil
}

// Delete marca o registro como inválido (lazy delete do MVCC).
// Bytes do doc e CreateLSN/PrevRecordID são preservados — transações
// antigas continuam conseguindo ler a versão.
func (h *HeapV2) Delete(rid int64, deleteLSN uint64) error {
	pid, slotID := DecodeRecordID(rid)
	if pid == pagestore.InvalidPageID {
		return fmt.Errorf("heap/v2: RecordID %d inválido (pageID=0)", rid)
	}

	handle, err := h.bp.FetchForWrite(pid)
	if err != nil {
		return err
	}
	defer handle.Release()

	sp := OpenSlottedPage(handle.Page())
	if err := sp.MarkDeleted(slotID, deleteLSN); err != nil {
		return err
	}
	handle.Page().AdvancePageLSN(deleteLSN)
	handle.MarkDirty()
	return nil
}

// Sync persiste tudo no disco (buffer pool → fsync).
func (h *HeapV2) Sync() error {
	return h.bp.FlushAll()
}

// Vacuum percorre todas as páginas do heap e chama Compact(minLSN) em
// cada uma. Retorna o total de slots vacuumados.
//
// `minLSN` é tipicamente o menor LSN entre transações ativas — registros
// deletados antes disso não são mais visíveis a ninguém e podem ser
// reclamados com segurança.
//
// Concorrência: usa FetchForWrite por página, então Writes em OUTRAS
// páginas podem prosseguir em paralelo. Writes na mesma página esperam.
func (h *HeapV2) Vacuum(minLSN uint64) (int, error) {
	// FlushAll antes de iterar: páginas recém-alocadas via NewPage ficam
	// no BufferPool com dirty=true mas PageFile.NumPages() só aumenta
	// quando WritePage é chamado. Sem o flush, páginas novas ficariam
	// fora do loop abaixo.
	if err := h.bp.FlushAll(); err != nil {
		return 0, err
	}

	numPages := h.pf.NumPages()
	total := 0

	for pageID := pagestore.PageID(1); uint64(pageID) < numPages; pageID++ {
		handle, err := h.bp.FetchForWrite(pageID)
		if err != nil {
			return total, err
		}

		sp := OpenSlottedPage(handle.Page())
		n, err := sp.Compact(minLSN)
		if err != nil {
			handle.Release()
			return total, err
		}
		if n > 0 {
			handle.Page().AdvancePageLSN(minLSN)
			handle.MarkDirty()
			// Registra espaço recém-liberado no FSM para reutilização futura.
			h.fsm.Register(pageID, sp.FreeSpace())
		}
		handle.Release()
		total += n
	}

	return total, nil
}

// FSM retorna o Free Space Map desta heap. Exposto para testes e diagnóstico.
func (h *HeapV2) FSM() *FreeSpaceMap { return h.fsm }
