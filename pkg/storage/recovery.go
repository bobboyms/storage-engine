package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/bobboyms/storage-engine/pkg/crypto"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// findLastCheckpointLSN varre o WAL e retorna o beginLSN do registro de
// checkpoint mais recente. Retorna (0, false) se não houver nenhum registro
// de checkpoint — recovery cai no caminho clássico (replay completo).
func findLastCheckpointLSN(walPath string) (uint64, bool, error) {
	return findLastCheckpointLSNWithCipher(walPath, nil)
}

func findLastCheckpointLSNWithCipher(walPath string, cipher crypto.Cipher) (uint64, bool, error) {
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		return 0, false, nil
	}

	reader, err := wal.NewWALReaderWithCipher(walPath, cipher)
	if err != nil {
		return 0, false, err
	}
	defer reader.Close()

	var lastCheckpointLSN uint64
	found := false

	for {
		entry, err := reader.ReadEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			if isExpectedWALTail(err) {
				break
			}
			return 0, false, err
		}
		if entry.Header.EntryType == wal.EntryCheckpoint && len(entry.Payload) >= 8 {
			beginLSN := binary.LittleEndian.Uint64(entry.Payload[:8])
			if beginLSN >= lastCheckpointLSN {
				lastCheckpointLSN = beginLSN
				found = true
			}
		}
		wal.ReleaseEntry(entry)
	}
	return lastCheckpointLSN, found, nil
}

const (
	txAwareWALVersion = 2
	txPayloadPrefix   = 8
)

type recoveryTxnStatus uint8

const (
	recoveryTxnUnknown recoveryTxnStatus = iota
	recoveryTxnActive
	recoveryTxnCommitted
	recoveryTxnAborted
)

type recoveryTxnState struct {
	Status   recoveryTxnStatus
	FirstLSN uint64
	LastLSN  uint64
}

type recoveryAnalysis struct {
	MaxLSN        uint64
	CheckpointLSN uint64 // beginLSN do último checkpoint; 0 = não encontrado
	DirtyIndexes  map[string]uint64
	TxTable       map[uint64]recoveryTxnState
	CommittedTxs  map[uint64]struct{}
	LoserTxs      map[uint64]struct{}
}

func newRecoveryAnalysis() *recoveryAnalysis {
	return &recoveryAnalysis{
		DirtyIndexes: make(map[string]uint64),
		TxTable:      make(map[uint64]recoveryTxnState),
		CommittedTxs: make(map[uint64]struct{}),
		LoserTxs:     make(map[uint64]struct{}),
	}
}

func wrapTxPayload(txID uint64, payload []byte) []byte {
	buf := make([]byte, txPayloadPrefix+len(payload))
	binary.LittleEndian.PutUint64(buf[:txPayloadPrefix], txID)
	copy(buf[txPayloadPrefix:], payload)
	return buf
}

func unwrapTxPayload(header wal.WALHeader, payload []byte) (txID uint64, body []byte, transactional bool, err error) {
	if header.Version < txAwareWALVersion {
		return 0, payload, false, nil
	}
	if len(payload) < txPayloadPrefix {
		return 0, nil, false, fmt.Errorf("wal entry version %d requires tx payload prefix", header.Version)
	}

	txID = binary.LittleEndian.Uint64(payload[:txPayloadPrefix])
	return txID, payload[txPayloadPrefix:], true, nil
}

func isExpectedWALTail(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF)
}

func (se *StorageEngine) analyzeRecovery(walPath string) (*recoveryAnalysis, error) {
	return se.analyzeRecoveryWithCipher(walPath, se.walCipher())
}

func (se *StorageEngine) analyzeRecoveryWithCipher(walPath string, cipher crypto.Cipher) (*recoveryAnalysis, error) {
	result := newRecoveryAnalysis()

	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		return result, nil
	}

	reader, err := wal.NewWALReaderWithCipher(walPath, cipher)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	for count := 0; ; count++ {
		entry, err := reader.ReadEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			if isExpectedWALTail(err) {
				break
			}
			return nil, fmt.Errorf("analysis error at entry %d: %w", count, err)
		}
		if entry.Header.LSN > result.MaxLSN {
			result.MaxLSN = entry.Header.LSN
		}

		// Atualiza o checkpoint LSN se encontrar registro mais recente.
		if entry.Header.EntryType == wal.EntryCheckpoint && len(entry.Payload) >= 8 {
			beginLSN := binary.LittleEndian.Uint64(entry.Payload[:8])
			if beginLSN >= result.CheckpointLSN {
				result.CheckpointLSN = beginLSN
			}
			wal.ReleaseEntry(entry)
			continue
		}

		txID, payload, transactional, err := unwrapTxPayload(entry.Header, entry.Payload)
		if err != nil {
			wal.ReleaseEntry(entry)
			return nil, fmt.Errorf("analysis unwrap failed at entry %d: %w", count, err)
		}

		if transactional {
			state := result.TxTable[txID]
			if state.FirstLSN == 0 || entry.Header.LSN < state.FirstLSN {
				state.FirstLSN = entry.Header.LSN
			}
			if entry.Header.LSN > state.LastLSN {
				state.LastLSN = entry.Header.LSN
			}

			switch entry.Header.EntryType {
			case wal.EntryBegin:
				if state.Status == recoveryTxnUnknown {
					state.Status = recoveryTxnActive
				}
			case wal.EntryCommit:
				state.Status = recoveryTxnCommitted
			case wal.EntryAbort:
				state.Status = recoveryTxnAborted
			default:
				if state.Status == recoveryTxnUnknown {
					state.Status = recoveryTxnActive
				}
			}
			result.TxTable[txID] = state
		}

		switch entry.Header.EntryType {
		case wal.EntryInsert, wal.EntryUpdate, wal.EntryDelete:
			tableName, indexName, _, _, err := DeserializeDocumentEntry(payload)
			if err != nil {
				wal.ReleaseEntry(entry)
				return nil, fmt.Errorf("analysis deserialize failed at entry %d: %w", count, err)
			}
			key := appliedLSNKey(tableName, indexName)
			if _, ok := result.DirtyIndexes[key]; !ok {
				result.DirtyIndexes[key] = entry.Header.LSN
			}
		case wal.EntryMultiInsert:
			tableName, keys, _, err := DeserializeMultiIndexEntry(payload)
			if err != nil {
				wal.ReleaseEntry(entry)
				return nil, fmt.Errorf("analysis deserialize multi failed at entry %d: %w", count, err)
			}
			for indexName := range keys {
				key := appliedLSNKey(tableName, indexName)
				if _, ok := result.DirtyIndexes[key]; !ok {
					result.DirtyIndexes[key] = entry.Header.LSN
				}
			}
		}

		wal.ReleaseEntry(entry)
	}

	for txID, state := range result.TxTable {
		switch state.Status {
		case recoveryTxnCommitted:
			result.CommittedTxs[txID] = struct{}{}
		case recoveryTxnActive:
			result.LoserTxs[txID] = struct{}{}
		}
	}

	return result, nil
}

func (ra *recoveryAnalysis) shouldRedo(entry *wal.WALEntry) ([]byte, bool, error) {
	// Entradas anteriores ao último checkpoint já estão em disco.
	// Pular o redo reduz o tempo de startup de O(WAL inteiro) para
	// O(WAL desde o último checkpoint).
	if ra.CheckpointLSN > 0 && entry.Header.LSN < ra.CheckpointLSN {
		_, payload, _, err := unwrapTxPayload(entry.Header, entry.Payload)
		return payload, false, err
	}

	txID, payload, transactional, err := unwrapTxPayload(entry.Header, entry.Payload)
	if err != nil {
		return nil, false, err
	}

	switch entry.Header.EntryType {
	case wal.EntryBegin, wal.EntryCommit, wal.EntryAbort, wal.EntryCheckpoint:
		return payload, false, nil
	}

	if !transactional {
		return payload, true, nil
	}

	_, committed := ra.CommittedTxs[txID]
	return payload, committed, nil
}

func (se *StorageEngine) redoDocumentEntry(entry *wal.WALEntry, payload []byte, loadedLSNs map[string]uint64) error {
	tableName, indexName, key, docBytes, err := DeserializeDocumentEntry(payload)
	if err != nil {
		return err
	}

	lookupKey := appliedLSNKey(tableName, indexName)
	if loadedLSNs[lookupKey] >= entry.Header.LSN {
		return nil
	}

	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return nil
	}
	index, err := table.GetIndex(indexName)
	if err != nil {
		return nil
	}

	if entry.Header.EntryType == wal.EntryDelete {
		if offset, found, _ := index.Tree.Get(key); found {
			if err := table.Heap.Delete(offset, entry.Header.LSN); err != nil {
				if isChainEndErr(err) {
					loadedLSNs[lookupKey] = entry.Header.LSN
					se.appliedLSN.MarkApplied(tableName, indexName, entry.Header.LSN)
					return nil
				}
				return fmt.Errorf("heap delete failed: %w", err)
			}
		}
	} else {
		prevOffset := int64(-1)
		if prev, found, _ := index.Tree.Get(key); found {
			prevOffset = prev
		}

		offset, err := table.Heap.Write(docBytes, entry.Header.LSN, prevOffset)
		if err != nil {
			return fmt.Errorf("heap write failed: %w", err)
		}
		if err := index.Tree.Replace(key, offset); err != nil {
			return fmt.Errorf("failed to update tree during recovery: %w", err)
		}
	}

	loadedLSNs[lookupKey] = entry.Header.LSN
	se.appliedLSN.MarkApplied(tableName, indexName, entry.Header.LSN)
	return nil
}

func (se *StorageEngine) redoMultiInsertEntry(entry *wal.WALEntry, payload []byte, loadedLSNs map[string]uint64) error {
	tableName, keys, docBytes, err := DeserializeMultiIndexEntry(payload)
	if err != nil {
		return err
	}

	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return nil
	}

	needsUpdate := false
	for indexName := range keys {
		if loadedLSNs[appliedLSNKey(tableName, indexName)] < entry.Header.LSN {
			needsUpdate = true
			break
		}
	}
	if !needsUpdate {
		return nil
	}

	offset, err := table.Heap.Write(docBytes, entry.Header.LSN, -1)
	if err != nil {
		return fmt.Errorf("heap write failed: %w", err)
	}

	for indexName, key := range keys {
		lookupKey := appliedLSNKey(tableName, indexName)
		if loadedLSNs[lookupKey] >= entry.Header.LSN {
			continue
		}

		index, err := table.GetIndex(indexName)
		if err != nil {
			continue
		}
		if err := index.Tree.Replace(key, offset); err != nil {
			return fmt.Errorf("failed to update index %s during recovery: %w", indexName, err)
		}

		loadedLSNs[lookupKey] = entry.Header.LSN
		se.appliedLSN.MarkApplied(tableName, indexName, entry.Header.LSN)
	}

	return nil
}

func (se *StorageEngine) undoLoserTransactions(analysis *recoveryAnalysis) {
	// ARIES-lite do write path atual:
	// transações explícitas só aplicam mudanças em heap/tree APÓS o
	// COMMIT estar durável no WAL. Portanto, tx "loser" (BEGIN sem
	// COMMIT/ABORT no crash) nunca chegaram a alterar o estado visível,
	// e o undo pós-crash reduz-se a descartar seus log records no redo.
	//
	// Mantemos a fase explícita porque ela fecha o contrato de recovery
	// e preserva o ponto de extensão caso o write path evolua para
	// updates uncommitted em página com CLRs.
	for txID := range analysis.LoserTxs {
		state := analysis.TxTable[txID]
		state.Status = recoveryTxnAborted
		analysis.TxTable[txID] = state
	}
}

func cloneKeys(src map[string]uint64) map[string]uint64 {
	dst := make(map[string]uint64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
