package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	btreev2 "github.com/bobboyms/storage-engine/pkg/btree/v2"
	"github.com/bobboyms/storage-engine/pkg/crypto"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// findLastCheckpointLSN varre o WAL e retorna o beginLSN do record de
// checkpoint mais recente. Retorna (0, false) se not houver nenhum record
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
	defer func() { _ = reader.Close() }()

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
	// txAwareWALVersion is the WAL entry version that carries the
	// transaction id in the payload prefix.
	txAwareWALVersion = 2
	// ariesWALVersion adds prevLSN after the txID. Together they form
	// the per-tx backlink chain ARIES needs for safe restartable undo.
	ariesWALVersion = 3
	txPayloadPrefix = 8 // bytes: just txID (v2 layout)
	// ariesPayloadPrefix = txID(8) + prevLSN(8) (v3 layout).
	ariesPayloadPrefix = 16
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
	CheckpointLSN uint64 // beginLSN do último checkpoint; 0 = not encontrado
	DirtyIndexes  map[string]uint64
	TxTable       map[uint64]recoveryTxnState
	CommittedTxs  map[uint64]struct{}
	LoserTxs      map[uint64]struct{}
	UndoneLSNs    map[uint64]map[uint64]struct{}
	// DPT captured from the latest checkpoint record. minRecLSN(DPT)
	// is the lower bound for physical redo: entries with LSN below
	// that point have necessarily been applied and flushed.
	DPT []dirtyPageEntry
	// PartialNTAs maps NTABegin LSN → its parsed payload for every
	// nested top action that did not see a matching NTACommit. These
	// represent structural ops that crashed mid-flight; recovery
	// restores the captured before-images for their pages.
	PartialNTAs map[uint64]ntaBeginRecord
}

type ntaBeginRecord struct {
	Kind  uint8
	Pages []NTAPage
}

// ingestTxEntry folds a transactional WAL entry into the analysis
// tx-table state and, when the entry is a CLR, records the LSN whose
// effects it undoes.
func (ra *recoveryAnalysis) ingestTxEntry(txID uint64, entry *wal.WALEntry, payload []byte) error {
	state := ra.TxTable[txID]
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
	ra.TxTable[txID] = state

	if entry.Header.EntryType == wal.EntryCLR {
		originalLSN, _, _, _, err := DeserializeCompensationEntry(payload)
		if err != nil {
			return fmt.Errorf("deserialize clr: %w", err)
		}
		if _, ok := ra.UndoneLSNs[txID]; !ok {
			ra.UndoneLSNs[txID] = make(map[uint64]struct{})
		}
		ra.UndoneLSNs[txID][originalLSN] = struct{}{}
	}
	return nil
}

// ingestNTA folds a nested-top-action entry (Begin or Commit) into
// the partial-NTA map so recovery can later restore before-images for
// NTAs whose Commit never made it to disk.
func (ra *recoveryAnalysis) ingestNTA(entry *wal.WALEntry) error {
	switch entry.Header.EntryType {
	case wal.EntryNTABegin:
		kind, pages, err := deserializeNTABeginPayload(entry.Payload)
		if err != nil {
			return err
		}
		ra.PartialNTAs[entry.Header.LSN] = ntaBeginRecord{Kind: kind, Pages: pages}
	case wal.EntryNTACommit:
		ntaID, err := deserializeNTACommitPayload(entry.Payload)
		if err != nil {
			return err
		}
		delete(ra.PartialNTAs, ntaID)
	}
	return nil
}

// ingestCheckpoint folds a checkpoint payload into the analysis state.
// Both v1 (beginLSN only) and v2 (beginLSN + DPT + ATT) formats are
// accepted; v2 overwrites the current snapshot when its beginLSN is at
// least as fresh.
func (ra *recoveryAnalysis) ingestCheckpoint(payload []byte) error {
	beginLSN, dpt, att, err := parseCheckpointPayload(payload)
	if err != nil {
		return err
	}
	if beginLSN < ra.CheckpointLSN {
		return nil
	}
	ra.CheckpointLSN = beginLSN
	ra.DPT = dpt
	for _, e := range att {
		state := ra.TxTable[e.TxID]
		if state.FirstLSN == 0 || e.LastLSN < state.FirstLSN {
			state.FirstLSN = e.LastLSN
		}
		if e.LastLSN > state.LastLSN {
			state.LastLSN = e.LastLSN
		}
		if state.Status == recoveryTxnUnknown {
			state.Status = recoveryTxnActive
		}
		ra.TxTable[e.TxID] = state
	}
	return nil
}

func newRecoveryAnalysis() *recoveryAnalysis {
	return &recoveryAnalysis{
		DirtyIndexes: make(map[string]uint64),
		TxTable:      make(map[uint64]recoveryTxnState),
		CommittedTxs: make(map[uint64]struct{}),
		LoserTxs:     make(map[uint64]struct{}),
		UndoneLSNs:   make(map[uint64]map[uint64]struct{}),
		PartialNTAs:  make(map[uint64]ntaBeginRecord),
	}
}

// wrapAriesPayload prefixes the WAL body with (txID, prevLSN). Use this
// for transactional entries written in WAL v3+. prevLSN == 0 means the
// entry is the first one for txID.
func wrapAriesPayload(txID, prevLSN uint64, payload []byte) []byte {
	buf := make([]byte, ariesPayloadPrefix+len(payload))
	binary.LittleEndian.PutUint64(buf[:8], txID)
	binary.LittleEndian.PutUint64(buf[8:16], prevLSN)
	copy(buf[ariesPayloadPrefix:], payload)
	return buf
}

// wrapTxPayload preserves the v2 prefix layout (txID only). Kept for
// callers that have not been upgraded; new write paths should use
// wrapAriesPayload.
func wrapTxPayload(txID uint64, payload []byte) []byte {
	buf := make([]byte, txPayloadPrefix+len(payload))
	binary.LittleEndian.PutUint64(buf[:txPayloadPrefix], txID)
	copy(buf[txPayloadPrefix:], payload)
	return buf
}

// unwrapTxPayload returns txID and body, hiding the version-aware prefix
// length. prevLSN is silently discarded; callers that need it must use
// unwrapTxPayloadWithPrev.
func unwrapTxPayload(header wal.WALHeader, payload []byte) (txID uint64, body []byte, transactional bool, err error) {
	txID, body, _, transactional, err = unwrapTxPayloadWithPrev(header, payload)
	return txID, body, transactional, err
}

// unwrapTxPayloadWithPrev returns the txID, body, and the prevLSN that
// links this entry to its predecessor in the same transaction (v3+).
// v2 entries report prevLSN=0.
func unwrapTxPayloadWithPrev(header wal.WALHeader, payload []byte) (txID uint64, body []byte, prevLSN uint64, transactional bool, err error) {
	if header.Version < txAwareWALVersion {
		return 0, payload, 0, false, nil
	}
	if header.Version >= ariesWALVersion {
		if len(payload) < ariesPayloadPrefix {
			return 0, nil, 0, false, fmt.Errorf("wal entry version %d requires aries payload prefix", header.Version)
		}
		txID = binary.LittleEndian.Uint64(payload[:8])
		prevLSN = binary.LittleEndian.Uint64(payload[8:16])
		return txID, payload[ariesPayloadPrefix:], prevLSN, true, nil
	}
	if len(payload) < txPayloadPrefix {
		return 0, nil, 0, false, fmt.Errorf("wal entry version %d requires tx payload prefix", header.Version)
	}
	txID = binary.LittleEndian.Uint64(payload[:txPayloadPrefix])
	return txID, payload[txPayloadPrefix:], 0, true, nil
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
	defer func() { _ = reader.Close() }()

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

		if entry.Header.EntryType == wal.EntryCheckpoint && len(entry.Payload) >= 8 {
			if err := result.ingestCheckpoint(entry.Payload); err != nil {
				wal.ReleaseEntry(entry)
				return nil, fmt.Errorf("analysis parse checkpoint at entry %d: %w", count, err)
			}
			wal.ReleaseEntry(entry)
			continue
		}

		if entry.Header.EntryType == wal.EntryNTABegin || entry.Header.EntryType == wal.EntryNTACommit {
			if err := result.ingestNTA(entry); err != nil {
				wal.ReleaseEntry(entry)
				return nil, fmt.Errorf("analysis parse NTA at entry %d: %w", count, err)
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
			if err := result.ingestTxEntry(txID, entry, payload); err != nil {
				wal.ReleaseEntry(entry)
				return nil, fmt.Errorf("analysis tx entry %d: %w", count, err)
			}
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
	case wal.EntryBegin, wal.EntryCommit, wal.EntryAbort, wal.EntryCheckpoint, wal.EntryPageRedo:
		return payload, false, nil
	case wal.EntryCLR:
		return payload, true, nil
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

	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		return nil
	}
	index, err := table.GetIndex(indexName)
	if err != nil {
		return nil
	}

	if entry.Header.EntryType == wal.EntryDelete {
		if shouldSkipDeleteRedo(table, index, key, entry.Header.LSN) {
			loadedLSNs[appliedLSNKey(tableName, indexName)] = entry.Header.LSN
			se.appliedLSN.MarkApplied(tableName, indexName, entry.Header.LSN)
			return nil
		}
		physicalKey := singleIndexPhysicalKey(index, key)
		if offset, found, _ := index.Tree.Get(physicalKey); found {
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
		skip, err := shouldSkipInsertRedo(table, index, key, docBytes, entry.Header.LSN)
		if err != nil {
			return err
		}
		if skip {
			loadedLSNs[appliedLSNKey(tableName, indexName)] = entry.Header.LSN
			se.appliedLSN.MarkApplied(tableName, indexName, entry.Header.LSN)
			return nil
		}
		prevOffset := int64(-1)
		physicalKey := singleIndexPhysicalKey(index, key)
		if prev, found, _ := index.Tree.Get(physicalKey); found {
			prevOffset = prev
		}

		offset, err := table.Heap.Write(docBytes, entry.Header.LSN, prevOffset)
		if err != nil {
			return fmt.Errorf("heap write failed: %w", err)
		}
		if treeV2, ok := index.Tree.(*btreev2.BTreeV2); ok {
			err = treeV2.ReplaceWithLSN(physicalKey, offset, entry.Header.LSN)
		} else {
			err = index.Tree.Replace(physicalKey, offset)
		}
		if err != nil {
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

	if skip, err := shouldSkipMultiInsertRedo(table, keys, docBytes, entry.Header.LSN); err != nil {
		return err
	} else if skip {
		for indexName := range keys {
			lookupKey := appliedLSNKey(tableName, indexName)
			loadedLSNs[lookupKey] = entry.Header.LSN
			se.appliedLSN.MarkApplied(tableName, indexName, entry.Header.LSN)
		}
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

	table.Lock()
	defer table.Unlock()

	prevOffset := int64(-1)
	primary, primaryKey, err := primaryIndexAndKey(table, keys)
	if err == nil {
		if oldOffset, found, getErr := primary.Tree.Get(primaryKey); getErr != nil {
			return fmt.Errorf("primary index get failed during recovery: %w", getErr)
		} else if found {
			prevOffset = oldOffset
		}
	}

	offset, err := table.Heap.Write(docBytes, entry.Header.LSN, prevOffset)
	if err != nil {
		return fmt.Errorf("heap write failed: %w", err)
	}

	if err := applyIndexPointersWithLSN(table, keys, offset, entry.Header.LSN); err != nil {
		return err
	}

	if prevOffset != -1 {
		if err := table.Heap.Delete(prevOffset, entry.Header.LSN); err != nil && !isChainEndErr(err) {
			return fmt.Errorf("heap delete previous version during recovery failed: %w", err)
		}
	}

	for indexName := range keys {
		lookupKey := appliedLSNKey(tableName, indexName)
		loadedLSNs[lookupKey] = entry.Header.LSN
		se.appliedLSN.MarkApplied(tableName, indexName, entry.Header.LSN)
	}

	return nil
}

func shouldSkipDeleteRedo(table *Table, index *Index, key types.Comparable, lsn uint64) bool {
	offset, found, err := index.Tree.Get(singleIndexPhysicalKey(index, key))
	if err != nil || !found {
		return err == nil
	}
	_, hdr, err := table.Heap.Read(offset)
	if err != nil {
		return false
	}
	return hdr.DeleteLSN >= lsn || hdr.CreateLSN > lsn
}

func shouldSkipInsertRedo(table *Table, index *Index, key types.Comparable, docBytes []byte, lsn uint64) (bool, error) {
	offset, found, err := index.Tree.Get(singleIndexPhysicalKey(index, key))
	if err != nil || !found {
		return false, err
	}
	currentDoc, hdr, err := table.Heap.Read(offset)
	if err != nil {
		return false, nil
	}
	if hdr.CreateLSN > lsn {
		return true, nil
	}
	return hdr.CreateLSN == lsn && bytes.Equal(currentDoc, docBytes), nil
}

func shouldSkipMultiInsertRedo(table *Table, keys map[string]types.Comparable, docBytes []byte, lsn uint64) (bool, error) {
	primary, primaryKey, err := primaryIndexAndKey(table, keys)
	if err != nil {
		return false, err
	}
	return shouldSkipInsertRedo(table, primary, primaryKey, docBytes, lsn)
}

func (se *StorageEngine) redoPageEntry(entry *wal.WALEntry, targets map[string]pageRedoTarget) (bool, error) {
	path, pageID, page, err := deserializePageRedoPayload(entry.Payload)
	if err != nil {
		return false, err
	}
	target := targets[path]
	if target == nil {
		return false, nil
	}
	return target.ApplyPageRedo(pageID, page, entry.Header.LSN)
}
