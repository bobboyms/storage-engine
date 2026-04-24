package storage

import (
	"path/filepath"
	"testing"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func writeTxMarkerForTest(t *testing.T, w *wal.WALWriter, txID, lsn uint64, entryType uint8) {
	t.Helper()

	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = txAwareWALVersion
	entry.Header.EntryType = entryType
	entry.Header.LSN = lsn
	entry.Payload = append(entry.Payload, wrapTxPayload(txID, nil)...)
	entry.Header.PayloadLen = uint32(len(entry.Payload))
	entry.Header.CRC32 = wal.CalculateCRC32(entry.Payload)

	if err := w.WriteEntry(entry); err != nil {
		wal.ReleaseEntry(entry)
		t.Fatalf("write marker lsn=%d: %v", lsn, err)
	}
	wal.ReleaseEntry(entry)
}

func writeTxDocumentForTest(t *testing.T, w *wal.WALWriter, txID, lsn uint64, tableName, indexName string, key types.Comparable, doc string) {
	t.Helper()

	payload, err := SerializeDocumentEntry(tableName, indexName, key, []byte(doc))
	if err != nil {
		t.Fatalf("serialize tx document: %v", err)
	}

	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = txAwareWALVersion
	entry.Header.EntryType = wal.EntryInsert
	entry.Header.LSN = lsn
	entry.Payload = append(entry.Payload, wrapTxPayload(txID, payload)...)
	entry.Header.PayloadLen = uint32(len(entry.Payload))
	entry.Header.CRC32 = wal.CalculateCRC32(entry.Payload)

	if err := w.WriteEntry(entry); err != nil {
		wal.ReleaseEntry(entry)
		t.Fatalf("write tx document lsn=%d: %v", lsn, err)
	}
	wal.ReleaseEntry(entry)
}

func TestRecovery_ExplicitTransaction_CommitsWinnersAndDropsLosers(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	writer, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("new wal writer: %v", err)
	}

	writeTxMarkerForTest(t, writer, 1001, 1, wal.EntryBegin)
	writeTxDocumentForTest(t, writer, 1001, 2, "users", "id", types.IntKey(1), `{"id":1}`)
	writeTxMarkerForTest(t, writer, 1001, 3, wal.EntryCommit)

	writeTxMarkerForTest(t, writer, 2002, 4, wal.EntryBegin)
	writeTxDocumentForTest(t, writer, 2002, 5, "users", "id", types.IntKey(2), `{"id":2}`)

	if err := writer.Close(); err != nil {
		t.Fatalf("close wal writer: %v", err)
	}

	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("new heap: %v", err)
	}

	tableMgr := NewTableMenager()
	if err := tableMgr.NewTable("users", []Index{{Name: "id", Primary: true, Type: TypeInt}}, 4, hm); err != nil {
		t.Fatalf("new table: %v", err)
	}

	walWriter, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("reopen wal writer: %v", err)
	}

	se, err := NewStorageEngine(tableMgr, walWriter)
	if err != nil {
		walWriter.Close()
		t.Fatalf("new storage engine: %v", err)
	}
	defer se.Close()

	if err := se.Recover(walPath); err != nil {
		t.Fatalf("recover: %v", err)
	}

	if _, found, err := se.Get("users", "id", types.IntKey(1)); err != nil {
		t.Fatalf("get committed tx key: %v", err)
	} else if !found {
		t.Fatalf("committed transaction key missing after recovery")
	}

	if _, found, err := se.Get("users", "id", types.IntKey(2)); err != nil {
		t.Fatalf("get loser tx key: %v", err)
	} else if found {
		t.Fatalf("loser transaction key should not be visible after recovery")
	}
}

func TestWriteTransaction_Rollback_PersistsAbortMarker(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("new heap: %v", err)
	}

	tableMgr := NewTableMenager()
	if err := tableMgr.NewTable("users", []Index{{Name: "id", Primary: true, Type: TypeInt}}, 4, hm); err != nil {
		t.Fatalf("new table: %v", err)
	}

	walWriter, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("new wal writer: %v", err)
	}

	se, err := NewStorageEngine(tableMgr, walWriter)
	if err != nil {
		walWriter.Close()
		t.Fatalf("new storage engine: %v", err)
	}

	tx := se.BeginWriteTransaction()
	if err := tx.Put("users", "id", types.IntKey(1), `{"id":1}`); err != nil {
		t.Fatalf("tx put: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("tx rollback: %v", err)
	}

	if err := se.Close(); err != nil {
		t.Fatalf("engine close: %v", err)
	}

	reader, err := wal.NewWALReader(walPath)
	if err != nil {
		t.Fatalf("new wal reader: %v", err)
	}
	defer reader.Close()

	begin, err := reader.ReadEntry()
	if err != nil {
		t.Fatalf("read begin entry: %v", err)
	}
	defer wal.ReleaseEntry(begin)

	abort, err := reader.ReadEntry()
	if err != nil {
		t.Fatalf("read abort entry: %v", err)
	}
	defer wal.ReleaseEntry(abort)

	if begin.Header.EntryType != wal.EntryBegin {
		t.Fatalf("expected first entry BEGIN, got %d", begin.Header.EntryType)
	}
	if abort.Header.EntryType != wal.EntryAbort {
		t.Fatalf("expected second entry ABORT, got %d", abort.Header.EntryType)
	}

	beginTxID, _, transactional, err := unwrapTxPayload(begin.Header, begin.Payload)
	if err != nil {
		t.Fatalf("unwrap begin tx payload: %v", err)
	}
	if !transactional {
		t.Fatalf("expected BEGIN marker to be tx-aware")
	}

	abortTxID, _, transactional, err := unwrapTxPayload(abort.Header, abort.Payload)
	if err != nil {
		t.Fatalf("unwrap abort tx payload: %v", err)
	}
	if !transactional {
		t.Fatalf("expected ABORT marker to be tx-aware")
	}
	if beginTxID != abortTxID {
		t.Fatalf("BEGIN/ABORT txid mismatch: %d vs %d", beginTxID, abortTxID)
	}
}
