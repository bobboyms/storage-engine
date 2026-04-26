package storage

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func writeTxDeleteForTest(t *testing.T, w *wal.WALWriter, txID, lsn uint64, tableName, indexName string, key types.Comparable) {
	t.Helper()

	payload, err := SerializeDocumentEntry(tableName, indexName, key, nil)
	if err != nil {
		t.Fatalf("serialize tx delete: %v", err)
	}

	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = txAwareWALVersion
	entry.Header.EntryType = wal.EntryDelete
	entry.Header.LSN = lsn
	entry.Payload = append(entry.Payload, wrapTxPayload(txID, payload)...)
	entry.Header.PayloadLen = uint32(len(entry.Payload))
	entry.Header.CRC32 = wal.CalculateCRC32(entry.Payload)

	if err := w.WriteEntry(entry); err != nil {
		wal.ReleaseEntry(entry)
		t.Fatalf("write tx delete lsn=%d: %v", lsn, err)
	}
	wal.ReleaseEntry(entry)
}

func writeTxMultiInsertForTest(t *testing.T, w *wal.WALWriter, txID, lsn uint64, tableName string, keys map[string]types.Comparable, doc string) {
	t.Helper()

	payload, err := SerializeMultiIndexEntry(tableName, keys, []byte(doc))
	if err != nil {
		t.Fatalf("serialize tx multi-insert: %v", err)
	}

	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = txAwareWALVersion
	entry.Header.EntryType = wal.EntryMultiInsert
	entry.Header.LSN = lsn
	entry.Payload = append(entry.Payload, wrapTxPayload(txID, payload)...)
	entry.Header.PayloadLen = uint32(len(entry.Payload))
	entry.Header.CRC32 = wal.CalculateCRC32(entry.Payload)

	if err := w.WriteEntry(entry); err != nil {
		wal.ReleaseEntry(entry)
		t.Fatalf("write tx multi-insert lsn=%d: %v", lsn, err)
	}
	wal.ReleaseEntry(entry)
}

func openRecoveryEngineWithIndexes(t *testing.T, fx recoveryFixture, indexes []Index, production bool) *StorageEngine {
	t.Helper()

	hm, err := NewHeapForTable(HeapFormatV2, fx.heapPath)
	if err != nil {
		t.Fatalf("NewHeapForTable: %v", err)
	}

	meta := NewTableMenager()
	if err := meta.NewTable(fx.tableName, indexes, 0, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}

	ww, err := wal.NewWALWriter(fx.walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("NewWALWriter: %v", err)
	}

	if production {
		se, err := NewProductionStorageEngine(meta, ww)
		if err != nil {
			_ = ww.Close()
			t.Fatalf("NewProductionStorageEngine: %v", err)
		}
		return se
	}

	se, err := NewStorageEngine(meta, ww)
	if err != nil {
		_ = ww.Close()
		t.Fatalf("NewStorageEngine: %v", err)
	}
	return se
}

func TestRecovery_UndoLoserTransactionAcrossPages(t *testing.T) {
	dir := t.TempDir()
	fx := newRecoveryFixture(dir, "users")

	base := openRecoveryEngine(t, fx, false)
	if err := base.Put("users", "id", types.IntKey(1), `{"id":1,"name":"before-update"}`); err != nil {
		t.Fatalf("base Put key 1: %v", err)
	}
	if err := base.Put("users", "id", types.IntKey(2), `{"id":2,"name":"before-delete"}`); err != nil {
		t.Fatalf("base Put key 2: %v", err)
	}
	if err := base.Close(); err != nil {
		t.Fatalf("base Close: %v", err)
	}

	se := openRecoveryEngine(t, fx, false)

	const txID uint64 = 9001
	writeTxMarkerForTest(t, se.WAL, txID, 10, wal.EntryBegin)
	writeTxDocumentForTest(t, se.WAL, txID, 11, "users", "id", types.IntKey(100), fmt.Sprintf(`{"id":100,"payload":"%s"}`, repeatedText("a", 3000)))
	writeTxDocumentForTest(t, se.WAL, txID, 12, "users", "id", types.IntKey(1), fmt.Sprintf(`{"id":1,"name":"after-update","payload":"%s"}`, repeatedText("b", 3000)))
	writeTxDeleteForTest(t, se.WAL, txID, 13, "users", "id", types.IntKey(2))
	writeTxDocumentForTest(t, se.WAL, txID, 14, "users", "id", types.IntKey(101), fmt.Sprintf(`{"id":101,"payload":"%s"}`, repeatedText("c", 3000)))

	loadedLSNs := make(map[string]uint64)
	for _, spec := range []struct {
		lsn       uint64
		entryType uint8
		key       types.Comparable
		document  []byte
	}{
		{lsn: 11, entryType: wal.EntryInsert, key: types.IntKey(100), document: []byte(fmt.Sprintf(`{"id":100,"payload":"%s"}`, repeatedText("a", 3000)))},
		{lsn: 12, entryType: wal.EntryInsert, key: types.IntKey(1), document: []byte(fmt.Sprintf(`{"id":1,"name":"after-update","payload":"%s"}`, repeatedText("b", 3000)))},
		{lsn: 13, entryType: wal.EntryDelete, key: types.IntKey(2)},
		{lsn: 14, entryType: wal.EntryInsert, key: types.IntKey(101), document: []byte(fmt.Sprintf(`{"id":101,"payload":"%s"}`, repeatedText("c", 3000)))},
	} {
		payload, err := SerializeDocumentEntry("users", "id", spec.key, spec.document)
		if err != nil {
			t.Fatalf("SerializeDocumentEntry lsn=%d: %v", spec.lsn, err)
		}
		entry := wal.AcquireEntry()
		entry.Header.Magic = wal.WALMagic
		entry.Header.Version = txAwareWALVersion
		entry.Header.EntryType = spec.entryType
		entry.Header.LSN = spec.lsn
		entry.Payload = append(entry.Payload, wrapTxPayload(txID, payload)...)
		entry.Header.PayloadLen = uint32(len(entry.Payload))
		entry.Header.CRC32 = wal.CalculateCRC32(entry.Payload)
		body, shouldRedo, err := (&recoveryAnalysis{CommittedTxs: map[uint64]struct{}{txID: {}}}).shouldRedo(entry)
		if err != nil {
			wal.ReleaseEntry(entry)
			t.Fatalf("shouldRedo lsn=%d: %v", spec.lsn, err)
		}
		if !shouldRedo {
			wal.ReleaseEntry(entry)
			t.Fatalf("expected injected loser lsn=%d to be applied manually", spec.lsn)
		}
		if err := se.redoDocumentEntry(entry, body, loadedLSNs); err != nil {
			wal.ReleaseEntry(entry)
			t.Fatalf("redoDocumentEntry lsn=%d: %v", spec.lsn, err)
		}
		wal.ReleaseEntry(entry)
	}

	if err := se.Close(); err != nil {
		t.Fatalf("inject Close: %v", err)
	}

	recovered := openRecoveryEngine(t, fx, true)
	defer recovered.Close()

	requireDocumentVisible(t, recovered, "users", 1, `{"id":1,"name":"before-update"}`)
	requireDocumentVisible(t, recovered, "users", 2, `{"id":2,"name":"before-delete"}`)

	if _, found, err := recovered.Get("users", "id", types.IntKey(100)); err != nil {
		t.Fatalf("Get loser insert 100: %v", err)
	} else if found {
		t.Fatalf("loser insert key 100 should have been undone")
	}
	if _, found, err := recovered.Get("users", "id", types.IntKey(101)); err != nil {
		t.Fatalf("Get loser insert 101: %v", err)
	} else if found {
		t.Fatalf("loser insert key 101 should have been undone")
	}
}

func TestRecovery_UndoLoserMultiIndexUpdateRestoresSecondaryIndexes(t *testing.T) {
	dir := t.TempDir()
	fx := recoveryFixture{
		tableName: "users",
		walPath:   filepath.Join(dir, "users.wal"),
		heapPath:  filepath.Join(dir, "users.heap"),
	}

	indexes := []Index{
		{Name: "id", Primary: true, Type: TypeInt},
		{Name: "email", Primary: false, Type: TypeVarchar},
	}

	base := openRecoveryEngineWithIndexes(t, fx, indexes, false)
	if err := base.InsertRow("users", `{"id":1,"email":"before@example.com","name":"Before"}`, map[string]types.Comparable{
		"id":    types.IntKey(1),
		"email": types.VarcharKey("before@example.com"),
	}); err != nil {
		t.Fatalf("InsertRow base: %v", err)
	}
	if err := base.Close(); err != nil {
		t.Fatalf("base Close: %v", err)
	}

	se := openRecoveryEngineWithIndexes(t, fx, indexes, false)

	const txID uint64 = 9010
	keys := map[string]types.Comparable{
		"id":    types.IntKey(1),
		"email": types.VarcharKey("after@example.com"),
	}
	writeTxMarkerForTest(t, se.WAL, txID, 20, wal.EntryBegin)
	writeTxMultiInsertForTest(t, se.WAL, txID, 21, "users", keys, `{"id":1,"email":"after@example.com","name":"After"}`)

	payload, err := SerializeMultiIndexEntry("users", keys, []byte(`{"id":1,"email":"after@example.com","name":"After"}`))
	if err != nil {
		t.Fatalf("SerializeMultiIndexEntry: %v", err)
	}
	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = txAwareWALVersion
	entry.Header.EntryType = wal.EntryMultiInsert
	entry.Header.LSN = 21
	entry.Payload = append(entry.Payload, wrapTxPayload(txID, payload)...)
	entry.Header.PayloadLen = uint32(len(entry.Payload))
	entry.Header.CRC32 = wal.CalculateCRC32(entry.Payload)
	body, shouldRedo, err := (&recoveryAnalysis{CommittedTxs: map[uint64]struct{}{txID: {}}}).shouldRedo(entry)
	if err != nil {
		wal.ReleaseEntry(entry)
		t.Fatalf("shouldRedo: %v", err)
	}
	if !shouldRedo {
		wal.ReleaseEntry(entry)
		t.Fatal("expected injected multi-index loser to be applied manually")
	}
	if err := se.redoMultiInsertEntry(entry, body, map[string]uint64{}); err != nil {
		wal.ReleaseEntry(entry)
		t.Fatalf("redoMultiInsertEntry: %v", err)
	}
	wal.ReleaseEntry(entry)

	if err := se.Close(); err != nil {
		t.Fatalf("inject Close: %v", err)
	}

	recovered := openRecoveryEngineWithIndexes(t, fx, indexes, true)
	defer recovered.Close()

	requireDocumentVisible(t, recovered, "users", 1, `{"id":1,"email":"before@example.com","name":"Before"}`)
	if _, found, err := recovered.Get("users", "email", types.VarcharKey("before@example.com")); err != nil {
		t.Fatalf("Get old email: %v", err)
	} else if !found {
		t.Fatalf("old secondary key should have been restored")
	}
	if _, found, err := recovered.Get("users", "email", types.VarcharKey("after@example.com")); err != nil {
		t.Fatalf("Get new email: %v", err)
	} else if found {
		t.Fatalf("new secondary key from loser update should have been undone")
	}
}

func TestRecovery_CrashDuringUndoRemainsRecoverable(t *testing.T) {
	dir := t.TempDir()
	fx := newRecoveryFixture(dir, "users")

	base := openRecoveryEngine(t, fx, false)
	if err := base.Put("users", "id", types.IntKey(1), `{"id":1,"name":"before-update"}`); err != nil {
		t.Fatalf("base Put key 1: %v", err)
	}
	if err := base.Put("users", "id", types.IntKey(2), `{"id":2,"name":"before-delete"}`); err != nil {
		t.Fatalf("base Put key 2: %v", err)
	}
	if err := base.Close(); err != nil {
		t.Fatalf("base Close: %v", err)
	}

	se := openRecoveryEngine(t, fx, false)

	const txID uint64 = 9020
	writeTxMarkerForTest(t, se.WAL, txID, 30, wal.EntryBegin)
	writeTxDocumentForTest(t, se.WAL, txID, 31, "users", "id", types.IntKey(3), `{"id":3,"name":"loser-insert"}`)
	writeTxDocumentForTest(t, se.WAL, txID, 32, "users", "id", types.IntKey(1), `{"id":1,"name":"after-update"}`)
	writeTxDeleteForTest(t, se.WAL, txID, 33, "users", "id", types.IntKey(2))

	loadedLSNs := make(map[string]uint64)
	for _, spec := range []struct {
		lsn       uint64
		entryType uint8
		key       types.Comparable
		document  []byte
	}{
		{lsn: 31, entryType: wal.EntryInsert, key: types.IntKey(3), document: []byte(`{"id":3,"name":"loser-insert"}`)},
		{lsn: 32, entryType: wal.EntryInsert, key: types.IntKey(1), document: []byte(`{"id":1,"name":"after-update"}`)},
		{lsn: 33, entryType: wal.EntryDelete, key: types.IntKey(2)},
	} {
		payload, err := SerializeDocumentEntry("users", "id", spec.key, spec.document)
		if err != nil {
			t.Fatalf("SerializeDocumentEntry lsn=%d: %v", spec.lsn, err)
		}
		entry := wal.AcquireEntry()
		entry.Header.Magic = wal.WALMagic
		entry.Header.Version = txAwareWALVersion
		entry.Header.EntryType = spec.entryType
		entry.Header.LSN = spec.lsn
		entry.Payload = append(entry.Payload, wrapTxPayload(txID, payload)...)
		entry.Header.PayloadLen = uint32(len(entry.Payload))
		entry.Header.CRC32 = wal.CalculateCRC32(entry.Payload)
		body, shouldRedo, err := (&recoveryAnalysis{CommittedTxs: map[uint64]struct{}{txID: {}}}).shouldRedo(entry)
		if err != nil {
			wal.ReleaseEntry(entry)
			t.Fatalf("shouldRedo lsn=%d: %v", spec.lsn, err)
		}
		if !shouldRedo {
			wal.ReleaseEntry(entry)
			t.Fatalf("expected injected loser lsn=%d to be applied manually", spec.lsn)
		}
		if err := se.redoDocumentEntry(entry, body, loadedLSNs); err != nil {
			wal.ReleaseEntry(entry)
			t.Fatalf("redoDocumentEntry lsn=%d: %v", spec.lsn, err)
		}
		wal.ReleaseEntry(entry)
	}
	if err := se.Close(); err != nil {
		t.Fatalf("inject Close: %v", err)
	}

	midRecovery := openRecoveryEngine(t, fx, false)
	analysis, err := midRecovery.analyzeRecovery(fx.walPath)
	if err != nil {
		t.Fatalf("analyzeRecovery: %v", err)
	}
	steps, err := midRecovery.undoLoserTransactionsWithLimit(fx.walPath, midRecovery.walCipher(), analysis, 1)
	if err != nil {
		t.Fatalf("undoLoserTransactionsWithLimit: %v", err)
	}
	if steps != 1 {
		t.Fatalf("expected exactly one undo step before simulated crash, got %d", steps)
	}
	if err := midRecovery.Close(); err != nil {
		t.Fatalf("midRecovery Close: %v", err)
	}

	recovered := openRecoveryEngine(t, fx, true)
	defer recovered.Close()

	requireDocumentVisible(t, recovered, "users", 1, `{"id":1,"name":"before-update"}`)
	requireDocumentVisible(t, recovered, "users", 2, `{"id":2,"name":"before-delete"}`)
	if _, found, err := recovered.Get("users", "id", types.IntKey(3)); err != nil {
		t.Fatalf("Get loser insert 3: %v", err)
	} else if found {
		t.Fatalf("loser insert key 3 should have been undone after recovery-of-recovery")
	}
}

func repeatedText(ch string, n int) string {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = ch[0]
	}
	return string(buf)
}
