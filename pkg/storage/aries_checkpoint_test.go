package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func TestARIES_CheckpointPayloadRoundTrip(t *testing.T) {
	beginLSN := uint64(42)
	dpt := []dirtyPageEntry{
		{Path: "/tmp/heap.data", PageID: 7, RecLSN: 30},
		{Path: "/tmp/index.btree", PageID: 3, RecLSN: 12},
	}
	att := []activeTxEntry{
		{TxID: 9, LastLSN: 21},
	}
	raw := serializeCheckpointPayloadV2(beginLSN, dpt, att)
	gotBegin, gotDPT, gotATT, err := parseCheckpointPayload(raw)
	if err != nil {
		t.Fatalf("parseCheckpointPayload: %v", err)
	}
	if gotBegin != beginLSN {
		t.Fatalf("beginLSN: got %d want %d", gotBegin, beginLSN)
	}
	if len(gotDPT) != len(dpt) {
		t.Fatalf("DPT len: got %d want %d", len(gotDPT), len(dpt))
	}
	for i := range dpt {
		if gotDPT[i] != dpt[i] {
			t.Fatalf("DPT[%d]: got %+v want %+v", i, gotDPT[i], dpt[i])
		}
	}
	if len(gotATT) != len(att) || gotATT[0] != att[0] {
		t.Fatalf("ATT mismatch: %+v vs %+v", gotATT, att)
	}

	if got := minRecLSN(dpt); got != 12 {
		t.Fatalf("minRecLSN: got %d want 12", got)
	}
}

func TestARIES_CheckpointPayloadV1Compat(t *testing.T) {
	raw := make([]byte, 8)
	raw[0] = 99
	begin, dpt, att, err := parseCheckpointPayload(raw)
	if err != nil {
		t.Fatalf("v1 payload: %v", err)
	}
	if begin != 99 {
		t.Fatalf("v1 beginLSN: got %d want 99", begin)
	}
	if dpt != nil || att != nil {
		t.Fatal("v1 payload should not yield DPT/ATT")
	}
}

func TestARIES_FuzzyCheckpointPersistsDPT(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("heap: %v", err)
	}
	tm := NewTableMenager()
	if err := tm.NewTable("users", []Index{{Name: "id", Primary: true, Type: TypeInt}}, 4, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	ww, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("wal: %v", err)
	}
	se, err := NewStorageEngine(tm, ww)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	defer se.Close()

	for i := 1; i <= 4; i++ {
		doc := `{"id":` + itoa(i) + `}`
		if err := se.Put(context.Background(), "users", "id", types.IntKey(i), doc); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	if err := se.FuzzyCheckpoint(context.Background()); err != nil {
		t.Fatalf("FuzzyCheckpoint: %v", err)
	}
	_ = se.WAL.Sync()

	reader, err := wal.NewWALReaderWithCipher(walPath, ww.Cipher())
	if err != nil {
		t.Fatalf("wal reader: %v", err)
	}
	defer reader.Close()

	foundCheckpoint := false
	for {
		entry, err := reader.ReadEntry()
		if err != nil {
			break
		}
		if entry.Header.EntryType == wal.EntryCheckpoint {
			foundCheckpoint = true
			_, dpt, _, perr := parseCheckpointPayload(entry.Payload)
			if perr != nil {
				t.Fatalf("parseCheckpointPayload: %v", perr)
			}
			// We just wrote 4 docs and immediately checkpointed. The
			// DPT snapshot is taken BEFORE the flush, so it must
			// have at least one entry. After flush they will be
			// clean — but the persisted snapshot still carries the
			// pre-flush state.
			if len(dpt) == 0 {
				t.Fatal("expected DPT snapshot to be non-empty in checkpoint payload")
			}
		}
		wal.ReleaseEntry(entry)
	}
	if !foundCheckpoint {
		t.Fatal("no EntryCheckpoint found in WAL")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}
