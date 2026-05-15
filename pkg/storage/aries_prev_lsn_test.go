package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// TestARIES_TxEntriesChainViaPrevLSN locks in the prevLSN invariant from
// ARIES: every WAL entry that belongs to a transaction carries the LSN
// of the previous entry of that same transaction. The chain lets undo
// walk backward without re-scanning the whole WAL, and lets recovery
// resume an interrupted undo at the right place.
func TestARIES_TxEntriesChainViaPrevLSN(t *testing.T) {
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
		t.Fatalf("wal writer: %v", err)
	}
	se, err := NewStorageEngine(tm, ww)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	t.Cleanup(func() { _ = se.Close() })

	ctx := context.Background()
	tx := se.BeginWriteTransaction()
	if err := tx.Put(ctx, "users", "id", types.IntKey(1), `{"id":1}`); err != nil {
		t.Fatalf("Put 1: %v", err)
	}
	if err := tx.Put(ctx, "users", "id", types.IntKey(2), `{"id":2}`); err != nil {
		t.Fatalf("Put 2: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	_ = se.WAL.Sync()

	// Walk the WAL collecting every entry that belongs to a tx and
	// confirm prevLSN of entry N+1 equals LSN of entry N within the
	// same tx.
	reader, err := wal.NewWALReaderWithCipher(walPath, ww.Cipher())
	if err != nil {
		t.Fatalf("wal reader: %v", err)
	}
	defer reader.Close()

	type seen struct{ lsn, prev uint64 }
	chain := map[uint64][]seen{}
	for {
		entry, err := reader.ReadEntry()
		if err != nil {
			break
		}
		txID, _, prevLSN, ok, herr := unwrapTxPayloadWithPrev(entry.Header, entry.Payload)
		if herr != nil {
			t.Fatalf("unwrap: %v", herr)
		}
		if ok {
			chain[txID] = append(chain[txID], seen{lsn: entry.Header.LSN, prev: prevLSN})
		}
		wal.ReleaseEntry(entry)
	}

	if len(chain) == 0 {
		t.Fatal("no transactional entries found in WAL")
	}
	for txID, entries := range chain {
		if len(entries) < 2 {
			continue
		}
		if entries[0].prev != 0 {
			t.Fatalf("tx %d: first entry prevLSN should be 0, got %d", txID, entries[0].prev)
		}
		for i := 1; i < len(entries); i++ {
			if entries[i].prev != entries[i-1].lsn {
				t.Fatalf("tx %d entry %d: prevLSN=%d, expected %d (prior entry LSN)", txID, i, entries[i].prev, entries[i-1].lsn)
			}
		}
	}
}
