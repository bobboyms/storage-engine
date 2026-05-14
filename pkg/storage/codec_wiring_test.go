package storage_test

import (
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/codec"
	"github.com/bobboyms/storage-engine/pkg/codec/bsoncodec"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// recordingCodec wraps bsoncodec and counts method invocations so we can prove
// the engine routes encoding/decoding through Options.Codec instead of calling
// BSON helpers directly.
type recordingCodec struct {
	inner       codec.Codec
	parseCalls  atomic.Int64
	openCalls   atomic.Int64
	decodeCalls atomic.Int64
}

func newRecordingCodec() *recordingCodec {
	return &recordingCodec{inner: bsoncodec.New()}
}

func (r *recordingCodec) Parse(doc string) (codec.Document, error) {
	r.parseCalls.Add(1)
	return r.inner.Parse(doc)
}

func (r *recordingCodec) Open(raw []byte) (codec.Document, error) {
	r.openCalls.Add(1)
	return r.inner.Open(raw)
}

func (r *recordingCodec) DecodeToText(raw []byte) (string, error) {
	r.decodeCalls.Add(1)
	return r.inner.DecodeToText(raw)
}

func TestEngineUsesCodecFromOptions(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "heap.data")

	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("heap: %v", err)
	}

	tableMgr := storage.NewTableMenager()
	if err := tableMgr.NewTable("users", []storage.Index{{Name: "id", Primary: true, Type: storage.TypeInt}}, 3, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}

	walWriter, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("wal: %v", err)
	}

	rec := newRecordingCodec()
	se, err := storage.NewStorageEngineWithOptions(tableMgr, walWriter, storage.Options{Codec: rec})
	if err != nil {
		t.Fatalf("NewStorageEngineWithOptions: %v", err)
	}
	defer se.Close()

	if err := se.Put("users", "id", types.IntKey(1), `{"id": 1, "name": "alice"}`); err != nil {
		t.Fatalf("Put: %v", err)
	}

	raw, found, err := se.GetBytes("users", "id", types.IntKey(1))
	if err != nil || !found {
		t.Fatalf("GetBytes: found=%v err=%v", found, err)
	}
	if len(raw) == 0 {
		t.Fatal("expected non-empty document bytes")
	}

	if rec.parseCalls.Load() == 0 {
		t.Fatal("expected codec.Parse to be called during Put")
	}
	// Reads now bypass the codec entirely (GetBytes returns raw heap
	// bytes), so we don't assert anything on decodeCalls. Sanity-check
	// the bytes round-trip through the recording codec to prove they
	// were the canonical form the codec produced on Put.
	if text, err := rec.DecodeToText(raw); err != nil || text == "" {
		t.Fatalf("recorded codec failed to decode stored bytes: %v / %q", err, text)
	}
}
