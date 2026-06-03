package storage

import (
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// FuzzRecoveryDeserializers feeds arbitrary bytes to every deserializer
// on the recovery hot path. These functions parse untrusted on-disk WAL
// payloads during crash recovery, so a corrupt or truncated record must
// always surface as an error — never a panic, slice out-of-range, or
// runaway allocation. The fuzzer fails if any deserializer panics.
func FuzzRecoveryDeserializers(f *testing.F) {
	// Seed corpus: valid payloads so the fuzzer starts from structurally
	// meaningful inputs and mutates outward.
	if doc, err := SerializeDocumentEntry("users", "id", types.IntKey(1), []byte(`{"id":1}`)); err == nil {
		f.Add(doc)
	}
	if multi, err := SerializeMultiIndexEntry("users", map[string]types.Comparable{
		"id": types.IntKey(1),
	}, []byte(`{"id":1}`)); err == nil {
		f.Add(multi)
	}
	f.Add(SerializeCompensationEntry(7, wal.EntryInsert, []byte("payload"), 3))
	f.Add(serializeCheckpointPayloadV2(42, []dirtyPageEntry{{Path: "users.heap", PageID: 1, RecLSN: 9}}, []activeTxEntry{{TxID: 5, LastLSN: 8}}))
	f.Add(serializeNTABeginPayload(NTAKindBTreeSplit, []NTAPage{{
		Path:     "users.btree",
		PageID:   2,
		PreImage: make([]byte, pagestore.PageSize),
	}}))
	if page, err := serializePageRedoPayload("users.heap", 1, &pagestore.Page{}); err == nil {
		f.Add(page)
	}
	f.Add([]byte{})
	f.Add([]byte{0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Each call must return cleanly (value or error) without panicking.
		_, _, _, _, _ = DeserializeDocumentEntry(data)
		_, _, _, _ = DeserializeMultiIndexEntry(data)
		_, _, _, _, _ = DeserializeCompensationEntry(data)
		_, _, _, _ = parseCheckpointPayload(data)
		_, _, _ = deserializeNTABeginPayload(data)
		_, _ = deserializeNTACommitPayload(data)
		_, _, _, _ = deserializePageRedoPayload(data)

		// unwrapTxPayloadWithPrev branches on the header version; exercise
		// each layout (legacy, v2 tx-aware, v3 aries) against the bytes.
		for _, version := range []uint8{1, txAwareWALVersion, ariesWALVersion} {
			_, _, _, _, _ = unwrapTxPayloadWithPrev(wal.WALHeader{Version: version}, data)
		}
	})
}
