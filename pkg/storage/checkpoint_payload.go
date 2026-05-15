package storage

import (
	"encoding/binary"
	"fmt"
)

// Checkpoint payload format versions.
const (
	checkpointPayloadV1 = 1 // legacy: [beginLSN:8]
	checkpointPayloadV2 = 2 // v2: [beginLSN:8][version:1][dptCount:4][dpt...][attCount:4][att...]
)

// dirtyPageEntry is a (path, pageID, recLSN) triple. recLSN is the
// oldest unflushed WAL LSN that has modified the page since it
// transitioned from clean to dirty. Recovery uses min(recLSN) across
// the DPT to bound the physical redo start.
type dirtyPageEntry struct {
	Path   string
	PageID uint64
	RecLSN uint64
}

// activeTxEntry is a (txID, lastLSN) pair snapshotted from the engine's
// active transaction table at checkpoint time. Analysis seeds its tx
// table from these so it does not have to rescan back to WAL start.
type activeTxEntry struct {
	TxID    uint64
	LastLSN uint64
}

// serializeCheckpointPayloadV2 packs beginLSN + DPT + ATT into the
// checkpoint record body. The first 8 bytes match the v1 layout so old
// readers can still extract beginLSN cleanly.
func serializeCheckpointPayloadV2(beginLSN uint64, dpt []dirtyPageEntry, att []activeTxEntry) []byte {
	size := 8 + 1 + 4
	for _, e := range dpt {
		size += 2 + len(e.Path) + 8 + 8
	}
	size += 4 + len(att)*16

	buf := make([]byte, 0, size)
	var u64 [8]byte
	var u32 [4]byte
	var u16 [2]byte

	binary.LittleEndian.PutUint64(u64[:], beginLSN)
	buf = append(buf, u64[:]...)
	buf = append(buf, checkpointPayloadV2)
	binary.LittleEndian.PutUint32(u32[:], uint32(len(dpt))) //nolint:gosec // dpt size bounded by buffer pool capacity
	buf = append(buf, u32[:]...)
	for _, e := range dpt {
		binary.LittleEndian.PutUint16(u16[:], uint16(len(e.Path))) //nolint:gosec // table file path length capped by filesystem
		buf = append(buf, u16[:]...)
		buf = append(buf, []byte(e.Path)...)
		binary.LittleEndian.PutUint64(u64[:], e.PageID)
		buf = append(buf, u64[:]...)
		binary.LittleEndian.PutUint64(u64[:], e.RecLSN)
		buf = append(buf, u64[:]...)
	}
	binary.LittleEndian.PutUint32(u32[:], uint32(len(att))) //nolint:gosec // att size bounded by active tx count
	buf = append(buf, u32[:]...)
	for _, e := range att {
		binary.LittleEndian.PutUint64(u64[:], e.TxID)
		buf = append(buf, u64[:]...)
		binary.LittleEndian.PutUint64(u64[:], e.LastLSN)
		buf = append(buf, u64[:]...)
	}
	return buf
}

// parseCheckpointPayload extracts beginLSN and, when present, the DPT
// and ATT snapshots. v1 payloads return nil/nil for both.
func parseCheckpointPayload(payload []byte) (beginLSN uint64, dpt []dirtyPageEntry, att []activeTxEntry, err error) {
	if len(payload) < 8 {
		return 0, nil, nil, fmt.Errorf("checkpoint payload too short")
	}
	beginLSN = binary.LittleEndian.Uint64(payload[:8])
	if len(payload) == 8 {
		return beginLSN, nil, nil, nil
	}
	if len(payload) < 9 {
		return 0, nil, nil, fmt.Errorf("checkpoint payload missing version byte")
	}
	version := payload[8]
	if version != checkpointPayloadV2 {
		// Unknown version → treat as v1 best-effort, ignore trailing
		// bytes rather than corrupt recovery.
		return beginLSN, nil, nil, nil
	}
	off := 9
	if off+4 > len(payload) {
		return 0, nil, nil, fmt.Errorf("checkpoint payload truncated at dpt count")
	}
	dptCount := binary.LittleEndian.Uint32(payload[off : off+4])
	off += 4
	dpt = make([]dirtyPageEntry, 0, dptCount)
	for i := uint32(0); i < dptCount; i++ {
		if off+2 > len(payload) {
			return 0, nil, nil, fmt.Errorf("checkpoint payload truncated at dpt[%d] pathLen", i)
		}
		pathLen := int(binary.LittleEndian.Uint16(payload[off : off+2]))
		off += 2
		if off+pathLen+16 > len(payload) {
			return 0, nil, nil, fmt.Errorf("checkpoint payload truncated at dpt[%d] body", i)
		}
		entry := dirtyPageEntry{Path: string(payload[off : off+pathLen])}
		off += pathLen
		entry.PageID = binary.LittleEndian.Uint64(payload[off : off+8])
		off += 8
		entry.RecLSN = binary.LittleEndian.Uint64(payload[off : off+8])
		off += 8
		dpt = append(dpt, entry)
	}
	if off+4 > len(payload) {
		return 0, nil, nil, fmt.Errorf("checkpoint payload truncated at att count")
	}
	attCount := binary.LittleEndian.Uint32(payload[off : off+4])
	off += 4
	att = make([]activeTxEntry, 0, attCount)
	for i := uint32(0); i < attCount; i++ {
		if off+16 > len(payload) {
			return 0, nil, nil, fmt.Errorf("checkpoint payload truncated at att[%d]", i)
		}
		entry := activeTxEntry{
			TxID:    binary.LittleEndian.Uint64(payload[off : off+8]),
			LastLSN: binary.LittleEndian.Uint64(payload[off+8 : off+16]),
		}
		off += 16
		att = append(att, entry)
	}
	return beginLSN, dpt, att, nil
}

// minRecLSN returns the smallest RecLSN in the DPT. Zero entries are
// ignored (they signal a page that is clean now). Returns 0 if the DPT
// is empty or has no positive RecLSN.
func minRecLSN(dpt []dirtyPageEntry) uint64 {
	var min uint64
	for _, e := range dpt {
		if e.RecLSN == 0 {
			continue
		}
		if min == 0 || e.RecLSN < min {
			min = e.RecLSN
		}
	}
	return min
}
