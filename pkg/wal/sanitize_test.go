package wal

import (
	"io"
	"math"
	"path/filepath"
	"testing"
)

func writeSanitizeEntry(t *testing.T, w *WALWriter, entryType uint8, lsn uint64, payload []byte) {
	t.Helper()
	entry := AcquireEntry()
	entry.Header.Magic = WALMagic
	entry.Header.Version = WALVersion
	entry.Header.EntryType = entryType
	entry.Header.LSN = lsn
	entry.Header.PayloadLen = uint32(len(payload)) //nolint:gosec // small test payloads
	entry.Header.CRC32 = CalculateCRC32(payload)
	entry.Payload = append(entry.Payload, payload...)
	if err := w.WriteEntry(entry); err != nil {
		t.Fatalf("write entry lsn=%d: %v", lsn, err)
	}
	ReleaseEntry(entry)
}

func readAllEntries(t *testing.T, base string) []WALHeader {
	t.Helper()
	r, err := NewWALReader(base)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer func() { _ = r.Close() }()
	var headers []WALHeader
	for {
		e, err := r.ReadEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read entry: %v", err)
		}
		headers = append(headers, e.Header)
		ReleaseEntry(e)
	}
	return headers
}

// TestSanitizeWAL_DropsMatchingEntriesAcrossSegments pins the fsck WAL
// repair: entries matching the drop predicate are removed from every segment
// (archived and active), surviving entries keep their LSNs and payloads, and
// the log stays readable end to end.
func TestSanitizeWAL_DropsMatchingEntriesAcrossSegments(t *testing.T) {
	base := filepath.Join(t.TempDir(), "data.wal")
	w, err := NewWALWriter(base, DefaultOptions())
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}

	writeSanitizeEntry(t, w, EntryMultiInsert, 1, []byte("keep-1"))
	writeSanitizeEntry(t, w, EntryPageRedo, math.MaxUint64, []byte("drop-1"))
	// Rotate so the poisoned entry above lands in an archived segment.
	if err := w.CheckpointLifecycle(0); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	writeSanitizeEntry(t, w, EntryMultiInsert, 2, []byte("keep-2"))
	writeSanitizeEntry(t, w, EntryPageRedo, math.MaxUint64, []byte("drop-2"))
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	dropped, err := SanitizeWAL(base, nil, func(h WALHeader, _ []byte) bool {
		return h.LSN == math.MaxUint64
	})
	if err != nil {
		t.Fatalf("SanitizeWAL: %v", err)
	}
	if dropped != 2 {
		t.Fatalf("dropped = %d, want 2", dropped)
	}

	headers := readAllEntries(t, base)
	if len(headers) != 2 {
		t.Fatalf("surviving entries = %d, want 2: %+v", len(headers), headers)
	}
	if headers[0].LSN != 1 || headers[1].LSN != 2 {
		t.Fatalf("surviving LSNs = %d,%d, want 1,2", headers[0].LSN, headers[1].LSN)
	}

	// The log must still accept a writer afterwards (locks released, files
	// structurally valid).
	w2, err := NewWALWriter(base, DefaultOptions())
	if err != nil {
		t.Fatalf("reopen writer after sanitize: %v", err)
	}
	writeSanitizeEntry(t, w2, EntryMultiInsert, 3, []byte("keep-3"))
	if err := w2.Close(); err != nil {
		t.Fatalf("close reopened writer: %v", err)
	}
	if headers := readAllEntries(t, base); len(headers) != 3 {
		t.Fatalf("entries after append = %d, want 3", len(headers))
	}
}

func TestSanitizeWAL_NoMatchesIsNoop(t *testing.T) {
	base := filepath.Join(t.TempDir(), "data.wal")
	w, err := NewWALWriter(base, DefaultOptions())
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	writeSanitizeEntry(t, w, EntryMultiInsert, 1, []byte("keep"))
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	dropped, err := SanitizeWAL(base, nil, func(WALHeader, []byte) bool { return false })
	if err != nil {
		t.Fatalf("SanitizeWAL: %v", err)
	}
	if dropped != 0 {
		t.Fatalf("dropped = %d, want 0", dropped)
	}
	if headers := readAllEntries(t, base); len(headers) != 1 || headers[0].LSN != 1 {
		t.Fatalf("log changed by a no-op sanitize: %+v", headers)
	}
}
