package wal

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/crypto"
)

// TestWALReader_TornEntryMidPayload_NeverYieldsShortEntry locks in the
// durability invariant behind a common worry: "if a kill -9 hit right while a
// record was being written, could recovery have rebuilt that record with its
// last field truncated?". The answer must be no. A record's whole serialized
// body lives inside a single WAL entry payload covered by one CRC32, so a write
// torn in the MIDDLE of that payload must never surface as a shorter, still
// "valid" entry — it must be rejected as an unexpected-EOF tail and dropped.
//
// Setup: a small entry A fully contained in the first page, then a large entry
// B whose payload spans into a later page. The file is then truncated back to a
// page boundary that cuts B in half (its continuation page is gone), exactly as
// a crash mid-append would leave it. Recovery must hand back A intact and then
// stop with io.ErrUnexpectedEOF for B — never an entry carrying a truncated
// payload.
func TestWALReader_TornEntryMidPayload_NeverYieldsShortEntry(t *testing.T) {
	const pageSize = 8192 // pagestore.PageSize; on-disk page granularity

	for _, tc := range []struct {
		name   string
		cipher crypto.Cipher
	}{
		{"plain", nil},
		{"encrypted", newCipher(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "wal.log")

			opts := DefaultOptions()
			opts.SyncPolicy = SyncEveryWrite
			opts.Cipher = tc.cipher

			w, err := NewWALWriter(path, opts)
			if err != nil {
				t.Fatal(err)
			}

			// Entry A: small, fully inside the first content page.
			payloadA := []byte("complete record: id=1,name=alice,active=true")
			a := &WALEntry{
				Header:  WALHeader{Magic: WALMagic, Version: WALVersion, EntryType: EntryInsert, LSN: 1, PayloadLen: uint32(len(payloadA)), CRC32: CalculateCRC32(payloadA)},
				Payload: payloadA,
			}
			if err := w.WriteEntry(a); err != nil {
				t.Fatalf("WriteEntry A: %v", err)
			}

			// Entry B: larger than a single page, so its payload necessarily
			// spans into a later page. Its trailing "field" is what a torn
			// write would chop off.
			payloadB := make([]byte, 9000)
			for i := range payloadB {
				payloadB[i] = byte(i)
			}
			b := &WALEntry{
				Header:  WALHeader{Magic: WALMagic, Version: WALVersion, EntryType: EntryInsert, LSN: 2, PayloadLen: uint32(len(payloadB)), CRC32: CalculateCRC32(payloadB)},
				Payload: payloadB,
			}
			if err := w.WriteEntry(b); err != nil {
				t.Fatalf("WriteEntry B: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			// Self-check: B must genuinely have spilled past the first content
			// page (reserved page 0 + at least 2 content pages), otherwise the
			// truncation below would not cut B mid-payload and the test would
			// not exercise the invariant.
			if info.Size() < 3*pageSize {
				t.Fatalf("setup: expected B to span >= 3 pages, file is only %d bytes", info.Size())
			}

			// Crash mid-append: drop B's continuation page by truncating back to
			// the page that holds A and B's head. The result is page-aligned, so
			// the tolerant open accepts it, but B's payload is now incomplete.
			if err := os.Truncate(path, 2*pageSize); err != nil {
				t.Fatal(err)
			}

			r, err := NewWALReaderWithCipher(path, tc.cipher)
			if err != nil {
				t.Fatalf("reader must open a WAL truncated mid-entry: %v", err)
			}
			defer r.Close()

			// A is fully durable and must come back byte-for-byte.
			gotA, err := r.ReadEntry()
			if err != nil {
				t.Fatalf("ReadEntry A: %v", err)
			}
			if gotA.Header.LSN != 1 || !bytes.Equal(gotA.Payload, payloadA) {
				t.Fatalf("entry A corrupted: LSN=%d len=%d, want LSN=1 len=%d", gotA.Header.LSN, len(gotA.Payload), len(payloadA))
			}
			ReleaseEntry(gotA)

			// B was torn mid-payload. The reader must reject it as an
			// unexpected-EOF tail, NOT reconstruct a shorter "valid" entry.
			gotB, err := r.ReadEntry()
			if gotB != nil {
				t.Fatalf("torn entry must not be reconstructed, got entry LSN=%d with %d-byte payload (declared %d) — last field truncated!", gotB.Header.LSN, len(gotB.Payload), gotB.Header.PayloadLen)
			}
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("torn mid-payload entry: got err %v, want io.ErrUnexpectedEOF", err)
			}
		})
	}
}

// TestWAL_RecoversAfterTornTrailingPage proves that a crash interrupting the
// next page write — leaving the WAL file at a size that is not a multiple of
// PageSize — does not make the whole log unreadable. Every entry written into
// a fully persisted page before the torn tail must still be recovered. This is
// exercised for both plaintext and encrypted (TDE) WALs because the encrypted
// path was the regression: the engine could not re-read the log after a crash.
func TestWAL_RecoversAfterTornTrailingPage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cipher crypto.Cipher
	}{
		{"plain", nil},
		{"encrypted", newCipher(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "wal.log")

			opts := DefaultOptions()
			opts.SyncPolicy = SyncEveryWrite
			opts.Cipher = tc.cipher

			w, err := NewWALWriter(path, opts)
			if err != nil {
				t.Fatal(err)
			}

			// Large payloads so the log spans several pages.
			payload := make([]byte, 4000)
			for i := range payload {
				payload[i] = byte(i)
			}
			const n = 20
			for i := 0; i < n; i++ {
				e := &WALEntry{
					Header:  WALHeader{Magic: WALMagic, Version: WALVersion, EntryType: EntryInsert, LSN: uint64(i + 1), PayloadLen: uint32(len(payload)), CRC32: CalculateCRC32(payload)},
					Payload: payload,
				}
				if err := w.WriteEntry(e); err != nil {
					t.Fatalf("WriteEntry %d: %v", i, err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			// Simulate a crash mid-write of the NEXT page: append partial
			// bytes so the file is no longer page-aligned.
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write(make([]byte, 100)); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}

			r, err := NewWALReaderWithCipher(path, tc.cipher)
			if err != nil {
				t.Fatalf("reader must open a WAL with a torn trailing page: %v", err)
			}
			defer r.Close()

			read := 0
			for {
				e, err := r.ReadEntry()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("ReadEntry after %d entries: %v", read, err)
				}
				if e.Header.LSN != uint64(read+1) {
					t.Fatalf("entry %d: LSN = %d, want %d", read, e.Header.LSN, read+1)
				}
				read++
				ReleaseEntry(e)
			}
			if read != n {
				t.Fatalf("recovered %d entries, want all %d committed before the torn tail", read, n)
			}
		})
	}
}
