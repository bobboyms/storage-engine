package wal

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/crypto"
)

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
