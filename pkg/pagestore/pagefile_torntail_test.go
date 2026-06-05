package pagestore

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/crypto"
)

// TestNewPageFileTolerant_TruncatesTornTrailingPage proves that an
// append-only log (WAL) whose last write was torn by a crash — leaving the
// file at a size that is not a multiple of PageSize — can still be opened.
// The incomplete trailing page is discarded and every fully written page
// before it is preserved. The strict constructor keeps rejecting such files.
func TestNewPageFileTolerant_TruncatesTornTrailingPage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cipher crypto.Cipher
	}{
		{"plain", nil},
		{"encrypted", newCipher(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wal.db")
			pf, err := NewPageFile(path, tc.cipher)
			if err != nil {
				t.Fatal(err)
			}
			usable := pf.cipher.UsableBodySize()

			var wantBodies [][]byte
			for i := 0; i < 2; i++ {
				id, err := pf.AllocatePage()
				if err != nil {
					t.Fatal(err)
				}
				var p Page
				fillBody(&p, byte(10+i), usable)
				if err := pf.WritePage(id, &p); err != nil {
					t.Fatal(err)
				}
				b := make([]byte, usable)
				copy(b, p.Body()[:usable])
				wantBodies = append(wantBodies, b)
			}
			if err := pf.Close(); err != nil {
				t.Fatal(err)
			}

			// Simulate a torn write of the next page: append partial bytes.
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

			info, _ := os.Stat(path)
			if info.Size()%PageSize == 0 {
				t.Fatalf("setup: file unexpectedly aligned (%d)", info.Size())
			}

			// Strict open must still reject the torn tail (corruption signal
			// for random-access stores such as heap/btree).
			if _, err := NewPageFile(path, tc.cipher); err == nil {
				t.Fatal("strict NewPageFile should reject non-aligned file")
			}

			// Tolerant open repairs the torn tail and exposes complete pages.
			pf2, err := NewPageFileTolerant(path, tc.cipher)
			if err != nil {
				t.Fatalf("NewPageFileTolerant: %v", err)
			}
			defer pf2.Close()

			if got := pf2.NumPages(); got != 3 { // reserved slot 0 + 2 data pages
				t.Fatalf("NumPages = %d, want 3", got)
			}
			info2, _ := os.Stat(path)
			if info2.Size()%PageSize != 0 {
				t.Fatalf("tolerant open left non-aligned file size %d", info2.Size())
			}
			for i := 0; i < 2; i++ {
				p, err := pf2.ReadPage(PageID(i + 1))
				if err != nil {
					t.Fatalf("ReadPage(%d): %v", i+1, err)
				}
				if !bytes.Equal(p.Body()[:usable], wantBodies[i]) {
					t.Fatalf("page %d body mismatch after torn-tail repair", i+1)
				}
			}
		})
	}
}
