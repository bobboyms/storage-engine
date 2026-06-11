package wal

import (
	"errors"
	"io"
	"path/filepath"
	"testing"
)

// TestWALReader_SetLimitLSN is the reader-side primitive for point-in-time
// recovery: with a limit set, the log must appear to end right before the
// first entry whose LSN exceeds the target, exactly as if the process had
// crashed at that moment.
func TestWALReader_SetLimitLSN(t *testing.T) {
	path := filepath.Join(t.TempDir(), "limit.wal")
	w, err := NewWALWriter(path, Options{SyncPolicy: SyncEveryWrite})
	if err != nil {
		t.Fatalf("NewWALWriter: %v", err)
	}
	for lsn := uint64(1); lsn <= 5; lsn++ {
		entry := makeTestEntry(lsn)
		err := w.WriteEntry(entry)
		ReleaseEntry(entry)
		if err != nil {
			t.Fatalf("WriteEntry %d: %v", lsn, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewWALReader(path)
	if err != nil {
		t.Fatalf("NewWALReader: %v", err)
	}
	defer func() { _ = r.Close() }()
	r.SetLimitLSN(3)

	var got []uint64
	for {
		entry, err := r.ReadEntry()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadEntry: %v", err)
		}
		got = append(got, entry.Header.LSN)
	}
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("expected LSNs [1 2 3] with limit 3, got %v", got)
	}
}
