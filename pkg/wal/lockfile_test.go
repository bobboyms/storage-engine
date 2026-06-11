package wal

import (
	"path/filepath"
	"testing"
)

// TestNewWALWriter_RefusesConcurrentWriterOnSamePath guards against the
// classic double-open corruption: two engines (processes or goroutines)
// appending to the same WAL interleave pages and destroy the log. The
// second open must fail while the first writer is alive, and succeed again
// after it closes.
func TestNewWALWriter_RefusesConcurrentWriterOnSamePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locked.wal")

	w1, err := NewWALWriter(path, Options{SyncPolicy: SyncEveryWrite})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}

	if w2, err := NewWALWriter(path, Options{SyncPolicy: SyncEveryWrite}); err == nil {
		_ = w2.Close()
		_ = w1.Close()
		t.Fatal("second writer on the same WAL path must fail while the first is open")
	}

	// The refused open must not have broken the active writer.
	entry := makeTestEntry(1)
	err = w1.WriteEntry(entry)
	ReleaseEntry(entry)
	if err != nil {
		t.Fatalf("active writer must keep working after a refused second open: %v", err)
	}

	if err := w1.Close(); err != nil {
		t.Fatalf("close first writer: %v", err)
	}

	w3, err := NewWALWriter(path, Options{SyncPolicy: SyncEveryWrite})
	if err != nil {
		t.Fatalf("reopen after close must succeed: %v", err)
	}
	if err := w3.Close(); err != nil {
		t.Fatalf("close reopened writer: %v", err)
	}
}
