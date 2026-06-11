package wal

import (
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
)

// TestWALWriter_GroupCommit exercises SyncEveryWrite under concurrency:
// committers that arrive while an fsync is in flight must be absorbed by
// the next fsync instead of each paying their own. Durability stays
// strict — every WriteEntry that returned still has to be readable — but
// the number of fsyncs must be lower than the number of entries.
func TestWALWriter_GroupCommit(t *testing.T) {
	const writers = 8
	const entriesPerWriter = 50
	const total = writers * entriesPerWriter

	path := filepath.Join(t.TempDir(), "group_commit.wal")
	w, err := NewWALWriter(path, Options{SyncPolicy: SyncEveryWrite})
	if err != nil {
		t.Fatalf("NewWALWriter: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < entriesPerWriter; i++ {
				entry := makeTestEntry(uint64(g*entriesPerWriter + i + 1))
				err := w.WriteEntry(entry)
				ReleaseEntry(entry)
				if err != nil {
					errCh <- fmt.Errorf("writer %d entry %d: %w", g, i, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	syncs := w.SyncCount()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if syncs >= total {
		t.Fatalf("expected group commit to batch concurrent fsyncs: got %d fsyncs for %d entries", syncs, total)
	}

	// Strict durability: every acknowledged entry must be in the log.
	r, err := NewWALReader(path)
	if err != nil {
		t.Fatalf("NewWALReader: %v", err)
	}
	defer func() { _ = r.Close() }()
	count := 0
	for {
		_, err := r.ReadEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadEntry after %d entries: %v", count, err)
		}
		count++
	}
	if count != total {
		t.Fatalf("expected %d durable entries, found %d", total, count)
	}
}
