package wal

import (
	"path/filepath"
	"testing"
	"time"
)

func makeTestEntry(lsn uint64) *WALEntry {
	payload := []byte("payload")
	entry := AcquireEntry()
	entry.Header = WALHeader{
		Magic:      WALMagic,
		Version:    1,
		EntryType:  EntryInsert,
		PayloadLen: uint32(len(payload)), //nolint:gosec // small fixed test payload
		CRC32:      CalculateCRC32(payload),
		LSN:        lsn,
	}
	entry.Payload = append(entry.Payload, payload...)
	return entry
}

// TestWALWriter_BackgroundSyncFailurePoisonsWriter covers the SyncInterval
// policy: when the background fsync fails (dying disk, EIO), the writer must
// stop accepting writes instead of silently buffering entries that will never
// become durable. Acknowledging writes after a failed fsync is how
// acknowledged commits get lost.
func TestWALWriter_BackgroundSyncFailurePoisonsWriter(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test_wal_bgsync_fail.log")

	w, err := NewWALWriter(tmpFile, Options{
		SyncPolicy:           SyncInterval,
		SyncIntervalDuration: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer func() { _ = w.Close() }()

	entry := makeTestEntry(1)
	if err := w.WriteEntry(entry); err != nil {
		t.Fatalf("WriteEntry before fault: %v", err)
	}
	ReleaseEntry(entry)

	// Simulate a dead disk: every page write/fsync from now on fails.
	w.mu.Lock()
	_ = w.pf.Close()
	w.mu.Unlock()

	// The background ticker must observe the failure and poison the writer:
	// subsequent WriteEntry calls have to fail instead of buffering in memory.
	deadline := time.Now().Add(2 * time.Second)
	for lsn := uint64(2); ; lsn++ {
		entry := makeTestEntry(lsn)
		err := w.WriteEntry(entry)
		ReleaseEntry(entry)
		if err != nil {
			return // poisoned as expected
		}
		if time.Now().After(deadline) {
			t.Fatal("WriteEntry kept accepting writes after background fsync failure; buffered entries would be silently lost")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
