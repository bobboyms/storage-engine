package wal

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func lifecycleEntry(lsn uint64, payload []byte) *WALEntry {
	entry := AcquireEntry()
	entry.Header.Magic = WALMagic
	entry.Header.Version = WALVersion
	entry.Header.EntryType = EntryInsert
	entry.Header.LSN = lsn
	entry.Header.PayloadLen = uint32(len(payload))
	entry.Header.CRC32 = CalculateCRC32(payload)
	entry.Payload = append(entry.Payload[:0], payload...)
	return entry
}

func readLifecycleLSNs(t *testing.T, path string) []uint64 {
	t.Helper()
	reader, err := NewWALReader(path)
	if err != nil {
		t.Fatalf("NewWALReader: %v", err)
	}
	defer reader.Close()

	var lsns []uint64
	for {
		entry, err := reader.ReadEntry()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadEntry: %v", err)
		}
		lsns = append(lsns, entry.Header.LSN)
		ReleaseEntry(entry)
	}
	return lsns
}

func TestWALLifecycle_RotationAndSegmentReader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	opts := DefaultOptions()
	opts.MaxSegmentBytes = 1
	opts.RetentionSegments = 0
	writer, err := NewWALWriter(path, opts)
	if err != nil {
		t.Fatalf("NewWALWriter: %v", err)
	}
	for i := uint64(1); i <= 5; i++ {
		entry := lifecycleEntry(i, []byte("payload"))
		if err := writer.WriteEntry(entry); err != nil {
			t.Fatalf("WriteEntry %d: %v", i, err)
		}
		ReleaseEntry(entry)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	paths, err := SegmentPaths(path)
	if err != nil {
		t.Fatalf("SegmentPaths: %v", err)
	}
	if len(paths) < 2 {
		t.Fatalf("expected rotated segments plus active WAL, got %v", paths)
	}

	got := readLifecycleLSNs(t, path)
	if len(got) != 5 {
		t.Fatalf("expected 5 entries across segments, got %v", got)
	}
	for i, lsn := range got {
		if lsn != uint64(i+1) {
			t.Fatalf("entry %d: expected LSN %d, got %d", i, i+1, lsn)
		}
	}
}

func TestWALLifecycle_ArchiveTruncateAndRestore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	archiveDir := filepath.Join(dir, "archive")

	opts := DefaultOptions()
	opts.MaxSegmentBytes = 1
	opts.RetentionSegments = 0
	writer, err := NewWALWriter(path, opts)
	if err != nil {
		t.Fatalf("NewWALWriter: %v", err)
	}
	for i := uint64(1); i <= 5; i++ {
		entry := lifecycleEntry(i, []byte("payload"))
		if err := writer.WriteEntry(entry); err != nil {
			t.Fatalf("WriteEntry %d: %v", i, err)
		}
		ReleaseEntry(entry)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := ArchiveAndTruncate(path, nil, archiveDir, 4, 0); err != nil {
		t.Fatalf("ArchiveAndTruncate: %v", err)
	}

	archiveEntries, err := os.ReadDir(archiveDir)
	if err != nil {
		t.Fatalf("ReadDir archive: %v", err)
	}
	if len(archiveEntries) == 0 {
		t.Fatal("expected archived segments")
	}

	gotAfterTruncate := readLifecycleLSNs(t, path)
	if len(gotAfterTruncate) != 2 || gotAfterTruncate[0] != 4 || gotAfterTruncate[1] != 5 {
		t.Fatalf("expected only retained WAL from checkpoint onward, got %v", gotAfterTruncate)
	}

	if err := RestoreArchivedSegments(path, archiveDir); err != nil {
		t.Fatalf("RestoreArchivedSegments: %v", err)
	}
	gotAfterRestore := readLifecycleLSNs(t, path)
	if len(gotAfterRestore) != 5 {
		t.Fatalf("expected restored full WAL, got %v", gotAfterRestore)
	}
}
