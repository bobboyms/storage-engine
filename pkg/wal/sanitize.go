package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/bobboyms/storage-engine/pkg/crypto"
)

// SanitizeWAL rewrites every local segment of the log rooted at base
// (archived segments and the active file), keeping only the entries for
// which drop returns false. Surviving entries are preserved verbatim
// (header fields and payload). It returns how many entries were removed.
//
// This is the mechanical half of the offline repair (fsck): the caller
// decides what is corrupt via the predicate. The log must be quiescent —
// SanitizeWAL takes the writer's exclusive lock for the whole operation and
// fails if another writer holds it. Each segment is rewritten into a sibling
// temp file and atomically renamed into place, so a crash mid-repair leaves
// either the old or the new segment, never a half-written one.
func SanitizeWAL(base string, cipher crypto.Cipher, drop func(WALHeader, []byte) bool) (int, error) {
	lock, err := acquireFileLock(base + ".lock")
	if err != nil {
		return 0, err
	}
	defer releaseFileLock(lock)

	paths, err := SegmentPaths(base)
	if err != nil {
		return 0, err
	}

	dropped := 0
	for _, path := range paths {
		n, err := sanitizeSegment(path, cipher, drop)
		if err != nil {
			return dropped, fmt.Errorf("wal: sanitize %s: %w", path, err)
		}
		dropped += n
	}
	if dropped > 0 {
		if err := fsyncDir(filepath.Dir(base)); err != nil {
			return dropped, err
		}
	}
	return dropped, nil
}

// sanitizeSegment rewrites one segment file without the dropped entries. A
// segment with nothing to drop is left untouched.
func sanitizeSegment(path string, cipher crypto.Cipher, drop func(WALHeader, []byte) bool) (int, error) {
	reader, err := newSinglePathReader(path, cipher)
	if err != nil {
		return 0, err
	}

	type keptEntry struct {
		header  WALHeader
		payload []byte
	}
	var kept []keptEntry
	dropped := 0
	for {
		entry, err := reader.ReadEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				break // torn tail: rewrite up to the last complete entry.
			}
			_ = reader.Close()
			return 0, err
		}
		if drop(entry.Header, entry.Payload) {
			dropped++
		} else {
			kept = append(kept, keptEntry{
				header:  entry.Header,
				payload: append([]byte(nil), entry.Payload...),
			})
		}
		ReleaseEntry(entry)
	}
	if err := reader.Close(); err != nil {
		return 0, err
	}
	if dropped == 0 {
		return 0, nil
	}

	tmp := path + ".sanitize.tmp"
	opts := Options{Cipher: cipher} // no rotation, no background sync
	writer, err := NewWALWriter(tmp, opts)
	if err != nil {
		return 0, err
	}
	for _, e := range kept {
		entry := AcquireEntry()
		entry.Header = e.header
		entry.Payload = append(entry.Payload, e.payload...)
		err := writer.WriteEntry(entry)
		ReleaseEntry(entry)
		if err != nil {
			_ = writer.Close()
			_ = os.Remove(tmp)
			return 0, err
		}
	}
	if err := writer.Close(); err != nil {
		_ = os.Remove(tmp)
		return 0, err
	}
	// The temp writer's lock sidecar is ours alone; remove it before the
	// rename so no stray file is left next to the repaired segment.
	_ = os.Remove(tmp + ".lock")

	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return 0, err
	}
	return dropped, nil
}
