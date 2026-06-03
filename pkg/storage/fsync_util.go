package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// durableWriteFile writes `data` to `path` with strong durability
// guarantees. It replaces `os.WriteFile`, which:
//   - does NOT fsync the file (content stays in the OS page cache)
//   - does NOT fsync the directory (the inode table entry can vanish after a crash)
//
// Durability contract after a successful return:
//   - The file content is on disk (file fsync)
//   - The file's directory entry is on disk (parent dir fsync)
//   - The old file, if it existed at the same path, was replaced atomically
//
// Pattern: write temp → fsync temp → rename → fsync dir.
func durableWriteFile(path string, data []byte) error {
	tmpPath := path + ".tmp"

	// 1. Write to the temporary file
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("durableWriteFile: open temp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("durableWriteFile: write: %w", err)
	}

	// 2. fsync the temp file — guarantees the bytes are on disk
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("durableWriteFile: fsync temp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("durableWriteFile: close temp: %w", err)
	}

	// 3. Atomic rename
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("durableWriteFile: rename: %w", err)
	}

	// 4. fsync the directory — guarantees the name entry is on disk
	// Without this, after a crash the rename can "vanish" (filesystem did not persist the dir).
	return fsyncDir(filepath.Dir(path))
}

// fsyncDir opens the directory and fsyncs it. Critical on POSIX to guarantee
// that dir-level operations (create, rename) survive a crash.
// On Windows, opening a directory does not work as expected — we return
// nil by convention (Windows has different durability behavior).
func fsyncDir(dirPath string) error {
	d, err := os.Open(dirPath)
	if err != nil {
		// On some FSes/OSes the directory cannot be opened for write;
		// we use a read-only Sync only. If it fails, propagate the error.
		return fmt.Errorf("fsyncDir: open %s: %w", dirPath, err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsyncDir: sync %s: %w", dirPath, err)
	}
	return nil
}
