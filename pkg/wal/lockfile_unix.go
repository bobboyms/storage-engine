//go:build unix

package wal

import (
	"fmt"
	"os"
	"syscall"
)

// flockFile wraps syscall.Flock for an open file.
func flockFile(f *os.File, how int) error {
	return syscall.Flock(int(f.Fd()), how) //nolint:gosec // file descriptors originate as C ints in the kernel API; uintptr->int cannot overflow
}

// acquireFileLock takes an advisory exclusive lock on `path`, creating the
// file if needed. It fails immediately (LOCK_NB) when another writer — in
// this or any other process — already holds it, which is the guard against
// two engines appending to the same WAL and corrupting the log.
func acquireFileLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("wal: open lock file %s: %w", path, err)
	}
	if err := flockFile(f, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("wal: %s is locked by another writer (is another engine instance running?): %w", path, err)
	}
	return f, nil
}

// releaseFileLock drops the advisory lock. The lock file itself is left in
// place: removing it is racy (a third opener could lock the doomed inode).
func releaseFileLock(f *os.File) {
	if f == nil {
		return
	}
	_ = flockFile(f, syscall.LOCK_UN)
	_ = f.Close()
}
