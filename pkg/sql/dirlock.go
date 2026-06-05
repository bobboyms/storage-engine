package sql

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// ErrDatabaseLocked is returned by OpenDatabaseWithOptions when the directory is
// already held open by another live database handle (in this or another
// process). Opening the same directory twice would let two engines write to the
// same heap and B-tree files concurrently and silently corrupt the indexes.
var ErrDatabaseLocked = errors.New("sql: database directory is already in use by another process")

// lockFileName is the advisory lock file created inside a database directory. It
// carries no payload; only the OS-level flock on its descriptor matters.
const lockFileName = "LOCK"

// dirLock is an exclusive, advisory, whole-directory lock backed by flock(2) on
// a sentinel file. The kernel releases the lock automatically when the
// descriptor is closed or the owning process exits, so a crashed process never
// leaves a stale lock behind.
type dirLock struct {
	f *os.File
}

// acquireDirLock takes an exclusive, non-blocking lock on dir. It returns
// ErrDatabaseLocked when another live handle already holds the lock.
func acquireDirLock(dir string) (*dirLock, error) {
	path := filepath.Join(dir, lockFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("sql: open lock file: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil { //nolint:gosec // os.File.Fd() returns a small kernel descriptor that always fits in int
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", ErrDatabaseLocked, dir)
		}
		return nil, fmt.Errorf("sql: lock database dir: %w", err)
	}
	return &dirLock{f: f}, nil
}

// release drops the lock and closes the descriptor. It is safe to call on a nil
// lock.
func (l *dirLock) release() error {
	if l == nil || l.f == nil {
		return nil
	}
	// Closing the descriptor releases the flock; do it explicitly first so the
	// lock is dropped even if Close is delayed.
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN) //nolint:gosec // os.File.Fd() returns a small kernel descriptor that always fits in int
	err := l.f.Close()
	l.f = nil
	return err
}
