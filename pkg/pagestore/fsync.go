package pagestore

import (
	"fmt"
	"os"
)

var syncFile = func(f *os.File) error {
	return f.Sync()
}

// fsyncDir opens the directory and calls Sync, which is the POSIX
// guarantee that creates and renames inside it persist across a crash.
//
// On Windows the behavior is different; opening a directory for reading
// usually does not work like POSIX. The function propagates the error and
// lets the caller decide; in our case, NewPageFile treats it as fatal.
func fsyncDir(dirPath string) error {
	d, err := os.Open(dirPath)
	if err != nil {
		return fmt.Errorf("pagestore: open dir %s: %w", dirPath, err)
	}
	defer d.Close()
	if err := syncFile(d); err != nil {
		return fmt.Errorf("pagestore: fsync dir %s: %w", dirPath, err)
	}
	return nil
}
