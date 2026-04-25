//go:build faults

package pagestore

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const fsyncFaultMarker = ".fail_fsync_now"

func init() {
	realSyncFile := syncFile
	syncFile = func(f *os.File) error {
		if shouldInjectFsyncFailure(f.Name()) {
			return syscall.EIO
		}
		return realSyncFile(f)
	}
}

func shouldInjectFsyncFailure(path string) bool {
	root := os.Getenv("STORAGE_ENGINE_FSYNC_FAIL_DIR")
	if root == "" {
		return false
	}

	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	pathAbs, err := filepath.Abs(path)
	if err != nil {
		return false
	}

	rel, err := filepath.Rel(rootAbs, pathAbs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false
	}

	_, err = os.Stat(filepath.Join(rootAbs, fsyncFaultMarker))
	return err == nil
}
