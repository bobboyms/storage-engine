//go:build !unix

package wal

import "os"

// File locking is not implemented on this platform: double-open protection
// for the WAL is unavailable and the caller is responsible for ensuring a
// single writer per path.
func acquireFileLock(string) (*os.File, error) { return nil, nil }

func releaseFileLock(*os.File) {}
