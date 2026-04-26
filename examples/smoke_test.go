package main

import (
	"os"
	"testing"
)

func TestMainSmoke(t *testing.T) {
	runInTempDir(t, main)
}

func runInTempDir(t *testing.T, fn func()) {
	t.Helper()

	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	tmpDir := t.TempDir()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("Chdir(%s): %v", tmpDir, err)
	}
	defer func() {
		if err := os.Chdir(oldWD); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	}()

	fn()
}
