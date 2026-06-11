package crypto

import (
	"os"
	"path/filepath"
	"testing"
)

// TestKeyStore_SaveIsAtomic verifies that persisting the keystore never
// rewrites the existing file in place. Losing the wrapped DEKs makes every
// byte encrypted with them unrecoverable, so save must write a temp file
// and rename it over the old one (atomic on POSIX).
//
// The hard link pins the inode of the first save: an in-place write would
// show the new content through the link, while write-temp-then-rename
// leaves the linked (old) inode untouched.
func TestKeyStore_SaveIsAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")

	ks, err := NewKeyStore(path, mustKey(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ks.GetOrCreateDEK("heap:users"); err != nil {
		t.Fatal(err)
	}

	firstSave, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	linkPath := filepath.Join(dir, "keys.json.pinned")
	if err := os.Link(path, linkPath); err != nil {
		t.Fatal(err)
	}

	// Second save must go to a fresh inode and be renamed over `path`.
	if _, err := ks.GetOrCreateDEK("wal:main"); err != nil {
		t.Fatal(err)
	}

	pinned, err := os.ReadFile(linkPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(pinned) != string(firstSave) {
		t.Fatal("save rewrote the keystore file in place; a crash mid-write could destroy every wrapped DEK")
	}

	// The real path must hold the new content and reopen cleanly.
	ks2, err := NewKeyStore(path, ks.masterKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(ks2.wrapped) != 2 {
		t.Fatalf("expected 2 wrapped DEKs after reopen, got %d", len(ks2.wrapped))
	}

	// No temp artifacts may survive a successful save.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "keys.json" && e.Name() != "keys.json.pinned" {
			t.Fatalf("unexpected leftover file after save: %s", e.Name())
		}
	}
}
