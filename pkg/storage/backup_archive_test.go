package storage

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestArchiveAndExtractRoundTrip(t *testing.T) {
	ctx := context.Background()
	src := filepath.Join(t.TempDir(), "db")
	db := newBackupTestDB(t, src)
	defer db.engine.Close()
	for i := int64(1); i <= 5; i++ {
		putAccount(t, db.engine, i, fmt.Sprintf("user-%d@example.com", i))
	}

	backupDir := filepath.Join(t.TempDir(), "backup")
	if _, err := db.engine.BackupOnline(ctx, backupDir); err != nil {
		t.Fatalf("BackupOnline: %v", err)
	}

	// Pack the whole backup (manifest + files) into one .tar.gz.
	archive := filepath.Join(t.TempDir(), "backup.tar.gz")
	if err := ArchiveBackup(backupDir, archive); err != nil {
		t.Fatalf("ArchiveBackup: %v", err)
	}
	info, err := os.Stat(archive)
	if err != nil || info.IsDir() {
		t.Fatalf("archive should be a single file: err=%v", err)
	}

	// Extract to a fresh dir and verify + restore from it.
	extracted := filepath.Join(t.TempDir(), "extracted")
	if err := ExtractBackup(archive, extracted); err != nil {
		t.Fatalf("ExtractBackup: %v", err)
	}
	if _, err := VerifyBackup(ctx, extracted); err != nil {
		t.Fatalf("VerifyBackup(extracted): %v", err)
	}

	restoreDir := filepath.Join(t.TempDir(), "restore")
	if _, err := RestoreBackup(ctx, extracted, restoreDir); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	restored := reopenBackupTestDB(t, restoreDir)
	defer restored.Close()
	for i := int64(1); i <= 5; i++ {
		got, ok, err := getDocString(t, restored, "accounts", "id", types.IntKey(i))
		if err != nil || !ok || !strings.Contains(got, fmt.Sprintf("user-%d@example.com", i)) {
			t.Fatalf("record %d missing after archive round trip: ok=%v got=%s err=%v", i, ok, got, err)
		}
	}
}

func TestExtractBackupRejectsPathTraversal(t *testing.T) {
	// Craft a malicious archive with an entry that escapes the destination.
	archive := filepath.Join(t.TempDir(), "evil.tar.gz")
	f, err := os.Create(archive)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	payload := []byte("pwned")
	if err := tw.WriteHeader(&tar.Header{
		Name: "../escape.txt", Mode: 0o600, Size: int64(len(payload)), Typeflag: tar.TypeReg,
	}); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if _, err := tw.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = tw.Close()
	_ = gz.Close()
	_ = f.Close()

	dest := filepath.Join(t.TempDir(), "out")
	if err := ExtractBackup(archive, dest); err == nil {
		t.Fatal("expected ExtractBackup to reject a path-traversal entry")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dest), "escape.txt")); err == nil {
		t.Fatal("path traversal wrote a file outside the destination")
	}
}
