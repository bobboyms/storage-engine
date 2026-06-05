package storage

import (
	"context"
	stderrors "errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBackupManifestRecordsDataFormatVersion(t *testing.T) {
	db := newBackupTestDB(t, filepath.Join(t.TempDir(), "db"))
	defer db.engine.Close()
	putAccount(t, db.engine, 1, "a@x.com")

	backupDir := filepath.Join(t.TempDir(), "backup")
	manifest, err := db.engine.BackupOnline(context.Background(), backupDir)
	if err != nil {
		t.Fatalf("BackupOnline: %v", err)
	}
	if manifest.DataFormatVersion != DataFormatVersion {
		t.Fatalf("manifest data format = %d, want %d", manifest.DataFormatVersion, DataFormatVersion)
	}
}

func TestVerifyBackupRejectsNewerDataFormat(t *testing.T) {
	db := newBackupTestDB(t, filepath.Join(t.TempDir(), "db"))
	defer db.engine.Close()
	putAccount(t, db.engine, 1, "a@x.com")

	backupDir := filepath.Join(t.TempDir(), "backup")
	if _, err := db.engine.BackupOnline(context.Background(), backupDir); err != nil {
		t.Fatalf("BackupOnline: %v", err)
	}

	// Tamper the manifest to claim a future data format version.
	manifestPath := filepath.Join(backupDir, backupManifestName)
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	bumped := strings.Replace(string(data),
		`"data_format_version": 1`, `"data_format_version": 999`, 1)
	if bumped == string(data) {
		t.Fatalf("manifest did not contain the expected data_format_version field: %s", data)
	}
	if err := os.WriteFile(manifestPath, []byte(bumped), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	_, err = VerifyBackup(context.Background(), backupDir)
	if !stderrors.Is(err, ErrUnsupportedDataFormat) {
		t.Fatalf("expected ErrUnsupportedDataFormat, got %v", err)
	}
}
