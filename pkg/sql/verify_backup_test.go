package sql

import (
	"bytes"
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func TestVerifyBackup_CleanBackupIsValid(t *testing.T) {
	ctx := context.Background()
	dir := buildVerifyDir(t)

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer func() { _ = db.Close() }()

	parent := t.TempDir()
	backupDir := filepath.Join(parent, "backup")
	if _, err := db.Backup(ctx, backupDir); err != nil {
		t.Fatalf("backup: %v", err)
	}

	report, err := VerifyBackup(ctx, backupDir, VerifyOptions{})
	if err != nil {
		t.Fatalf("VerifyBackup: %v", err)
	}
	if len(report.Findings) != 0 {
		t.Fatalf("clean backup reported findings: %v", report.Findings)
	}
	if report.LiveRows != 7 {
		t.Fatalf("report.LiveRows = %d, want 7", report.LiveRows)
	}

	// The scratch restore used for verification must not linger.
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatalf("read backup parent: %v", err)
	}
	for _, entry := range entries {
		if entry.Name() != "backup" {
			t.Fatalf("verification left scratch entry %q behind", entry.Name())
		}
	}
}

// TestBackup_RejectsCorruptSnapshot pins that Backup does not report success
// for a snapshot that fails verification. The corruption here predates the
// backup (a poisoned WAL entry from a pre-clamp binary): the copy is faithful
// — every SHA-256 in the manifest matches — but the state itself is bad, which
// only the scrub can see.
func TestBackup_RejectsCorruptSnapshot(t *testing.T) {
	ctx := context.Background()
	dir := buildVerifyDir(t)

	writer, err := wal.NewWALWriter(filepath.Join(dir, "data.wal"), wal.DefaultOptions())
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = wal.WALVersion
	entry.Header.EntryType = wal.EntryPageRedo
	entry.Header.LSN = math.MaxUint64
	// A well-formed page-redo payload (empty path, full page image) so the
	// reopen's replay can parse and skip it; only the LSN is poisoned.
	payload := make([]byte, 2+8+pagestore.PageSize)
	entry.Header.PayloadLen = uint32(len(payload)) //nolint:gosec // bounded test payload
	entry.Header.CRC32 = wal.CalculateCRC32(payload)
	entry.Payload = append(entry.Payload, payload...)
	if err := writer.WriteEntry(entry); err != nil {
		t.Fatalf("write poisoned entry: %v", err)
	}
	wal.ReleaseEntry(entry)
	if err := writer.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer func() { _ = db.Close() }()

	backupDir := filepath.Join(t.TempDir(), "backup")
	_, err = db.Backup(ctx, backupDir)
	if err == nil {
		t.Fatal("Backup of a corrupt snapshot succeeded, want a verification error")
	}
	if !strings.Contains(err.Error(), "wal_poisoned_lsn") {
		t.Fatalf("backup error does not name the finding: %v", err)
	}
}

// TestBackup_EncryptedDatabaseVerifies guards the TDE wiring: verifying the
// restored scratch copy needs the master key, which Backup must carry from
// the options the database was opened with.
func TestBackup_EncryptedDatabaseVerifies(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	key := bytes.Repeat([]byte{7}, 32)
	enc := &EncryptionOptions{MasterKey: key}

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true, Encryption: enc})
	if err != nil {
		t.Fatalf("open encrypted database: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(ctx, "CREATE TABLE users (id VARCHAR PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES ('u-1', 'n')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	backupDir := filepath.Join(t.TempDir(), "backup")
	if _, err := db.Backup(ctx, backupDir); err != nil {
		t.Fatalf("backup of encrypted database: %v", err)
	}

	report, err := VerifyBackup(ctx, backupDir, VerifyOptions{Encryption: enc})
	if err != nil {
		t.Fatalf("VerifyBackup: %v", err)
	}
	if len(report.Findings) != 0 || report.LiveRows != 1 {
		t.Fatalf("encrypted backup: findings=%v liveRows=%d, want clean with 1 row", report.Findings, report.LiveRows)
	}
}
