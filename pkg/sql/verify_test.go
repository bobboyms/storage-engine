package sql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// buildVerifyDir creates a small database (two tables, a secondary index,
// rows, an update, and a delete) and closes it, returning the directory.
func buildVerifyDir(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	stmts := []string{
		"CREATE TABLE users (id VARCHAR PRIMARY KEY, name VARCHAR)",
		"CREATE TABLE sessions (id VARCHAR PRIMARY KEY, name VARCHAR)",
		"CREATE INDEX ON users (name)",
	}
	for _, s := range stmts {
		if _, err := db.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	for i := 0; i < 4; i++ {
		for _, table := range []string{"users", "sessions"} {
			q := fmt.Sprintf("INSERT INTO %s (id, name) VALUES ('%s-%d', 'n-%d')", table, table, i, i)
			if _, err := db.Exec(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
	}
	if _, err := db.Exec(ctx, "UPDATE users SET name = 'renamed' WHERE id = 'users-1'"); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := db.Exec(ctx, "DELETE FROM sessions WHERE id = 'sessions-0'"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return dir
}

func verifyFindingWithCode(findings []storage.VerifyFinding, code string) (storage.VerifyFinding, bool) {
	for _, f := range findings {
		if f.Code == code {
			return f, true
		}
	}
	return storage.VerifyFinding{}, false
}

func TestVerifyDir_HealthyDatabaseIsClean(t *testing.T) {
	dir := buildVerifyDir(t)
	report, err := VerifyDir(context.Background(), dir, VerifyOptions{})
	if err != nil {
		t.Fatalf("VerifyDir: %v", err)
	}
	if len(report.Findings) != 0 {
		t.Fatalf("healthy database reported findings: %v", report.Findings)
	}
	if report.Tables != 2 {
		t.Fatalf("report.Tables = %d, want 2", report.Tables)
	}
	if report.LiveRows != 7 {
		t.Fatalf("report.LiveRows = %d, want 7 (8 inserted, 1 deleted)", report.LiveRows)
	}
}

func TestVerifyDir_FlagsPoisonedWAL(t *testing.T) {
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
	payload := make([]byte, 2+8+pagestore.PageSize)
	entry.Header.PayloadLen = uint32(len(payload)) //nolint:gosec // small test payload
	entry.Header.CRC32 = wal.CalculateCRC32(payload)
	entry.Payload = append(entry.Payload, payload...)
	if err := writer.WriteEntry(entry); err != nil {
		t.Fatalf("write poisoned entry: %v", err)
	}
	wal.ReleaseEntry(entry)
	if err := writer.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	report, err := VerifyDir(context.Background(), dir, VerifyOptions{})
	if err != nil {
		t.Fatalf("VerifyDir: %v", err)
	}
	if _, ok := verifyFindingWithCode(report.Findings, "wal_poisoned_lsn"); !ok {
		t.Fatalf("missing wal_poisoned_lsn finding, got %v", report.Findings)
	}
	if !report.HasErrors() {
		t.Fatal("report.HasErrors() = false for a poisoned WAL")
	}
}

func TestVerifyDir_FlagsMissingIndexFile(t *testing.T) {
	dir := buildVerifyDir(t)
	victim := storage.IndexFilePath(filepath.Join(dir, "users.heap"), "users", "name")
	if err := os.Remove(victim); err != nil {
		t.Fatalf("remove index file: %v", err)
	}

	report, err := VerifyDir(context.Background(), dir, VerifyOptions{})
	if err != nil {
		t.Fatalf("VerifyDir: %v", err)
	}
	f, ok := verifyFindingWithCode(report.Findings, "catalog_missing_file")
	if !ok {
		t.Fatalf("missing catalog_missing_file finding, got %v", report.Findings)
	}
	if f.Severity != storage.VerifyError || f.Table != "users" {
		t.Fatalf("catalog_missing_file = %+v, want error severity on table users", f)
	}
	// The missing file must not have been silently (re)created by the check.
	if _, err := os.Stat(victim); !os.IsNotExist(err) {
		t.Fatalf("verifier recreated the missing index file (stat err=%v)", err)
	}
}

func TestVerifyDir_FlagsOrphanFiles(t *testing.T) {
	dir := buildVerifyDir(t)
	orphan := filepath.Join(dir, "ghost.heap")
	if err := os.WriteFile(orphan, []byte("x"), 0o600); err != nil {
		t.Fatalf("write orphan: %v", err)
	}

	report, err := VerifyDir(context.Background(), dir, VerifyOptions{})
	if err != nil {
		t.Fatalf("VerifyDir: %v", err)
	}
	f, ok := verifyFindingWithCode(report.Findings, "catalog_orphan_file")
	if !ok {
		t.Fatalf("missing catalog_orphan_file finding, got %v", report.Findings)
	}
	if f.Severity != storage.VerifyWarning {
		t.Fatalf("catalog_orphan_file severity = %s, want warning", f.Severity)
	}
}

func TestVerifyDir_EncryptedDatabase(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	key := bytes.Repeat([]byte{7}, 32)

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{
		DisableMaintenance: true,
		Encryption:         &EncryptionOptions{MasterKey: key},
	})
	if err != nil {
		t.Fatalf("open encrypted database: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE users (id VARCHAR PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES ('u-1', 'n')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	report, err := VerifyDir(ctx, dir, VerifyOptions{Encryption: &EncryptionOptions{MasterKey: key}})
	if err != nil {
		t.Fatalf("VerifyDir with the right key: %v", err)
	}
	if len(report.Findings) != 0 || report.LiveRows != 1 {
		t.Fatalf("encrypted database: findings=%v liveRows=%d, want clean with 1 row", report.Findings, report.LiveRows)
	}

	wrong := bytes.Repeat([]byte{9}, 32)
	if _, err := VerifyDir(ctx, dir, VerifyOptions{Encryption: &EncryptionOptions{MasterKey: wrong}}); err == nil {
		t.Fatal("VerifyDir with the wrong master key succeeded, want an error")
	}
}

func TestVerifyDir_RefusesLiveDatabase(t *testing.T) {
	dir := buildVerifyDir(t)
	db, err := OpenDatabaseWithOptions(context.Background(), dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := VerifyDir(context.Background(), dir, VerifyOptions{}); !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("VerifyDir on a live database returned %v, want ErrDatabaseLocked", err)
	}
}
