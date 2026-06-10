package sql

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

// assertPlaintextOnDisk fails unless some regular file under dir contains the
// given plaintext. It is the inverse of assertNoPlaintextOnDisk and proves that
// a non-encrypted backup keeps row data in the clear (so the encrypted-at-rest
// test below is meaningful: the same marker is found here and absent there).
func assertPlaintextOnDisk(t *testing.T, dir string, plaintext []byte) {
	t.Helper()
	found := false
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(raw, plaintext) {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	if !found {
		t.Errorf("expected plaintext %q somewhere under %s, but it was not found", plaintext, dir)
	}
}

// TestExecutor_BackupRestore_NoEncryptionAtRest covers backup and restore for a
// database opened WITHOUT Transparent Data Encryption. The round-trip must
// reproduce exactly the rows present at backup time (no schema setup, and no
// write that landed after the snapshot). As a contrast with the encrypted case,
// it also asserts the row data is stored in the clear inside the backup.
func TestExecutor_BackupRestore_NoEncryptionAtRest(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	backupDir := filepath.Join(t.TempDir(), "backup")
	restoreDir := filepath.Join(t.TempDir(), "restored")

	marker := []byte("canary_plain_value_42")

	db, err := OpenDatabase(ctx, srcDir)
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'canary_plain_value_42')"); err != nil {
		t.Fatalf("INSERT 1: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (2, 'bob')"); err != nil {
		t.Fatalf("INSERT 2: %v", err)
	}

	manifest, err := db.Backup(ctx, backupDir)
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}
	if manifest == nil || len(manifest.Files) == 0 {
		t.Fatalf("Backup returned empty manifest")
	}

	// Without TDE the backed-up heap holds the row value in the clear.
	assertPlaintextOnDisk(t, backupDir, marker)

	// A write AFTER the backup must not appear in the restore.
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (99, 'carol')"); err != nil {
		t.Fatalf("post-backup INSERT: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close source: %v", err)
	}

	if _, err := RestoreDatabase(ctx, backupDir, restoreDir); err != nil {
		t.Fatalf("RestoreDatabase: %v", err)
	}

	restored, err := OpenDatabase(ctx, restoreDir)
	if err != nil {
		t.Fatalf("OpenDatabase(restored): %v", err)
	}
	defer restored.Close()

	rs, err := restored.Query(ctx, "SELECT id FROM users")
	if err != nil {
		t.Fatalf("Query restored: %v", err)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("restored rows = %d, want 2 (the rows present at backup time)", len(rs.Rows))
	}
	rs99, err := restored.Query(ctx, "SELECT id FROM users WHERE id = 99")
	if err != nil {
		t.Fatalf("Query id=99: %v", err)
	}
	if len(rs99.Rows) != 0 {
		t.Fatalf("restore must not contain the post-backup row, found %d", len(rs99.Rows))
	}
}

// TestExecutor_BackupRestore_EncryptionAtRest covers backup and restore for a
// database opened WITH Transparent Data Encryption. Besides the round-trip, it
// asserts the encryption-at-rest guarantee survives the backup: the row value
// appears nowhere in the backup files nor in the restored directory, and the
// restore is only readable with the original master key.
func TestExecutor_BackupRestore_EncryptionAtRest(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	backupDir := filepath.Join(t.TempDir(), "backup")
	restoreDir := filepath.Join(t.TempDir(), "restored")

	secret := []byte("secret_value_at_rest_42")
	enc := OpenOptions{Encryption: &EncryptionOptions{MasterKey: testMasterKey}}

	db, err := OpenDatabaseWithOptions(ctx, srcDir, enc)
	if err != nil {
		t.Fatalf("OpenDatabaseWithOptions: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'secret_value_at_rest_42')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	if _, err := db.Backup(ctx, backupDir); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	// Encryption at rest must hold inside the backup: the plaintext value never
	// touches disk there (only the catalog and wrapped keys do).
	assertNoPlaintextOnDisk(t, backupDir, secret)

	if err := db.Close(); err != nil {
		t.Fatalf("Close source: %v", err)
	}

	if _, err := RestoreDatabase(ctx, backupDir, restoreDir); err != nil {
		t.Fatalf("RestoreDatabase: %v", err)
	}
	// ...and it still holds in the restored directory.
	assertNoPlaintextOnDisk(t, restoreDir, secret)

	// Wrong master key cannot open the restored database (keystore validation
	// fails), proving the data is genuinely protected at rest.
	wrongKey := []byte("ffffffffffffffffffffffffffffffff")
	if _, err := OpenDatabaseWithOptions(ctx, restoreDir, OpenOptions{
		Encryption: &EncryptionOptions{MasterKey: wrongKey},
	}); err == nil {
		t.Fatal("opening the restored encrypted database with a wrong master key must fail")
	}

	// The correct master key reads the row back intact.
	restored, err := OpenDatabaseWithOptions(ctx, restoreDir, enc)
	if err != nil {
		t.Fatalf("OpenDatabaseWithOptions(restored): %v", err)
	}
	defer restored.Close()

	rs, err := restored.Query(ctx, "SELECT name FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query restored: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("restored rows = %d, want 1", len(rs.Rows))
	}
}

// TestExecutor_Backup_RequiresOpenDatabase proves that Backup is only valid for
// an executor created via OpenDatabase (it needs the database directory and
// catalog). An executor built with NewExecutor has no directory and must error.
func TestExecutor_Backup_RequiresOpenDatabase(t *testing.T) {
	e := NewExecutor(nil, NewCatalog(), nil)
	if _, err := e.Backup(context.Background(), t.TempDir()); err == nil {
		t.Fatal("Backup on a NewExecutor (no database directory) must fail")
	}
}
