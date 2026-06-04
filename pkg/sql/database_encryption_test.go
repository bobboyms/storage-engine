package sql

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

// testMasterKey is a fixed 32-byte key used only by the encryption tests.
var testMasterKey = []byte("0123456789abcdef0123456789abcdef")

// assertNoPlaintextOnDisk fails if any regular file under dir contains the
// given plaintext, proving the data is encrypted at rest.
func assertNoPlaintextOnDisk(t *testing.T, dir string, plaintext []byte) {
	t.Helper()
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
			t.Errorf("file %s contains plaintext %q", path, plaintext)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
}

func TestOpenDatabaseEncryptionAtRest(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	secret := "super-secret-card-4111-2222-3333"
	email := "tde-secret@example.com"

	opts := OpenOptions{
		DisableMaintenance: true,
		Encryption:         &EncryptionOptions{MasterKey: testMasterKey},
	}

	db, err := OpenDatabaseWithOptions(ctx, dir, opts)
	if err != nil {
		t.Fatalf("OpenDatabaseWithOptions: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE accounts (email VARCHAR PRIMARY KEY, note VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO accounts (email, note) VALUES ('"+email+"', '"+secret+"')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Heap, WAL, and the VARCHAR primary index must not leak plaintext.
	assertNoPlaintextOnDisk(t, dir, []byte(secret))
	assertNoPlaintextOnDisk(t, dir, []byte(email))

	// Reopen with the same master key: data round-trips.
	db2, err := OpenDatabaseWithOptions(ctx, dir, opts)
	if err != nil {
		t.Fatalf("reopen with correct key: %v", err)
	}
	rs, err := db2.Query(ctx, "SELECT note FROM accounts WHERE email = '"+email+"'")
	if err != nil {
		t.Fatalf("Query after reopen: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows after reopen = %d, want 1", len(rs.Rows))
	}
	if got := strColumn(t, rs, "note"); got[0] != secret {
		t.Fatalf("note = %q, want %q", got[0], secret)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("Close after reopen: %v", err)
	}
}

func TestOpenDatabaseEncryptionWrongKeyFails(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	opts := OpenOptions{
		DisableMaintenance: true,
		Encryption:         &EncryptionOptions{MasterKey: testMasterKey},
	}
	db, err := OpenDatabaseWithOptions(ctx, dir, opts)
	if err != nil {
		t.Fatalf("OpenDatabaseWithOptions: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	wrongKey := []byte("ffffffffffffffffffffffffffffffff")
	wrongOpts := OpenOptions{
		DisableMaintenance: true,
		Encryption:         &EncryptionOptions{MasterKey: wrongKey},
	}
	if _, err := OpenDatabaseWithOptions(ctx, dir, wrongOpts); err == nil {
		t.Fatal("expected error reopening with the wrong master key")
	}
}

func TestOpenDatabaseEncryptionRejectsBadKey(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	opts := OpenOptions{
		DisableMaintenance: true,
		Encryption:         &EncryptionOptions{MasterKey: []byte("too-short")},
	}
	if _, err := OpenDatabaseWithOptions(ctx, dir, opts); err == nil {
		t.Fatal("expected error opening with an invalid master key length")
	}
}
