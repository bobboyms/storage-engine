package sql

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/bobboyms/storage-engine/pkg/storage"
)

// sqlSidecarDirName is the subdirectory inside a backup that holds the
// SQL-layer files the storage engine does not know about (the persisted
// catalog and, for an encrypted database, the keystore). Keeping them under a
// dedicated subdirectory avoids colliding with the engine's manifest.json and
// files/ tree.
const sqlSidecarDirName = "sql"

// Backup creates a consistent, self-contained snapshot of the database into
// backupDir, which must be empty. It snapshots the engine files (table heaps,
// indexes, and the write-ahead log) while the database stays open — writes are
// briefly paused during the checkpoint/copy, reads continue — and additionally
// captures the SQL catalog (schema.json) and, when Transparent Data Encryption
// is enabled and the keystore lives in the database directory, the keystore.
// The result is a directory that RestoreDatabase can materialize into a fresh
// database directory which OpenDatabase opens directly, with no Go setup.
//
// Backup requires an executor created by OpenDatabase; one built with
// NewExecutor has no database directory and returns an error.
func (e *Executor) Backup(ctx context.Context, backupDir string) (*storage.BackupManifest, error) {
	if e.ddl == nil {
		return nil, fmt.Errorf("%w: Backup requires a database opened with OpenDatabase", ErrExec)
	}

	manifest, err := e.engine.BackupOnline(ctx, backupDir)
	if err != nil {
		return nil, err
	}

	sidecarDir := filepath.Join(backupDir, sqlSidecarDirName)
	if err := os.MkdirAll(sidecarDir, 0o700); err != nil {
		return nil, fmt.Errorf("sql: create backup sidecar dir: %w", err)
	}

	// Persist the live catalog into the backup so a restore reproduces the same
	// tables. Writing from the in-memory schema set (rather than copying the
	// file) keeps the backup catalog consistent with the executor's view.
	if err := saveSchemas(sidecarDir, e.ddl.schemas); err != nil {
		return nil, err
	}

	// For an encrypted database, the keystore holds the wrapped data-encryption
	// keys needed to read the heaps/indexes/WAL after a restore. Copy it when it
	// lives in the database directory (the default location).
	if err := e.backupKeystore(sidecarDir); err != nil {
		return nil, err
	}

	return manifest, nil
}

// backupKeystore copies the TDE keystore into sidecarDir when encryption is on
// and the keystore file lives inside the database directory. A keystore placed
// outside the database directory is treated as externally managed and skipped.
func (e *Executor) backupKeystore(sidecarDir string) error {
	if e.ddl.keystore == nil || e.ddl.keystorePath == "" {
		return nil
	}
	rel, inside := pathWithinDir(e.ddl.dir, e.ddl.keystorePath)
	if !inside {
		return nil
	}
	return copyRegularFile(e.ddl.keystorePath, filepath.Join(sidecarDir, rel))
}

// RestoreDatabase materializes a backup created by Executor.Backup into
// targetDir, which must not already contain the restored files. It verifies the
// engine snapshot (manifest, sizes, SHA-256) and copies both the engine files
// and the SQL sidecars (catalog and, if present, keystore) so the caller can
// OpenDatabase(targetDir) afterwards. For an encrypted database, reopen with the
// same EncryptionOptions (master key) used when the backup was taken.
func RestoreDatabase(ctx context.Context, backupDir, targetDir string) (*storage.BackupManifest, error) {
	manifest, err := storage.RestoreBackup(ctx, backupDir, targetDir)
	if err != nil {
		return nil, err
	}
	if err := restoreSQLSidecars(filepath.Join(backupDir, sqlSidecarDirName), targetDir); err != nil {
		return nil, err
	}
	return manifest, nil
}

// restoreSQLSidecars copies every regular file from a backup's sql/ sidecar
// directory into targetDir without overwriting existing files. A backup with no
// sidecar directory (e.g. a database that never created a table) is accepted.
func restoreSQLSidecars(sidecarDir, targetDir string) error {
	entries, err := os.ReadDir(sidecarDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("sql: read backup sidecar dir: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		dst := filepath.Join(targetDir, entry.Name())
		if _, err := os.Stat(dst); err == nil {
			return fmt.Errorf("restore: file already exists: %s", dst)
		} else if !os.IsNotExist(err) {
			return err
		}
		if err := copyRegularFile(filepath.Join(sidecarDir, entry.Name()), dst); err != nil {
			return err
		}
	}
	return nil
}

// pathWithinDir reports whether file lives inside dir and, if so, its path
// relative to dir. Both paths are resolved to absolute form first so symbolic
// differences (relative vs absolute) do not cause a false negative.
func pathWithinDir(dir, file string) (string, bool) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return "", false
	}
	absFile, err := filepath.Abs(file)
	if err != nil {
		return "", false
	}
	rel, err := filepath.Rel(absDir, absFile)
	if err != nil {
		return "", false
	}
	if rel == "." || rel == ".." || filepath.IsAbs(rel) || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", false
	}
	return rel, true
}

// copyRegularFile copies src to dst (creating parent directories), fsyncing the
// destination so the copy is durable. It fails if dst already exists.
func copyRegularFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	info, err := in.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("sql: backup sidecar %s is not a regular file", src)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(dst)
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
