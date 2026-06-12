package sql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bobboyms/storage-engine/pkg/codec/bsoncodec"
	"github.com/bobboyms/storage-engine/pkg/crypto"
	"github.com/bobboyms/storage-engine/pkg/storage"
)

// VerifyOptions tunes VerifyDir. Encryption must carry the same master key
// the database was created with when TDE is enabled.
type VerifyOptions struct {
	Encryption *EncryptionOptions
}

// VerifyDir runs the read-only scrub over a SQL database directory: catalog
// completeness (every schema file exists, no orphan data files), write-ahead
// log invariants, heap structural integrity, and the index↔heap cross-checks.
// Nothing is repaired and no recovery is replayed — the on-disk state is
// inspected exactly as a future open would find it.
//
// The directory must be quiescent: VerifyDir takes the same exclusive
// directory lock as OpenDatabase and returns ErrDatabaseLocked if the
// database is in use. Run it against a stopped database, a backup, or a copy.
func VerifyDir(ctx context.Context, dir string, opts VerifyOptions) (*storage.VerifyReport, error) {
	lock, err := acquireDirLock(dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = lock.release() }()
	return verifyDirLocked(ctx, dir, opts)
}

// verifyDirLocked is VerifyDir without the directory lock, for callers (the
// repair flow) that already hold it across several passes.
func verifyDirLocked(ctx context.Context, dir string, opts VerifyOptions) (*storage.VerifyReport, error) {
	schemas, err := loadSchemas(dir)
	if err != nil {
		return nil, fmt.Errorf("sql: verify: load schemas: %w", err)
	}
	keystore, _, err := newKeyStore(dir, opts.Encryption)
	if err != nil {
		return nil, err
	}

	report := &storage.VerifyReport{}
	complete := catalogFindings(report, dir, schemas)

	walCipher, err := walCipherFor(keystore)
	if err != nil {
		return nil, err
	}
	report.Findings = append(report.Findings, storage.VerifyWALFile(filepath.Join(dir, "data.wal"), walCipher)...)

	tm := storage.NewTableMenager()
	if keystore != nil {
		indexCipher, err := keystore.GetOrCreateDEK("index")
		if err != nil {
			return nil, fmt.Errorf("sql: verify: derive index key: %w", err)
		}
		tm.SetDefaultIndexCipher(indexCipher)
	}
	for _, s := range complete {
		if err := registerTable(tm, dir, s, keystore); err != nil {
			report.Findings = append(report.Findings, storage.VerifyFinding{
				Severity: storage.VerifyError,
				Code:     "table_unopenable",
				Table:    s.Name,
				Detail:   err.Error(),
			})
		}
	}
	// A bare engine (no WAL, no recovery) only so Close releases every heap
	// and tree the verification opened.
	engine, err := storage.NewStorageEngine(tm, nil)
	if err != nil {
		return nil, fmt.Errorf("sql: verify: open tables: %w", err)
	}
	defer func() { _ = engine.Close() }()

	tablesReport, err := storage.VerifyTables(ctx, tm, bsoncodec.New())
	if err != nil {
		return nil, err
	}
	// Catalog and WAL findings come first, then the per-table ones; the
	// table/index/row counters come from the table scan.
	tablesReport.Findings = append(report.Findings, tablesReport.Findings...)
	return tablesReport, nil
}

// walCipherFor resolves the WAL data-encryption key when TDE is on.
func walCipherFor(keystore *crypto.KeyStore) (crypto.Cipher, error) {
	if keystore == nil {
		return nil, nil
	}
	c, err := keystore.GetOrCreateDEK("wal")
	if err != nil {
		return nil, fmt.Errorf("sql: verify: derive wal key: %w", err)
	}
	return c, nil
}

// catalogFindings cross-checks the persisted schema against the directory
// contents. It reports tables with missing data files (returning only the
// complete schemas, so the verifier never implicitly creates the missing
// file by opening it) and data files no schema references.
func catalogFindings(report *storage.VerifyReport, dir string, schemas []TableSchema) []TableSchema {
	referenced := map[string]bool{}
	complete := make([]TableSchema, 0, len(schemas))

	for _, s := range schemas {
		heapPath := filepath.Join(dir, s.Name+".heap")
		expected := []string{heapPath}
		for _, idx := range indicesForSchema(s) {
			expected = append(expected, storage.IndexFilePath(heapPath, s.Name, idx.Name))
		}

		missing := false
		for _, path := range expected {
			referenced[filepath.Base(path)] = true
			if _, err := os.Stat(path); err != nil {
				missing = true
				report.Findings = append(report.Findings, storage.VerifyFinding{
					Severity: storage.VerifyError,
					Code:     "catalog_missing_file",
					Table:    s.Name,
					Detail:   fmt.Sprintf("schema references %s but it is absent (%v)", filepath.Base(path), err),
				})
			}
		}
		if !missing {
			complete = append(complete, s)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		report.Findings = append(report.Findings, storage.VerifyFinding{
			Severity: storage.VerifyError,
			Code:     "catalog_unreadable",
			Detail:   err.Error(),
		})
		return complete
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || referenced[name] {
			continue
		}
		if strings.HasSuffix(name, ".heap") || strings.HasSuffix(name, ".btree.v2") {
			report.Findings = append(report.Findings, storage.VerifyFinding{
				Severity: storage.VerifyWarning,
				Code:     "catalog_orphan_file",
				Detail:   fmt.Sprintf("%s is not referenced by any schema", name),
			})
		}
	}
	return complete
}
