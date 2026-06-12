package sql

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bobboyms/storage-engine/pkg/codec/bsoncodec"
	"github.com/bobboyms/storage-engine/pkg/crypto"
	heapv2 "github.com/bobboyms/storage-engine/pkg/heap/v2"
	"github.com/bobboyms/storage-engine/pkg/pagestore"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// RepairReport describes one repair run: the verification findings before
// and after, plus a human-readable log of every action applied.
type RepairReport struct {
	Actions []string
	Before  *storage.VerifyReport
	After   *storage.VerifyReport
}

// Repaired reports whether the run actually fixed something: the directory
// had errors before and has none now.
func (r *RepairReport) Repaired() bool {
	return r.Before.HasErrors() && !r.After.HasErrors()
}

// RepairDir is the offline repair (fsck) for a SQL database directory. It
// runs the read-only verification, applies the repairs that have an
// unambiguous source of truth, and verifies again:
//
//   - WAL entries carrying the poisoned MaxUint64 sentinel (and checkpoint
//     records with a poisoned beginLSN) are removed; recovery already
//     distrusts them, removal just makes the log clean again.
//   - Heap pages stamped with the sentinel PageLSN are healed to the log's
//     real maximum LSN.
//   - Indexes with error findings — and indexes whose backing file is
//     missing — are rebuilt from the heap, the source of truth.
//
// A missing heap is unrepairable (there is nothing to rebuild from) and is
// left untouched, reported in the after-findings. Take a backup of the
// directory before repairing; RepairDir rewrites files in place. The
// directory must be quiescent: the same exclusive lock as OpenDatabase is
// held for the whole run.
func RepairDir(ctx context.Context, dir string, opts VerifyOptions) (*RepairReport, error) {
	lock, err := acquireDirLock(dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = lock.release() }()

	before, err := verifyDirLocked(ctx, dir, opts)
	if err != nil {
		return nil, err
	}
	report := &RepairReport{Before: before, After: before}
	if !before.HasErrors() {
		return report, nil
	}

	keystore, _, err := newKeyStore(dir, opts.Encryption)
	if err != nil {
		return nil, err
	}
	walCipher, err := walCipherFor(keystore)
	if err != nil {
		return nil, err
	}
	walPath := filepath.Join(dir, "data.wal")

	if hasFindingCode(before, "wal_poisoned_lsn") || hasFindingCode(before, "wal_checkpoint_poisoned") {
		dropped, err := wal.SanitizeWAL(walPath, walCipher, isPoisonedWALEntry)
		if err != nil {
			return nil, fmt.Errorf("sql: repair: sanitize WAL: %w", err)
		}
		report.Actions = append(report.Actions, fmt.Sprintf("removed %d poisoned WAL entries", dropped))
	}

	if err := repairTables(ctx, dir, keystore, walPath, walCipher, before, report); err != nil {
		return nil, err
	}

	after, err := verifyDirLocked(ctx, dir, opts)
	if err != nil {
		return nil, err
	}
	report.After = after
	return report, nil
}

// repairTables opens every table whose heap exists and applies the
// table-level repairs the before-findings call for: healing poisoned page
// LSNs and rebuilding broken or missing indexes from the heap.
func repairTables(ctx context.Context, dir string, keystore *crypto.KeyStore, walPath string, walCipher crypto.Cipher, before *storage.VerifyReport, report *RepairReport) error {
	schemas, err := loadSchemas(dir)
	if err != nil {
		return err
	}

	healLSN, err := storage.MaxWALLSN(walPath, walCipher)
	if err != nil {
		return fmt.Errorf("sql: repair: scan WAL max LSN: %w", err)
	}

	rebuildAll, rebuildOne, healHeaps := planTableRepairs(before)

	tm := storage.NewTableMenager()
	if keystore != nil {
		indexCipher, err := keystore.GetOrCreateDEK("index")
		if err != nil {
			return fmt.Errorf("sql: repair: derive index key: %w", err)
		}
		tm.SetDefaultIndexCipher(indexCipher)
	}
	registered := map[string]TableSchema{}
	for _, s := range schemas {
		// The heap is the source of truth: without it there is nothing to
		// rebuild from, so the table is left untouched (and keeps failing
		// verification). Missing index files are recreated empty by
		// registerTable and then rebuilt below.
		if _, err := os.Stat(filepath.Join(dir, s.Name+".heap")); err != nil {
			continue
		}
		if missingIndexFile(dir, s) {
			rebuildAll[s.Name] = true
		}
		if err := registerTable(tm, dir, s, keystore); err != nil {
			return fmt.Errorf("sql: repair: open table %q: %w", s.Name, err)
		}
		registered[s.Name] = s
	}
	engine, err := storage.NewStorageEngine(tm, nil)
	if err != nil {
		return fmt.Errorf("sql: repair: open tables: %w", err)
	}
	defer func() { _ = engine.Close() }()

	codec := bsoncodec.New()
	for name, s := range registered {
		if healHeaps[name] {
			table, err := tm.GetTableByName(name)
			if err != nil {
				return err
			}
			if heapV2, ok := table.Heap.(*heapv2.HeapV2); ok {
				healed, err := heapV2.HealPoisonedPageLSNs(ctx, healLSN)
				if err != nil {
					return fmt.Errorf("sql: repair: heal heap %q: %w", name, err)
				}
				if healed > 0 {
					report.Actions = append(report.Actions, fmt.Sprintf("healed %d poisoned page LSNs in table %q", healed, name))
				}
			}
		}

		indexes := map[string]bool{}
		if rebuildAll[name] {
			for _, idx := range indicesForSchema(s) {
				indexes[idx.Name] = true
			}
		}
		for idxName := range rebuildOne[name] {
			indexes[idxName] = true
		}
		for idxName := range indexes {
			if err := storage.RebuildIndex(ctx, tm, codec, name, idxName); err != nil {
				return fmt.Errorf("sql: repair: rebuild index %s.%s: %w", name, idxName, err)
			}
			report.Actions = append(report.Actions, fmt.Sprintf("rebuilt index %s.%s from the heap", name, idxName))
		}
	}
	return nil
}

// planTableRepairs maps verification findings to table-level repair work:
// tables needing every index rebuilt, specific (table, index) rebuilds, and
// heaps to heal.
func planTableRepairs(before *storage.VerifyReport) (rebuildAll map[string]bool, rebuildOne map[string]map[string]bool, healHeaps map[string]bool) {
	rebuildAll = map[string]bool{}
	rebuildOne = map[string]map[string]bool{}
	healHeaps = map[string]bool{}
	for _, f := range before.Findings {
		if f.Severity != storage.VerifyError || f.Table == "" {
			continue
		}
		switch f.Code {
		case "index_key_order", "index_dangling_pointer", "index_key_mismatch", "index_doc_missing_field", "index_unreadable", "doc_undecodable":
			if f.Index != "" {
				if rebuildOne[f.Table] == nil {
					rebuildOne[f.Table] = map[string]bool{}
				}
				rebuildOne[f.Table][f.Index] = true
			} else {
				rebuildAll[f.Table] = true
			}
		case "index_row_count_mismatch":
			rebuildAll[f.Table] = true
		case "heap_integrity":
			healHeaps[f.Table] = true
		}
	}
	return rebuildAll, rebuildOne, healHeaps
}

// missingIndexFile reports whether any of the table's index files is absent.
func missingIndexFile(dir string, s TableSchema) bool {
	heapPath := filepath.Join(dir, s.Name+".heap")
	for _, idx := range indicesForSchema(s) {
		if _, err := os.Stat(storage.IndexFilePath(heapPath, s.Name, idx.Name)); err != nil {
			return true
		}
	}
	return false
}

// hasFindingCode reports whether the report contains a finding with code.
func hasFindingCode(report *storage.VerifyReport, code string) bool {
	for _, f := range report.Findings {
		if f.Code == code {
			return true
		}
	}
	return false
}

// isPoisonedWALEntry is the SanitizeWAL predicate for the repair flow: any
// entry whose header LSN is the MaxUint64 sentinel, or a checkpoint record
// whose beginLSN is, must leave the log.
func isPoisonedWALEntry(h wal.WALHeader, payload []byte) bool {
	if pagestore.IsPoisonedPageLSN(h.LSN) {
		return true
	}
	if h.EntryType == wal.EntryCheckpoint && len(payload) >= 8 {
		return pagestore.IsPoisonedPageLSN(binary.LittleEndian.Uint64(payload[:8]))
	}
	return false
}
