// Verifier ("scrub"): read-only integrity checks over a database's on-disk
// state. The verifier never repairs anything — it reports violations of
// invariants that hold in every healthy directory, so corruption persisted by
// an old bug, a crash, or bad hardware surfaces as a precise finding instead
// of a mystery at query time.
//
// The directory must be quiescent (no live engine writing to it): run it
// against a stopped database, a backup, or a copy.
package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	btreev2 "github.com/bobboyms/storage-engine/pkg/btree/v2"
	"github.com/bobboyms/storage-engine/pkg/codec"
	"github.com/bobboyms/storage-engine/pkg/codec/bsoncodec"
	"github.com/bobboyms/storage-engine/pkg/crypto"
	heapv2 "github.com/bobboyms/storage-engine/pkg/heap/v2"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// verifyChainStepLimit bounds version-chain walks so a corrupted chain that
// loops (or is absurdly long) is reported instead of hanging the verifier.
const verifyChainStepLimit = 100_000

// VerifySeverity classifies a finding: errors are invariant violations,
// warnings are conditions worth surfacing that have a legitimate explanation
// (e.g. a torn WAL tail after a crash).
type VerifySeverity string

const (
	VerifyError   VerifySeverity = "error"
	VerifyWarning VerifySeverity = "warning"
)

// VerifyFinding is one verifier observation. Table/Index are empty for
// findings not tied to a table (e.g. WAL-level ones).
type VerifyFinding struct {
	Severity VerifySeverity
	Code     string
	Table    string
	Index    string
	Detail   string
}

func (f VerifyFinding) String() string {
	where := ""
	if f.Table != "" {
		where = " table=" + f.Table
		if f.Index != "" {
			where += " index=" + f.Index
		}
	}
	return fmt.Sprintf("[%s] %s%s: %s", f.Severity, f.Code, where, f.Detail)
}

// VerifyReport aggregates the verifier's output over a set of tables.
type VerifyReport struct {
	Findings []VerifyFinding
	Tables   int
	Indexes  int
	// LiveRows is the number of currently visible rows summed over every
	// table, as counted through each table's primary index.
	LiveRows int
}

// HasErrors reports whether any finding has error severity.
func (r *VerifyReport) HasErrors() bool {
	for _, f := range r.Findings {
		if f.Severity == VerifyError {
			return true
		}
	}
	return false
}

func (r *VerifyReport) add(f VerifyFinding) {
	r.Findings = append(r.Findings, f)
}

// VerifyTables runs the read-only scrub over every table known to tm: heap
// structural integrity, index ordering, and the index↔heap cross-checks
// (every entry resolves to a readable record, live rows carry the key their
// index claims, and every index of a table sees the same number of live
// rows). c decodes heap payloads for the key cross-check; nil uses the
// engine's default BSON codec.
//
// The tables must be quiescent — run against a stopped database or a copy.
func VerifyTables(ctx context.Context, tm *TableMetaData, c codec.Codec) (*VerifyReport, error) {
	if c == nil {
		c = bsoncodec.New()
	}
	report := &VerifyReport{}

	for _, tableName := range tm.ListTables() {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		table, err := tm.GetTableByName(tableName)
		if err != nil {
			report.add(VerifyFinding{Severity: VerifyError, Code: "table_unreadable", Table: tableName, Detail: err.Error()})
			continue
		}
		report.Tables++

		if heapV2, ok := table.Heap.(*heapv2.HeapV2); ok {
			issues, err := heapV2.CheckIntegrity(ctx)
			if err != nil {
				return report, err
			}
			for _, issue := range issues {
				report.add(VerifyFinding{Severity: VerifyError, Code: "heap_integrity", Table: tableName, Detail: issue.String()})
			}
		} else {
			report.add(VerifyFinding{Severity: VerifyWarning, Code: "heap_unsupported", Table: tableName, Detail: fmt.Sprintf("heap type %T not scrubbable", table.Heap)})
		}

		liveCounts := make(map[string]int)
		for _, index := range table.GetIndices() {
			if err := ctx.Err(); err != nil {
				return report, err
			}
			report.Indexes++
			live, ok := verifyIndex(ctx, report, c, table, index)
			if ok {
				liveCounts[index.Name] = live
				if index.Primary {
					report.LiveRows += live
				}
			}
		}
		reportCountMismatches(report, tableName, liveCounts)
	}
	return report, nil
}

// verifyIndex scans one index end to end. It returns the number of live rows
// the index resolves to and whether that count is trustworthy (false when the
// index could not be scanned at all).
func verifyIndex(ctx context.Context, report *VerifyReport, c codec.Codec, table *Table, index *Index) (int, bool) {
	treeV2, ok := index.Tree.(*btreev2.BTreeV2)
	if !ok {
		report.add(VerifyFinding{Severity: VerifyWarning, Code: "index_unsupported", Table: table.Name, Index: index.Name, Detail: fmt.Sprintf("tree type %T not scrubbable", index.Tree)})
		return 0, false
	}
	cur, err := treeV2.NewCursor(nil, nil)
	if err != nil {
		report.add(VerifyFinding{Severity: VerifyError, Code: "index_unreadable", Table: table.Name, Index: index.Name, Detail: err.Error()})
		return 0, false
	}
	defer func() { _ = cur.Close() }()

	live := 0
	entries := 0
	var prevKey types.Comparable
	for cur.Next() {
		if entries%256 == 0 && ctx.Err() != nil {
			return live, false
		}
		entries++
		physicalKey := cur.Key()

		if prevKey != nil {
			if cmp, err := prevKey.Compare(physicalKey); err != nil || cmp >= 0 {
				report.add(VerifyFinding{Severity: VerifyError, Code: "index_key_order", Table: table.Name, Index: index.Name,
					Detail: fmt.Sprintf("keys out of order at entry %d (%v then %v)", entries, prevKey, physicalKey)})
			}
		}
		prevKey = physicalKey

		if header, doc, ok := resolveHead(report, table, index, physicalKey, cur.Value()); ok && header.Valid {
			live++
			checkDocCarriesKey(report, c, table, index, physicalKey, doc)
		}
	}
	if err := cur.Err(); err != nil {
		report.add(VerifyFinding{Severity: VerifyError, Code: "index_unreadable", Table: table.Name, Index: index.Name, Detail: fmt.Sprintf("scan stopped: %v", err)})
		return live, false
	}
	return live, true
}

// resolveHead reads the record an index entry points at and walks its version
// chain looking for cycles. It returns the head's header and payload; ok is
// false when the entry resolves to nothing (vacuumed — a legal stale entry)
// or when a finding was reported.
func resolveHead(report *VerifyReport, table *Table, index *Index, physicalKey types.Comparable, offset int64) (*heapv2.RecordHeader, []byte, bool) {
	doc, header, err := table.Heap.Read(offset)
	if isChainEndErr(err) {
		return nil, nil, false // vacuumed: stale entries may legally point here.
	}
	if err != nil {
		report.add(VerifyFinding{Severity: VerifyError, Code: "index_dangling_pointer", Table: table.Name, Index: index.Name,
			Detail: fmt.Sprintf("key %v points at offset %d: %v", physicalKey, offset, err)})
		return nil, nil, false
	}

	// Walk the rest of the chain only to bound it: targets were already
	// bounds-checked by the heap scrub, cycles are detected here.
	steps := 0
	for prev := header.PrevRecordID; prev != heapv2.NoRecordID; steps++ {
		if steps >= verifyChainStepLimit {
			report.add(VerifyFinding{Severity: VerifyError, Code: "heap_chain_cycle", Table: table.Name, Index: index.Name,
				Detail: fmt.Sprintf("version chain from key %v exceeds %d hops (cycle?)", physicalKey, verifyChainStepLimit)})
			return nil, nil, false
		}
		_, prevHeader, err := table.Heap.Read(prev)
		if err != nil {
			break // end of chain (vacuumed) or already reported by the heap scrub.
		}
		prev = prevHeader.PrevRecordID
	}
	return header, doc, true
}

// checkDocCarriesKey cross-checks that a live document really contains, for
// this index's field, the logical key its index entry claims.
func checkDocCarriesKey(report *VerifyReport, c codec.Codec, table *Table, index *Index, physicalKey types.Comparable, raw []byte) {
	parsed, err := c.Open(raw)
	if err != nil {
		report.add(VerifyFinding{Severity: VerifyError, Code: "doc_undecodable", Table: table.Name, Index: index.Name,
			Detail: fmt.Sprintf("key %v: %v", physicalKey, err)})
		return
	}
	docKey, present, err := parsed.Key(index.Name)
	if err != nil || !present {
		report.add(VerifyFinding{Severity: VerifyError, Code: "index_doc_missing_field", Table: table.Name, Index: index.Name,
			Detail: fmt.Sprintf("key %v: document does not expose field %q (present=%v err=%v)", physicalKey, index.Name, present, err)})
		return
	}
	logical := logicalIndexKey(index, physicalKey)
	if !sameComparableKey(docKey, logical) {
		report.add(VerifyFinding{Severity: VerifyError, Code: "index_key_mismatch", Table: table.Name, Index: index.Name,
			Detail: fmt.Sprintf("entry %v resolves to a document whose %q is %v", logical, index.Name, docKey)})
	}
}

// reportCountMismatches flags tables whose indexes disagree on how many live
// rows exist: every index must see exactly one entry per visible row.
func reportCountMismatches(report *VerifyReport, tableName string, liveCounts map[string]int) {
	var refName string
	ref := -1
	for name, count := range liveCounts {
		if ref == -1 {
			refName, ref = name, count
			continue
		}
		if count != ref {
			report.add(VerifyFinding{Severity: VerifyError, Code: "index_row_count_mismatch", Table: tableName,
				Detail: fmt.Sprintf("index %q sees %d live rows but %q sees %d", refName, ref, name, count)})
		}
	}
}

// VerifyWALFile scans every segment of the write-ahead log at path and
// reports entries that violate WAL invariants: undecodable content, poisoned
// (MaxUint64 sentinel) entry LSNs, and checkpoint records whose beginLSN is
// poisoned. A torn tail (crash mid-append) is reported as a warning.
func VerifyWALFile(path string, cipher crypto.Cipher) []VerifyFinding {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return []VerifyFinding{{
			Severity: VerifyWarning,
			Code:     "wal_missing",
			Detail:   fmt.Sprintf("no WAL at %s (fresh or incomplete directory?)", path),
		}}
	}

	reader, err := wal.NewWALReaderWithCipher(path, cipher)
	if err != nil {
		return []VerifyFinding{{
			Severity: VerifyError,
			Code:     "wal_unreadable",
			Detail:   fmt.Sprintf("open %s: %v", path, err),
		}}
	}
	defer func() { _ = reader.Close() }()

	var findings []VerifyFinding
	for count := 0; ; count++ {
		entry, err := reader.ReadEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			if isExpectedWALTail(err) {
				findings = append(findings, VerifyFinding{
					Severity: VerifyWarning,
					Code:     "wal_torn_tail",
					Detail:   fmt.Sprintf("partial entry at the end of the log (entry %d): crash mid-append", count),
				})
				break
			}
			findings = append(findings, VerifyFinding{
				Severity: VerifyError,
				Code:     "wal_unreadable",
				Detail:   fmt.Sprintf("entry %d: %v", count, err),
			})
			break
		}

		if isPoisonedLSN(entry.Header.LSN) {
			findings = append(findings, VerifyFinding{
				Severity: VerifyError,
				Code:     "wal_poisoned_lsn",
				Detail:   fmt.Sprintf("entry %d (type %d) carries the MaxUint64 sentinel as its LSN", count, entry.Header.EntryType),
			})
		}
		if entry.Header.EntryType == wal.EntryCheckpoint && len(entry.Payload) >= 8 {
			if beginLSN := binary.LittleEndian.Uint64(entry.Payload[:8]); isPoisonedLSN(beginLSN) {
				findings = append(findings, VerifyFinding{
					Severity: VerifyError,
					Code:     "wal_checkpoint_poisoned",
					Detail:   fmt.Sprintf("checkpoint record at entry %d carries a poisoned beginLSN", count),
				})
			}
		}
		wal.ReleaseEntry(entry)
	}
	return findings
}
