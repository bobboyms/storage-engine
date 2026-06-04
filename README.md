# Storage Engine

`github.com/bobboyms/storage-engine` is a database-style storage engine written in Go. It provides the durability, recovery, and concurrency primitives of a relational/document store: an 8KB page store, a buffer pool, a slotted-page heap, B+ tree indexes, a write-ahead log, ARIES-lite crash recovery, MVCC snapshot reads, write transactions, online backup/restore, vacuum, and optional transparent data encryption (TDE).

A thin SQL layer (`pkg/sql`) sits on top of these primitives: a single-database SQL dialect with `CREATE TABLE` and schema persistence, `SELECT` (joins, aggregates, subqueries), `INSERT`/`UPDATE`/`DELETE`, explicit transactions, and scheduled maintenance.

It is designed to be read as **data infrastructure**: every change is guarded by tests for durability, crash recovery, concurrency, and index integrity.

## Table of Contents

- [Highlights](#highlights)
- [Scope and Maturity](#scope-and-maturity)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Quick Start](#quick-start)
- [Minimal Usage](#minimal-usage)
- [SQL Layer (`pkg/sql`)](#sql-layer-pkgsql)
- [Key and Data Types](#key-and-data-types)
- [Durability Model](#durability-model)
- [Crash Recovery](#crash-recovery)
- [Transactions and Isolation](#transactions-and-isolation)
- [Backup and Restore](#backup-and-restore)
- [Encryption (TDE)](#encryption-tde)
- [Testing](#testing)
- [Benchmarks](#benchmarks)
- [Examples](#examples)
- [Documentation](#documentation)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

## Highlights

- **8KB page store** with plaintext headers, magic bytes, `pageLSN`, CRC32-Castagnoli checksums over the page body, page-ID validation, and optional AES-GCM body encryption.
- **Buffer pool** with LRU eviction, pinning, per-frame latches, dirty tracking, and synchronous durable flush.
- **Slotted-page heap (v2)** with variable-size records, MVCC version chains, stable `RecordID`s, an in-memory free-space map, and vacuum.
- **B+ tree indexes (v2)** for fixed-size keys and variable-length (varchar) keys, with split, delete + rebalance, range scan, and latch crabbing.
- **Write-ahead log** with CRC checks, paged storage, configurable sync policies, segment rotation/retention, optional archiving, and optional encryption.
- **ARIES-lite crash recovery**: analysis → physical page redo (by `pageLSN`) → idempotent logical redo → logical undo of losers with CLRs. Repeatable and idempotent, including crash-in-the-middle-of-undo.
- **MVCC snapshot reads** with `RepeatableRead` (snapshot isolation) and `ReadCommitted` levels.
- **Write transactions** with strict 2PL locking, WAL-first commit, automatic deadlock detection (waits-for graph, youngest-victim abort), stale-read conflict detection, and a fail-stop degraded mode.
- **In-process recovery (`Heal`)**: rebuild partial in-memory state after a post-commit apply failure without restarting the process.
- **Online backup/restore** with a manifest, file-size validation, and SHA-256 verification.
- **Optional TDE** for heap, indexes, and WAL, with per-subsystem ciphers and a keystore for DEKs.
- **SQL layer (`pkg/sql`)**: a single-database SQL dialect — `CREATE TABLE` with persisted schema, `SELECT` (operators, aggregates/`GROUP BY`/`HAVING`, `INNER`/`LEFT JOIN`, `IN`/scalar/`EXISTS`/`FROM` subqueries), `INSERT`/`UPDATE`/`DELETE`, and explicit transactions with `FOR UPDATE`.
- **Scheduled maintenance**: opened SQL databases run write-activity-gated background maintenance (and a final pass on `Close`) that checkpoints to prune WAL, vacuums tables that had deletes/updates, and clears orphan temp files.
- **Aggressive test suites**: chaos (`kill -9` + reopen), fault injection (corruption, ENOSPC, fsync failure), stress, and the race detector — all wired into CI.

## Scope and Maturity

This engine implements production-grade **durability and recovery primitives** and is exercised by crash, fault, stress, and race tests. It is a strong foundation for studying storage internals, prototypes, and controlled internal workloads.

It is **not** a drop-in replacement for a mature general-purpose RDBMS. Before relying on it for critical data, read the detailed feature matrix and failure model in [docs/ProductionGuide.md](docs/ProductionGuide.md).

What is implemented today:

- WAL with strict per-write fsync (`SyncEveryWrite`), checksums, and segment lifecycle.
- Two-phase crash recovery (physical page redo by `pageLSN` + idempotent logical redo) with logical undo and CLRs.
- MVCC reads, snapshots, and a transactional lock manager with deadlock detection.
- Write transactions with WAL-first commit, fail-stop degraded mode, and in-process `Heal`.
- Online backup/restore with SHA-256 verification, manual vacuum, and optional TDE.
- A SQL layer (`pkg/sql`) with `CREATE TABLE` + persisted schema, `SELECT` (joins, aggregates, subqueries), DML, explicit transactions, and scheduled checkpoint/vacuum/temp-file maintenance.

Known gaps (tracked in the [Roadmap](#roadmap) and ProductionGuide):

- no full ARIES with a persisted dirty-page table or page-oriented physical undo;
- no `Serializable` isolation, predicate/range locking, or write-skew detection;
- no formal anti-starvation / fairness policy;
- no structured metrics / observability subsystem;
- no native fuzzing or differential testing against a reference;
- no replication / failover;
- no compression, column-store, LSM-tree, or hash indexes;
- no persistent free-page list or physical file shrink after deletes (`VACUUM FULL`);
- no background writer / read-ahead (the SQL layer does provide scheduled, activity-gated checkpoint + vacuum maintenance).

## Architecture

Main packages:

| Package | Responsibility |
|---|---|
| `pkg/pagestore` | 8KB page format, `PageFile`, checksums, TDE integration, fsync helpers, and `BufferPool`. |
| `pkg/wal` | WAL entry format, writer, reader, checksums, sync policies, segment lifecycle, and encrypted WAL. |
| `pkg/heap/v2` | Page-based heap, slotted pages, record/MVCC headers, free-space map, and vacuum. |
| `pkg/btree/v2` | Page-based B+ tree indexes, fixed/variable key layouts, split, delete, scan, and latch crabbing. |
| `pkg/storage` | Public engine API: tables, indexes, transactions, recovery, backup, checkpoint, BSON serialization, vacuum, and `Heal`. |
| `pkg/sql` | SQL layer: catalog, lexer/parser, planner, executor, transactions, `CREATE TABLE` + schema persistence, and scheduled maintenance. |
| `pkg/codec` | Document encoding contract and the default BSON codec. |
| `pkg/types` | Comparable key/value types. |
| `pkg/crypto` | Ciphers, AES-GCM, and keystore used by TDE. |
| `pkg/errors` | Project-specific error types. |
| `tests/chaos` | `kill -9` / reopen crash-recovery tests. |
| `tests/faults` | Corruption, ENOSPC, and fsync fault-injection tests. |
| `tests/stress` | Concurrent write/read/delete/scan/checkpoint/vacuum tests. |

## Requirements

- Go `1.25.10` or newer (matches `go.mod`).
- Direct dependencies (kept minimal): `github.com/google/uuid`, `go.mongodb.org/mongo-driver/v2` (BSON), `google.golang.org/protobuf` (WAL payloads).

## Quick Start

Install dependencies:

```bash
go mod download
```

Build and run the binary:

```bash
make build
make run
```

Run the full standard test suite:

```bash
go test ./...
```

Run an example:

```bash
go run ./examples/basic_crud
```

## Minimal Usage

```go
package main

import (
	"log"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func main() {
	heap, err := storage.NewHeapForTable(storage.HeapFormatV2, "data.heap")
	if err != nil {
		log.Fatal(err)
	}

	tables := storage.NewTableMenager()
	if err := tables.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 0, heap); err != nil {
		log.Fatal(err)
	}

	walWriter, err := wal.NewWALWriter("data.wal", wal.DefaultOptions())
	if err != nil {
		log.Fatal(err)
	}

	// NewProductionStorageEngine requires a WAL and runs automatic recovery.
	engine, err := storage.NewProductionStorageEngine(tables, walWriter)
	if err != nil {
		_ = walWriter.Close()
		log.Fatal(err)
	}
	defer engine.Close()

	if err := engine.Put("users", "id", types.IntKey(1), `{"id":1,"name":"Alice"}`); err != nil {
		log.Fatal(err)
	}

	doc, found, err := engine.Get("users", "id", types.IntKey(1))
	if err != nil {
		log.Fatal(err)
	}
	if found {
		log.Println(doc)
	}
}
```

Treat errors from `Put`, `Commit`, `Close`, `Recover`, and `NewProductionStorageEngine` as critical.

## SQL Layer (`pkg/sql`)

`pkg/sql` is a thin SQL layer over the engine. It manages a single database rooted at a directory, persists the schema, and translates a SQL subset into the engine's native access paths (point lookups and ordered index scans plus a residual filter).

```go
ctx := context.Background()
db, err := sql.OpenDatabase(ctx, "/path/to/db") // opens or creates, recovers, starts maintenance
if err != nil {
	log.Fatal(err)
}
defer db.Close() // stops maintenance and runs a final checkpoint

db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT INDEX)")
db.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (1, 'alice', 30)")

rs, err := db.Query(ctx, "SELECT name FROM users WHERE age >= 18 ORDER BY age LIMIT 10")
```

Supported today:

- **DDL + persistence** — `CREATE TABLE name (col TYPE [PRIMARY KEY] [INDEX], …)`. Column types map common SQL names (`INT`, `VARCHAR`, `BOOL`, `FLOAT`, `DATE`, …) to engine types; the schema (columns, types, indexes) is written to `schema.json` and restored on reopen, so tables and data survive restarts with no Go setup code.
- **SELECT** — projection / `*` / `AS` aliases; `WHERE` with `= <> < <= > >=`, `AND`/`OR`, parentheses, `IS [NOT] NULL`, `IN`, `BETWEEN`, `LIKE`; `ORDER BY`, `LIMIT`/`OFFSET`.
- **Aggregates** — `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` (including `COUNT(*)` and `DISTINCT`), `GROUP BY`, and `HAVING`.
- **Joins** — `INNER` and `LEFT [OUTER] JOIN` (chainable), with table aliases and qualified columns (`u.id`).
- **Subqueries** — derived tables in `FROM`, plus `IN (SELECT …)`, scalar `= (SELECT …)`, and correlated `EXISTS`.
- **DML** — `INSERT`, `UPDATE`, `DELETE`.
- **Transactions** — `db.Begin()` returns a `*Tx` exposing `Query`/`Exec`/`Commit`/`Rollback`/`Savepoint`/`RollbackToSavepoint`, with read-your-writes and `SELECT … FOR UPDATE` row locking.
- **Scheduled maintenance** — opened databases run background maintenance on a configurable interval (default 5 min, write-activity-gated so an idle database does no periodic I/O) and a final pass on `Close`: a fuzzy checkpoint that prunes WAL segments, a vacuum of tables that had `DELETE`/`UPDATE`, and a sweep of orphan temp files. Use `db.RunMaintenance(ctx)` to force a pass, or `OpenDatabaseWithOptions(ctx, dir, sql.OpenOptions{...})` to tune the interval or disable it.

Scope and limits: single database per process; the planner chooses one index plus a residual filter (no cost-based optimization); joins use nested loops and full scans; `VACUUM FULL` / physical file shrink is not implemented (vacuum reclaims space for reuse, not back to the OS); inside a transaction, reads are single-table point/scan operations (joins and `FROM` subqueries are rejected). See `examples/sql` and `examples/sql_ddl`.

## Key and Data Types

Index key types are declared via `storage.DataType`:

| `DataType` | Description | Matching `types` key |
|---|---|---|
| `TypeInt` | 64-bit integer | `types.IntKey` |
| `TypeVarchar` | Variable-length string | `types.VarcharKey` |
| `TypeBoolean` | Boolean | `types.BoolKey` |
| `TypeFloat` | 64-bit float | `types.FloatKey` |
| `TypeDate` | Timestamp | `types.DateKey` |
| `TypeBytes` | Binary / BLOB | `types.BytesKey` |

The `pkg/types` package also provides additional comparable types used as keys/values, including `UUIDKey` (native 16-byte UUID), `DateOnlyKey` (calendar date, distinct from timestamp), `DecimalKey` (fixed-point), `NullKey` (NULLS-FIRST cross-type ordering), and `CompositeKey` (used internally by secondary indexes).

## Durability Model

The safest supported configuration is:

- `wal.DefaultOptions()` (uses `SyncEveryWrite`: every WAL entry is fsynced before the write returns);
- `storage.NewProductionStorageEngine` (requires WAL and runs recovery automatically);
- the page-based heap/index formats (`HeapFormatV2`, `BTreeFormatV2`);
- treating errors from durable operations as fatal.

On disk, every page carries a plaintext header (magic bytes, version, page type, `PageID`, `pageLSN`) plus a CRC32-Castagnoli checksum over the body. The WAL validates a CRC32 per logical entry. Corruption in heap, index, or WAL pages is surfaced as an explicit error. When TDE is enabled, the checksum covers the ciphertext so corruption is detected before any decryption attempt.

Alternative WAL sync policies (`SyncInterval`, `SyncBatch`) trade strict per-write durability for throughput and should be chosen deliberately.

## Crash Recovery

Recovery is two-phase and follows an ARIES-lite model:

1. **Analysis** — scan the WAL, build the transaction table, classify winners/losers, and compute the redo start point from the latest checkpoint.
2. **Physical redo** — reapply page after-images when `record.LSN > page.pageLSN` or when the on-disk page is unreadable/corrupt. This repairs torn heap/index pages when the WAL holds the corresponding after-image.
3. **Logical redo** — idempotently reapply winner/autocommit operations not yet reflected in the current state.
4. **Logical undo** — reapply CLRs during redo and undo pending losers at the end, writing new CLRs and `ABORT` markers.

Recovery is idempotent and survives a crash in the middle of undo. It is validated by dedicated chaos tests (`kill -9` + reopen, 100× repeated reopen) and fault tests (torn pages, corrupted WAL/heap/B+ tree). Checkpoints (`CreateCheckpoint`, `FuzzyCheckpoint`) sync the WAL before flushing dirty pages and allow safe WAL truncation.

## Transactions and Isolation

Explicit write transactions are created via `BeginWriteTransaction`:

- writes are buffered in a per-transaction write set and are invisible until `Commit`;
- `Commit` is WAL-first: it writes `BEGIN`, the operation entries (tagged with `txID`), and `COMMIT` to the WAL — durably with `SyncEveryWrite` — **before** applying changes to heap and indexes;
- the apply phase runs under an exclusive runtime barrier, so no public read/write observes a partially applied prefix;
- if the apply phase fails mid-way, the engine enters a **degraded** fail-stop mode and returns `ErrEngineDegraded` until recovered;
- `Rollback` discards the write set and writes an `ABORT` marker when a WAL is present.

Concurrency control:

- strict 2PL via an exclusive lock per logical item `(table, index, key)` for writes (autocommit and transactional paths share the lock manager);
- automatic deadlock detection over a waits-for graph, aborting the youngest transaction with `ErrDeadlockVictim`, plus a lock-wait timeout (5s default) as an additional fence;
- stale-read conflict detection on the same item, rejected with `ErrSerializationConflict` (prevents classic lost updates).

Read isolation levels:

- `ReadCommitted` — fresh snapshot per operation; prevents dirty reads.
- `RepeatableRead` — fixed per-transaction snapshot (snapshot isolation); prevents dirty, non-repeatable, and observational phantom reads within the transaction.

Both levels still allow write skew (no predicate/range locking; no `Serializable`).

### In-process recovery (`Heal`)

Because `COMMIT` is durable before apply, a degraded engine can rebuild its partial in-memory state without a process restart:

- `StorageEngine.Heal(ctx)` replays the WAL idempotently against the already-open files and clears the degraded flag (no-op on a healthy engine; if the underlying fault persists, the engine stays degraded, preserving fail-stop).
- `Options.AutoHealAfterApplyFailure` (default `false`) makes `Commit` attempt the heal automatically on apply failure.

Recommended pattern with the default fail-stop policy: on `storage.ErrEngineDegraded`, call `Heal(ctx)` and retry. See `examples/auto_recovery`.

## Backup and Restore

Online backup pauses writes (`opMu`), runs a checkpoint/flush, copies heap, indexes, and WAL, and writes a `manifest.json` recording each file's size and SHA-256. Restore targets an empty directory and verifies the manifest. The WAL additionally supports size-based rotation, segment retention, optional archiving, and restore of archived segments. See `examples/backup_restore`.

## Encryption (TDE)

TDE is optional and applied per subsystem:

- AES-GCM over each page body, with the page header kept in plaintext for diagnostics/recovery;
- AAD bound to the `PageID`;
- a keystore for DEKs;
- independent ciphers for heap, B+ tree, and WAL.

TDE protects data at rest; it does not provide authentication, authorization, access control, or master-key management. See `examples/tde`.

## Testing

Standard and race tests:

```bash
go test ./...
go test ./pkg/... -race
```

Aggressive suites (build-tagged):

```bash
go test ./tests/chaos  -tags chaos  -count=1 -v
go test ./tests/faults -tags faults -count=1 -v
go test ./tests/stress -tags stress -count=1 -v
go test ./tests/stress -tags stress -race -count=1 -v
```

Make targets:

```bash
make test            # go test ./...
make test-race       # race detector
make test-chaos      # kill -9 / reopen recovery
make test-faults     # corruption, ENOSPC, fsync failure
make test-stress     # concurrent load
make test-stress-race
make test-safety
```

Quality gates (must pass before any change is considered done):

```bash
make lint    # go mod tidy -diff + golangci-lint (0 issues)
make vuln    # govulncheck (0 affecting vulnerabilities)
```

Coverage gate is **70%** total:

```bash
go test ./... -coverprofile=/tmp/coverage.out
go tool cover -func=/tmp/coverage.out
```

CI (`.github/workflows/ci.yml`) runs tidy check, golangci-lint, govulncheck, vet, build, unit tests, race tests, chaos, stress-with-race, and selected disk-fault tests. CodeQL (`security-and-quality`) runs on push/PR to `main` and weekly.

## Benchmarks

Dedicated benchmarks currently live under `experiments/pagestore`, measuring page read/write throughput and AES-GCM encryption overhead for the page format (these inform [ADR 001](docs/adr/001-page-format.md)).

Full-engine benchmarks for large datasets, p95/p99 latency, long WAL recovery, and comparisons against external databases are not yet in place — see the [Roadmap](#roadmap).

## Examples

Runnable examples under `examples/`:

- `basic_crud`, `data_types`, `insert_row_example`, `multi_index`
- `transactions`, `isolation_levels`, `select_for_update`
- `cursor_navigation`, `iterator`, `pagination`
- `backup_restore`, `checkpoint_recovery`, `auto_recovery`
- `concurrent_access`, `vacuum_demo`, `observability`, `tde`
- `heap_internals`, `heap_v2`
- `sql` (queries, joins, subqueries, transactions), `sql_ddl` (`CREATE TABLE`, persistence, maintenance)

Run any example with `go run ./examples/<name>`.