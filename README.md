# Storage Engine

`github.com/bobboyms/storage-engine` is an storage engine written in Go. It implements page-based storage primitives, B+ tree indexes, WAL, crash recovery, MVCC-style reads, write transactions, backup/restore, vacuum, and optional transparent data encryption.

## Status

The project has a meaningful storage foundation and aggressive tests, but it is not a general-purpose production database yet.

Use it for:

- studying storage engine internals;
- prototypes and controlled internal workloads;
- experimenting with WAL, page stores, B+ trees, MVCC, TDE, and recovery.

Do not use it as the primary store for critical production data unless you have reviewed the failure model in [docs/ProductionGuide.md](docs/ProductionGuide.md).

Important current limits:

- no complete ARIES implementation;
- no physical page-level redo using `pageLSN`;
- no physical undo log or CLRs;
- no serializable isolation;
- no deadlock detector;
- no structured metrics/observability;
- no native fuzzing or differential testing;
- no replication/failover;
- no compression;
- no persistent free-page list;
- no autovacuum/background writer.

## Features

- Fixed-size 8KB page store with page headers, magic bytes, checksums, page IDs, and optional AES-GCM body encryption.
- Buffer pool with LRU eviction, pinning, per-frame latches, dirty tracking, and durable flush.
- Heap v2 based on slotted pages, variable-size records, MVCC metadata, stable `RecordID`s, and vacuum.
- B+ tree v2 indexes for fixed-size keys and varchar keys.
- WAL with CRC checks, page-based storage, sync policies, segment lifecycle, and optional encryption.
- Production constructor with automatic recovery: `storage.NewProductionStorageEngine`.
- Logical recovery for autocommit entries and committed write transactions.
- Read transactions with `RepeatableRead` and `ReadCommitted` behavior.
- Explicit write transactions with `BEGIN`, operation entries, `COMMIT`, and `ABORT` markers in WAL.
- Backup/restore with manifest, file size validation, and SHA-256 verification.
- Optional TDE for heap, indexes, and WAL.
- Chaos, fault-injection, stress, race, and corruption tests.

## Architecture

Main packages:

- `pkg/pagestore`: 8KB page format, `PageFile`, checksums, TDE integration, fsync helpers, and `BufferPool`.
- `pkg/wal`: WAL entry format, writer, reader, checksums, sync policies, segment lifecycle, and encrypted WAL support.
- `pkg/heap/v2`: page-based heap, slotted pages, record headers, MVCC chains, free space map, and vacuum.
- `pkg/btree/v2`: page-based B+ tree indexes, fixed/variable key layouts, split, delete, scan, and latch crabbing.
- `pkg/storage`: public storage engine API, tables, indexes, transactions, recovery, backup, checkpoint, BSON serialization, and vacuum dispatch.
- `pkg/types`: comparable key types.
- `pkg/query`: scan conditions and operators.
- `tests/chaos`: kill/reopen recovery tests.
- `tests/faults`: corruption, ENOSPC, and fsync fault tests.
- `tests/stress`: concurrent write/read/delete/scan/checkpoint/vacuum tests.

## Quick Start

Prerequisite:

- Go `1.25.6` or newer, matching `go.mod`.

Install dependencies:

```bash
go mod download
```

Run the full normal test suite:

```bash
go test ./...
```

Build the command:

```bash
make build
```

Run the command:

```bash
make run
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

For more examples, see:

- `examples/basic_crud`
- `examples/transactions`
- `examples/isolation_levels`
- `examples/backup_restore`
- `examples/tde`
- `examples/vacuum_demo`

## Durability Model

The safest supported path is:

- use `wal.DefaultOptions()`;
- use `storage.NewProductionStorageEngine`;
- use page-based heap/index formats;
- treat errors from `Put`, `Commit`, `Close`, `Recover`, and startup as critical.

`wal.DefaultOptions()` uses `SyncEveryWrite`, so each WAL entry is fsynced before the write returns. The page store also uses page checksums and validates magic bytes/page IDs on read.

Recovery is currently logical and idempotent. It reconstructs heap/index state from WAL entries and skips entries covered by fuzzy checkpoints when possible. It is not yet full ARIES.

## Transactions

The engine supports explicit write transactions through `BeginWriteTransaction`.

Current behavior:

- writes are buffered in a transaction write set;
- `Commit` writes `BEGIN`, operation entries, and `COMMIT` to WAL before applying changes;
- `Rollback` discards the pending write set and writes `ABORT` when WAL is present;
- crash recovery reapplies committed transaction entries and drops loser/aborted transactions.

Important limitation: after a durable `COMMIT`, the in-memory application step still applies operations sequentially. If the live process returns an error mid-application, there is no runtime undo of the already-applied prefix. Crash after durable commit is handled by recovery, but live partial-application errors are not yet fully atomic.

## Testing

Standard tests:

```bash
go test ./...
```

Race tests:

```bash
go test ./pkg/... -race
```

Aggressive suites:

```bash
go test ./tests/chaos -tags chaos -count=1 -v
go test ./tests/faults -tags faults -count=1 -v
go test ./tests/stress -tags stress -count=1 -v
go test ./tests/stress -tags stress -race -count=1 -v
```

Make targets:

```bash
make test
make test-race
make test-chaos
make test-faults
make test-stress
make test-stress-race
make test-safety
```

CI runs unit tests, race tests, chaos tests, stress tests with race detector, and selected disk fault tests.

## Benchmarks

The only dedicated benchmarks currently live under `experiments/pagestore`. They measure page read/write and encryption overhead for the page format.

There are not yet mature full-engine benchmarks for large datasets, p95/p99 latency, long WAL recovery, mixed read/write workloads, or comparisons against external databases.

## Documentation

- [Production Guide](docs/ProductionGuide.md): current implemented/not-implemented feature matrix and safety notes.
- [ADR 001: Page Format](docs/adr/001-page-format.md): 8KB page layout, checksum, TDE, and format rationale.
- [Page-Based Migration Plan](docs/page_based_migration_plan.md): historical implementation plan for page-based storage.
- [Relational Database Roadmap](docs/relational_database_roadmap.md): larger roadmap notes.
- [Tutorial](docs/tutorial.md): Go/tutorial material used around the project.

## Roadmap

High-value next steps:

- page-level redo using `pageLSN`;
- physical undo or explicit formalization of the no-physical-undo model;
- stronger runtime atomicity for write transactions after durable commit;
- persistent free-page list and page allocator;
- structured metrics for WAL, BufferPool, recovery, vacuum, locks, and fsync;
- native fuzzing for WAL, page files, slotted pages, and B+ tree pages;
- differential tests against a reference implementation;
- large-dataset benchmarks;
- background writer and read-ahead;
- autovacuum and bloat thresholds;
- compression;
- replication/failover.

## Contributing

This project treats storage behavior as high risk. Small changes can affect durability, recovery, concurrency, or data integrity.

Before opening a change:

- read nearby code and tests;
- add or update tests first when changing behavior;
- keep coverage at or above the project minimum of 70%;
- run focused tests for the touched package;
- run `go test ./...` before submitting;
- run race/chaos/fault/stress tests when touching WAL, recovery, page files, transactions, concurrency, vacuum, or BufferPool;
- do not change on-disk formats without versioning and migration/recovery notes;
- update [docs/ProductionGuide.md](docs/ProductionGuide.md) when guarantees or limitations change.

## License

No license file is currently present. Add one before publishing or accepting external contributions.
