# AGENTS.md

## Project Context

This repository implements a storage engine in Go (`github.com/bobboyms/storage-engine`). Its goal is to provide database-style storage primitives and guarantees, including B+ Tree indexing, pagestore, heap storage, WAL, recovery, checkpointing, transactions, MVCC, indexes, serialization, and comparable data types.

Treat this codebase as data infrastructure: small regressions can affect durability, consistency, crash recovery, concurrency, and index integrity. Before changing behavior, read the nearby package code and tests.

## Stack and Dependencies

- Language: Go.
- Version: defined in `go.mod` (`go 1.25.6`).
- Module: `github.com/bobboyms/storage-engine`.
- Notable observed dependencies: `github.com/google/uuid`, protobuf, Mongo BSON, compression libraries, Pebble, and indirect helper libraries.
- Main build target: `cmd/storage-engine`.

## Project Structure

- `cmd/storage-engine/`: application and binary entry point.
- `pkg/storage/`: main storage engine layer. Contains engine, table, recovery, transactions, MVCC, backup, checkpointing, serialization, and storage integration tests.
- `pkg/btree/`: B+ Tree interfaces and related implementation used for indexes.
- `pkg/heap/`: heap storage contracts and structures, including record headers.
- `pkg/pagestore/`: page-based storage, buffer pool, page file, fsync, and page-related failure handling.
- `pkg/wal/`: write-ahead log, writer, reader, lifecycle, checksum, log entries, options, and encryption support.
- `pkg/crypto/`: ciphers, AES-GCM, and keystore code used by encryption/TDE features.
- `pkg/types/`: comparable type system, such as integers, text, floats, booleans, and dates.
- `pkg/query/`: scan operators and query conditions.
- `pkg/errors/`: project-specific custom errors.
- `tests/chaos/`: chaos/crash and recovery tests.
- `tests/faults/`: fault-injection tests for corruption, ENOSPC, and fsync failures.
- `tests/stress/`: stress and concurrency tests.
- `experiments/`: isolated experiments, currently including pagestore work.
- `examples/`: executable usage examples for the storage engine, transactions, backup/restore, TDE, indexes, CRUD, vacuum, isolation, and recovery.
- `docs/`: documentation, ADRs, production guides, tutorials, and architecture plans.
- `.agents/skills/`: local project skills that guide Codex behavior.
- `.github/workflows/`: CI pipelines for vet, build, unit tests, race tests, chaos tests, stress tests, and fault tests.

## Available Skills

Use local skills when they are relevant to the task. The skill currently available in this project is:

- `$enforce-tdd-coverage`: enforces TDD with the Red, Green, Refactor cycle and a minimum 70% total coverage gate.
  - Path: `.agents/skills/enforce-tdd-coverage/SKILL.md`.
  - Implicit invocation: enabled in `.agents/skills/enforce-tdd-coverage/agents/openai.yaml`.
  - Must be used automatically for implementations, bug fixes, refactors, behavior changes, API changes, storage engine tasks, and test changes.

## Mandatory TDD Rule

For any production code or behavior change, follow the TDD cycle:

1. Red: write or update a test that fails because the feature/fix does not exist yet.
2. Green: write the minimum code necessary to make the test pass.
3. Refactor: improve organization, clarity, performance, naming, duplication, or structure without breaking the tests.

Rules:

- Do not implement production code before the Red test, except for purely documentary changes, formatting, or metadata with no behavioral effect.
- Keep each Red, Green, Refactor cycle small. For multiple behaviors, repeat the cycle per behavior.
- Do not weaken assertions, remove tests, skip tests, or reduce coverage criteria.
- Added tests must verify real behavior, not artificially inflate coverage.
- After Refactor, run the focused test or relevant suite again to prove the improvement did not break behavior.

## Minimum Coverage

The required minimum total coverage is `70.0%`.

Before finishing any implementation task, run:

```bash
go test ./... -coverprofile=/tmp/go_project_coverage.out
go tool cover -func=/tmp/go_project_coverage.out
```

If total coverage is below `70.0%`, add meaningful tests for the changed code or adjacent behavior before finishing.

Use `/tmp` for temporary coverage profiles to avoid repository churn.

## Development Commands

Build:

```bash
make build
go build ./...
```

Run:

```bash
make run
```

General tests:

```bash
make test
go test ./...
```

Focused package test:

```bash
go test ./pkg/<package> -run <TestName> -count=1
go test ./pkg/<package> -count=1
```

Vet:

```bash
go vet ./...
```

Race:

```bash
make test-race
go test ./pkg/... -race
```

Specialized suites:

```bash
make test-chaos
make test-faults
make test-stress
make test-stress-race
make test-safety
```

## When to Run Specialized Suites

- Changes in WAL, recovery, checkpointing, durability, fsync, or page files: run relevant package tests and consider `make test-faults`, `make test-chaos`, and `make test-safety`.
- Changes in concurrency, MVCC, transactions, buffer pool, or locks: run focused tests, the relevant package suite, and `make test-race`.
- Changes in stress paths or behavior under load: run `make test-stress` or `make test-stress-race`.
- Changes in faults, ENOSPC, corruption, or simulated filesystems: run `make test-faults`.
- General API or storage changes: run `go test ./...` and the full coverage gate.

## CI

The CI in `.github/workflows/ci.yml` runs:

- `go vet ./...`
- `go build ./...`
- `go test ./...`
- `go test ./pkg/... -race`
- `go test ./tests/chaos -tags chaos -count=1 -v`
- `go test ./tests/stress -tags stress -race -count=1 -v`
- fault tests with the `faults` tag, including WAL, heap, BTree, ENOSPC, and fsync.

Use the CI as a reference for required commands when the changed area touches these subsystems.

## Editing Rules

- Read nearby code and tests before editing.
- Preserve existing user changes. Do not revert files you did not change unless the user explicitly requests it.
- Prefer small, local changes.
- Follow existing package patterns before introducing new abstractions.
- Use English only across the repository.
- Do not introduce or keep text in any language other than English in code, comments, error messages, log messages, test names, test failure messages, examples, or documentation.
- When modifying an existing file, translate any non-English text you touch to English as part of the same change.
- Run `gofmt` on changed Go files.
- Avoid temporary files in the repository. Use `/tmp` for temporary artifacts.
- Do not use destructive commands, reset, file checkout, or artifact removal without an explicit user request.

## Expected Final Response

When completing an implementation task, report:

- Red command and result.
- Green command and result.
- Post-Refactor command and result.
- Coverage command.
- Final total coverage.
- Which specialized suites were run or why they were not necessary.

If a command cannot run due to environment limitations, state that explicitly and include the exact command that should be run.
