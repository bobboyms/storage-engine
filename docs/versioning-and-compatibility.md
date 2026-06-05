# Versioning and on-disk compatibility

This document is the policy for evolving the storage engine without breaking data
that existing databases and backups already hold. Follow it whenever you change
anything that is written to disk.

## Two independent version axes

Do **not** conflate these:

1. **Library / API version** — the Go module's semver (`v1`, `v2`, …). It governs
   the API that callers compile against. It may change without any on-disk change.
2. **On-disk format version** — one integer per persisted format, bumped *only*
   when the bytes or their meaning change in a way an older build cannot read.
   Compatibility on open and on restore depends on **this** axis, never on the API
   version.

A patch release can bump an on-disk format; a major API release might not touch
the on-disk format at all. Keep them separate.

## Where format versions live

| Format | Marker | Location |
|---|---|---|
| Catalog (`schema.json`) | `CatalogFormatVersion` + `{"format_version":N,…}` envelope | `pkg/sql/schema_store.go` |
| Engine data (heap / WAL / indexes) | `DataFormatVersion`, stamped into backups | `pkg/storage/backup.go` |
| WAL records | `WALMagic` + `WALVersion` | `pkg/wal/entry.go` |
| B+tree v2 | `treeMetaMagic` + `treeMetaVersion` | `pkg/btree/v2/btree.go` |
| Page files | `MagicV1` + `VersionV1` | `pkg/pagestore/pagefile.go` |
| Backup manifest | `backupManifestVer` + `manifest.Version` | `pkg/storage/backup.go` |

Every binary format carries a magic number and a version, and **refuses** a
version it does not understand rather than risking a misread.

## The compatibility rules

### Default: additive, self-describing changes (no version bump)

Prefer changes that old and new readers both tolerate:

- Catalog / metadata in JSON: add **optional** fields with `json:",omitempty"`. An
  old reader ignores the unknown field; a new reader defaults the missing one.
  (This is how composite indexes and `UNIQUE` were added — `Columns` and `Unique`
  are `omitempty`.)
- Binary formats: append **tag/length-prefixed** fields so an old reader can skip
  what it does not recognize.

If a change is purely additive in this sense, **keep the format version the same**.

### When a bump is required

Bump the format version when the layout or semantics change incompatibly (a field
is removed/renamed, a value is reinterpreted, an encoding changes). Then:

1. Increment the version constant (`CatalogFormatVersion` / `DataFormatVersion`).
2. Add a migration step (see below) that rewrites old data into the new shape.
3. Add or refresh a **golden fixture** (see below).

### Open: migrate forward, never sideways or backward

On open, if the stored format version is **older** than the build, run the ordered
migration chain up to the current version, then use the data. If it is **newer**,
refuse with a typed error (`ErrUnsupportedCatalogVersion`) — an older build must
not guess at a newer format.

### Restore: gate on the data format version

A backup records the `DataFormatVersion` of the files it captured. On
`VerifyBackup` / `RestoreBackup`:

| Backup vs. build | Action |
|---|---|
| equal | restore directly |
| backup older, migration path exists | restore, then migrate on open |
| backup newer than the build | **refuse** (`ErrUnsupportedDataFormat`) |

A legacy backup or catalog with no version field decodes to zero and is treated as
version 1, so pre-versioning data still restores and opens.

## Adding a catalog migration

The catalog migration chain lives in `pkg/sql/migrations.go`. To add one:

```go
catalogMigrations = append(catalogMigrations, catalogMigration{
    from: 1, to: 2,
    apply: func(tables []persistedTable) ([]persistedTable, error) {
        // rewrite the v1 persisted form into the v2 form
        return tables, nil
    },
})
```

Rules: the chain must be **contiguous** (each step's `to` is the next step's
`from`, ending at `CatalogFormatVersion`); steps must be deterministic and
idempotent in effect, because a catalog that is not re-stamped is migrated again
on the next open. Migrated tables are re-stamped to the current version on the
next schema write.

## Golden fixtures (the regression contract)

For each historical on-disk format, commit a small, **frozen** fixture and a test
that opens/loads it with the current binary (see
`pkg/sql/testdata/legacy_catalog/` and `TestGoldenLegacyCatalogOpens`). These are
the guardrail that a format change does not silently break old data.

When a format bump forces a fixture to change: **do not edit the frozen file to
make the test pass.** Keep the old fixture, add a migration that upgrades it, and
add a new fixture for the new format. The old fixture must keep loading (through
migration) forever, or for as many versions back as the project commits to
support.
