package sql

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/storage"
)

// execCreateIndex adds a secondary index to an existing table and backfills it
// from the table's current rows. The evolved schema is published to the
// in-memory catalog before the backfill so concurrent writes already maintain
// the new index; the schema file is only persisted after the backfill commits,
// so a crash mid-backfill leaves at worst an orphan index file that the
// persisted schema does not reference. On a backfill error (e.g. a UNIQUE
// index over duplicate data) the catalog is restored and the physical index
// is dropped, leaving no trace.
func (e *Executor) execCreateIndex(ctx context.Context, stmt *CreateIndexStmt) (int64, error) {
	if e.ddl == nil {
		return 0, fmt.Errorf("%w: CREATE INDEX requires a database opened with OpenDatabase", ErrExec)
	}
	pos, ok := e.ddl.schemaIndex(stmt.Table)
	if !ok {
		return 0, fmt.Errorf("%w: unknown table %q", ErrExec, stmt.Table)
	}
	schema := e.ddl.schemas[pos]

	idx, engineType, err := indexDefForCreate(schema, stmt)
	if err != nil {
		return 0, err
	}
	for _, existing := range schema.Indexes {
		if existing.Name == idx.Name {
			if stmt.IfNotExists {
				return 0, nil
			}
			return 0, fmt.Errorf("%w: index %q already exists on table %q", ErrExec, idx.Name, stmt.Table)
		}
	}

	if err := e.ddl.tm.AddIndex(schema.Name, storage.Index{Name: idx.Name, Type: engineType, Unique: idx.Unique}); err != nil {
		return 0, fmt.Errorf("%w: add index %q: %v", ErrExec, idx.Name, err)
	}

	ns := cloneSchema(schema)
	ns.Indexes = append(ns.Indexes, idx)
	if err := e.catalog.ReplaceTable(ns); err != nil {
		_ = e.ddl.tm.DropIndex(schema.Name, idx.Name)
		return 0, err
	}
	e.ddl.schemas[pos] = ns

	if err := e.backfillIndex(ctx, &ns); err != nil {
		_ = e.catalog.ReplaceTable(schema)
		e.ddl.schemas[pos] = schema
		_ = e.ddl.tm.DropIndex(schema.Name, idx.Name)
		return 0, err
	}

	if err := saveSchemas(e.ddl.dir, e.ddl.schemas); err != nil {
		return 0, err
	}
	// The backfill rewrote every row, leaving dead versions behind.
	e.markGarbage(stmt.Table)
	return 0, nil
}

// indexDefForCreate validates the statement's columns against the schema and
// builds the catalog index definition plus the engine key type. A composite
// index is registered as VARCHAR because its key is the encoded column tuple.
func indexDefForCreate(schema TableSchema, stmt *CreateIndexStmt) (IndexDef, storage.DataType, error) {
	for _, c := range stmt.Columns {
		if _, ok := schema.Column(c); !ok {
			return IndexDef{}, 0, fmt.Errorf("%w: unknown column %q in table %q", ErrExec, c, stmt.Table)
		}
	}
	idx := IndexDef{Unique: stmt.Unique}
	var engineType storage.DataType
	if len(stmt.Columns) == 1 {
		col, _ := schema.Column(stmt.Columns[0])
		idx.Name, idx.Column, engineType = stmt.Columns[0], stmt.Columns[0], col.Type
	} else {
		idx.Name, idx.Columns, engineType = compositeIndexName(stmt.Columns), stmt.Columns, storage.TypeVarchar
	}
	if stmt.Name != "" {
		idx.Name = stmt.Name
	}
	return idx, engineType, nil
}

// backfillIndex rewrites every committed row of the table through one write
// transaction so the newly added index (already present in schema) receives an
// entry per row. The committed rows are materialized before any write is
// staged so the scan cannot observe its own rewrites. The whole backfill is
// atomic: a failure (such as a UNIQUE violation over pre-existing duplicates)
// rolls back and leaves the table untouched.
func (e *Executor) backfillIndex(ctx context.Context, schema *TableSchema) error {
	pk, ok := schema.PrimaryIndex()
	if !ok {
		return fmt.Errorf("%w: table %q has no primary index", ErrExec, schema.Name)
	}
	it, err := e.engine.NewIterator(ctx, schema.Name, pk.Name, storage.IterOptions{})
	if err != nil {
		return fmt.Errorf("%w: open iterator: %v", ErrExec, err)
	}
	var raws [][]byte
	for it.Next() {
		raws = append(raws, append([]byte(nil), it.Value()...))
	}
	scanErr := it.Err()
	_ = it.Close()
	if scanErr != nil {
		return fmt.Errorf("%w: scan: %v", ErrExec, scanErr)
	}
	if len(raws) == 0 {
		return nil
	}

	wtx := e.engine.BeginWriteTransaction()
	for _, raw := range raws {
		doc, err := rawToMap(e.codec, raw)
		if err != nil {
			_ = wtx.Rollback(ctx)
			return err
		}
		keys, err := keysFromMap(schema, doc)
		if err != nil {
			_ = wtx.Rollback(ctx)
			return err
		}
		addCompositeKeyFields(doc, schema, keys)
		jsonDoc, err := json.Marshal(doc)
		if err != nil {
			_ = wtx.Rollback(ctx)
			return fmt.Errorf("%w: encode document: %v", ErrExec, err)
		}
		if err := wtx.WriteRow(ctx, schema.Name, string(jsonDoc), keys, false); err != nil {
			_ = wtx.Rollback(ctx)
			if ue := asUniqueViolation(err); ue != nil {
				return ue
			}
			return fmt.Errorf("%w: backfill index: %v", ErrExec, err)
		}
	}
	if err := wtx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit index backfill: %v", ErrExec, err)
	}
	return nil
}

// execDropIndex detaches a secondary index from the table, deletes its backing
// file, and persists the shrunken schema. With IfExists, a missing index is a
// silent no-op. The primary index backs the table and cannot be dropped.
func (e *Executor) execDropIndex(stmt *DropIndexStmt) (int64, error) {
	if e.ddl == nil {
		return 0, fmt.Errorf("%w: DROP INDEX requires a database opened with OpenDatabase", ErrExec)
	}
	pos, ok := e.ddl.schemaIndex(stmt.Table)
	if !ok {
		return 0, fmt.Errorf("%w: unknown table %q", ErrExec, stmt.Table)
	}
	schema := e.ddl.schemas[pos]

	found := -1
	for i, idx := range schema.Indexes {
		if idx.Name == stmt.Name {
			found = i
			break
		}
	}
	if found < 0 {
		if stmt.IfExists {
			return 0, nil
		}
		return 0, fmt.Errorf("%w: unknown index %q on table %q", ErrExec, stmt.Name, stmt.Table)
	}
	if schema.Indexes[found].Primary {
		return 0, fmt.Errorf("%w: cannot drop primary index %q", ErrExec, stmt.Name)
	}

	if err := e.ddl.tm.DropIndex(schema.Name, stmt.Name); err != nil {
		return 0, fmt.Errorf("%w: drop index %q: %v", ErrExec, stmt.Name, err)
	}

	ns := TableSchema{Name: schema.Name, Columns: append([]Column(nil), schema.Columns...)}
	for i, idx := range schema.Indexes {
		if i != found {
			ns.Indexes = append(ns.Indexes, idx)
		}
	}
	if err := e.catalog.ReplaceTable(ns); err != nil {
		return 0, err
	}
	e.ddl.schemas[pos] = ns
	return 0, saveSchemas(e.ddl.dir, e.ddl.schemas)
}
