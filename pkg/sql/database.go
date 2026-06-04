package sql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/bobboyms/storage-engine/pkg/codec/bsoncodec"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// btreeOrder is the B-tree order passed when registering a table; the V2 trees
// created by the engine size their nodes by page, so the exact value is not
// significant.
const btreeOrder = 3

// ddlManager holds the state needed to create tables at runtime and persist the
// schema. It is present only on executors opened via OpenDatabase.
type ddlManager struct {
	dir     string
	tm      *storage.TableMetaData
	schemas []TableSchema
}

// defaultMaintenanceInterval is how often a database started with default
// options runs background maintenance (a fuzzy checkpoint, gated on write
// activity, plus a temp-file sweep).
const defaultMaintenanceInterval = 5 * time.Minute

// OpenOptions tunes OpenDatabaseWithOptions. The zero value enables background
// maintenance at the default interval.
type OpenOptions struct {
	// MaintenanceInterval sets the background maintenance period; values <= 0
	// use the default interval.
	MaintenanceInterval time.Duration
	// DisableMaintenance turns off automatic background maintenance entirely;
	// the caller can still invoke RunMaintenance on demand.
	DisableMaintenance bool
}

// OpenDatabase opens (or creates) a SQL database rooted at dir with default
// options (background maintenance enabled). See OpenDatabaseWithOptions.
func OpenDatabase(ctx context.Context, dir string) (*Executor, error) {
	return OpenDatabaseWithOptions(ctx, dir, OpenOptions{})
}

// OpenDatabaseWithOptions opens (or creates) a SQL database rooted at dir. It
// reconstructs every table declared in the persisted schema (one heap file per
// table) and a shared write-ahead log, recovers committed data, and returns an
// Executor that can run queries, DML, and CREATE TABLE. Schema changes are
// persisted so a later open on the same directory restores the tables without
// any Go setup code. Unless disabled, it starts background maintenance that
// periodically checkpoints (bounding WAL growth) and clears orphan temp files.
func OpenDatabaseWithOptions(ctx context.Context, dir string, opts OpenOptions) (*Executor, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("sql: create database dir: %w", err)
	}

	// Clear orphan temp files left by an interrupted atomic write before any
	// new writing starts (minAge 0 is safe here: nothing is writing yet).
	if _, err := sweepTempFiles(dir, 0); err != nil {
		return nil, err
	}

	schemas, err := loadSchemas(dir)
	if err != nil {
		return nil, err
	}

	tm := storage.NewTableMenager()
	catalog := NewCatalog()
	for _, s := range schemas {
		if err := registerTable(tm, dir, s); err != nil {
			return nil, err
		}
		if err := catalog.AddTable(s); err != nil {
			return nil, err
		}
	}

	walPath := filepath.Join(dir, "data.wal")
	ww, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		return nil, fmt.Errorf("sql: open wal: %w", err)
	}
	engine, err := storage.NewStorageEngine(tm, ww)
	if err != nil {
		_ = ww.Close()
		return nil, fmt.Errorf("sql: open engine: %w", err)
	}
	if err := engine.Recover(ctx, walPath); err != nil {
		_ = engine.Close()
		return nil, fmt.Errorf("sql: recover: %w", err)
	}

	exec := &Executor{
		engine:  engine,
		catalog: catalog,
		codec:   bsoncodec.New(),
		ddl:     &ddlManager{dir: dir, tm: tm, schemas: schemas},
	}
	// Arm activity gating from the recovered state so an idle database does no
	// periodic checkpoint work until the first write.
	exec.lastCheckpointLSN.Store(engine.Stats().CurrentLSN)

	if !opts.DisableMaintenance {
		interval := opts.MaintenanceInterval
		if interval <= 0 {
			interval = defaultMaintenanceInterval
		}
		exec.StartMaintenance(interval)
	}
	return exec, nil
}

// Close stops any scheduled maintenance, runs a final maintenance pass (a
// gated checkpoint that compacts the WAL plus a temp-file sweep), and releases
// the underlying storage engine. It is only meaningful for an executor created
// by OpenDatabase.
func (e *Executor) Close() error {
	e.stopMaintenance()
	if e.engine == nil {
		return nil
	}
	_, _ = e.RunMaintenance(context.Background())
	return e.engine.Close()
}

// registerTable creates the table's heap file and registers it (with its
// indexes) in the engine's metadata.
func registerTable(tm *storage.TableMetaData, dir string, s TableSchema) error {
	heap, err := storage.NewHeapForTable(storage.HeapFormatV2, filepath.Join(dir, s.Name+".heap"))
	if err != nil {
		return fmt.Errorf("sql: create heap for %q: %w", s.Name, err)
	}
	if err := tm.NewTable(s.Name, indicesForSchema(s), btreeOrder, heap); err != nil {
		return fmt.Errorf("sql: register table %q: %w", s.Name, err)
	}
	return nil
}

// indicesForSchema builds the engine index list for a schema, resolving each
// index column's data type.
func indicesForSchema(s TableSchema) []storage.Index {
	indices := make([]storage.Index, 0, len(s.Indexes))
	for _, idx := range s.Indexes {
		col, _ := s.Column(idx.Column)
		indices = append(indices, storage.Index{
			Name:    idx.Name,
			Primary: idx.Primary,
			Type:    col.Type,
		})
	}
	return indices
}

// execCreateTable creates the physical table, registers its schema in the
// catalog, and persists the updated schema set.
func (e *Executor) execCreateTable(stmt *CreateTableStmt) (int64, error) {
	if e.ddl == nil {
		return 0, fmt.Errorf("%w: CREATE TABLE requires a database opened with OpenDatabase", ErrExec)
	}

	schema := schemaFromCreate(stmt)
	if err := schema.validate(); err != nil {
		return 0, err
	}
	if _, exists := e.catalog.Table(schema.Name); exists {
		return 0, fmt.Errorf("%w: %q", ErrDuplicateTable, schema.Name)
	}

	if err := registerTable(e.ddl.tm, e.ddl.dir, schema); err != nil {
		return 0, err
	}
	if err := e.catalog.AddTable(schema); err != nil {
		return 0, err
	}

	e.ddl.schemas = append(e.ddl.schemas, schema)
	if err := saveSchemas(e.ddl.dir, e.ddl.schemas); err != nil {
		return 0, err
	}
	return 0, nil
}

// schemaFromCreate converts a parsed CREATE TABLE into a catalog schema.
func schemaFromCreate(stmt *CreateTableStmt) TableSchema {
	schema := TableSchema{Name: stmt.Table}
	for _, c := range stmt.Columns {
		schema.Columns = append(schema.Columns, Column{Name: c.Name, Type: c.Type})
		switch {
		case c.Primary:
			schema.Indexes = append(schema.Indexes, IndexDef{Name: c.Name, Column: c.Name, Primary: true})
		case c.Index:
			schema.Indexes = append(schema.Indexes, IndexDef{Name: c.Name, Column: c.Name})
		}
	}
	return schema
}
