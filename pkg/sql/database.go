package sql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/bobboyms/storage-engine/pkg/codec/bsoncodec"
	"github.com/bobboyms/storage-engine/pkg/crypto"
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
	// keystore is non-nil when the database was opened with TDE. It mints a
	// per-table heap data-encryption key when tables are created at runtime so
	// CREATE TABLE-created heaps are encrypted like the ones built at open time.
	keystore *crypto.KeyStore
}

// defaultMaintenanceInterval is how often a database started with default
// options runs background maintenance (a fuzzy checkpoint, gated on write
// activity, plus a temp-file sweep).
const defaultMaintenanceInterval = 5 * time.Minute

// OpenOptions tunes OpenDatabaseWithOptions. The zero value enables background
// maintenance at the default interval and leaves Transparent Data Encryption
// off.
type OpenOptions struct {
	// MaintenanceInterval sets the background maintenance period; values <= 0
	// use the default interval.
	MaintenanceInterval time.Duration
	// DisableMaintenance turns off automatic background maintenance entirely;
	// the caller can still invoke RunMaintenance on demand.
	DisableMaintenance bool
	// Encryption, when non-nil, enables Transparent Data Encryption (TDE): the
	// heap files, automatic B+ tree indexes, and the write-ahead log are
	// encrypted at rest. The same configuration must be supplied to reopen the
	// database.
	Encryption *EncryptionOptions
}

// defaultKeyStoreFile is the keystore filename created inside the database
// directory when EncryptionOptions.KeyStorePath is empty.
const defaultKeyStoreFile = "keys.json"

// EncryptionOptions configures Transparent Data Encryption for a database
// opened with OpenDatabaseWithOptions.
type EncryptionOptions struct {
	// MasterKey is the 32-byte key-encryption key (KEK). It must come from
	// outside the process (env var, KMS, HSM, secret manager) and is never
	// written to disk. Reopening the database requires the same key.
	MasterKey []byte
	// KeyStorePath is where the wrapped data-encryption keys (DEKs) are
	// persisted. When empty it defaults to "keys.json" inside the database
	// directory.
	KeyStorePath string
}

// newKeyStore opens (or creates) the keystore backing a database's TDE,
// validating the master key. Returns nil when encryption is disabled.
func newKeyStore(dir string, enc *EncryptionOptions) (*crypto.KeyStore, error) {
	if enc == nil {
		return nil, nil
	}
	if len(enc.MasterKey) != crypto.KeySize {
		return nil, fmt.Errorf("sql: encryption master key must be %d bytes, got %d", crypto.KeySize, len(enc.MasterKey))
	}
	path := enc.KeyStorePath
	if path == "" {
		path = filepath.Join(dir, defaultKeyStoreFile)
	}
	ks, err := crypto.NewKeyStore(path, enc.MasterKey)
	if err != nil {
		return nil, fmt.Errorf("sql: open keystore: %w", err)
	}
	return ks, nil
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

	keystore, err := newKeyStore(dir, opts.Encryption)
	if err != nil {
		return nil, err
	}

	tm := storage.NewTableMenager()
	if keystore != nil {
		// Auto-created B+ tree indexes inherit a shared index DEK.
		indexCipher, err := keystore.GetOrCreateDEK("index")
		if err != nil {
			return nil, fmt.Errorf("sql: derive index key: %w", err)
		}
		tm.SetDefaultIndexCipher(indexCipher)
	}
	catalog := NewCatalog()
	for _, s := range schemas {
		if err := registerTable(tm, dir, s, keystore); err != nil {
			return nil, err
		}
		if err := catalog.AddTable(s); err != nil {
			return nil, err
		}
	}

	walPath := filepath.Join(dir, "data.wal")
	walOpts := wal.DefaultOptions()
	if keystore != nil {
		walCipher, err := keystore.GetOrCreateDEK("wal")
		if err != nil {
			return nil, fmt.Errorf("sql: derive wal key: %w", err)
		}
		walOpts.Cipher = walCipher
	}
	ww, err := wal.NewWALWriter(walPath, walOpts)
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
		ddl:     &ddlManager{dir: dir, tm: tm, schemas: schemas, keystore: keystore},
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
// indexes) in the engine's metadata. When keystore is non-nil, the heap is
// encrypted with a per-table data-encryption key.
func registerTable(tm *storage.TableMetaData, dir string, s TableSchema, keystore *crypto.KeyStore) error {
	var heapCipher []crypto.Cipher
	if keystore != nil {
		c, err := keystore.GetOrCreateDEK("heap:" + s.Name)
		if err != nil {
			return fmt.Errorf("sql: derive heap key for %q: %w", s.Name, err)
		}
		heapCipher = []crypto.Cipher{c}
	}
	heap, err := storage.NewHeapForTable(storage.HeapFormatV2, filepath.Join(dir, s.Name+".heap"), heapCipher...)
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
		if stmt.IfNotExists {
			return 0, nil
		}
		return 0, fmt.Errorf("%w: %q", ErrDuplicateTable, schema.Name)
	}

	if err := registerTable(e.ddl.tm, e.ddl.dir, schema, e.ddl.keystore); err != nil {
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

// execAlterTable applies an ADD/DROP COLUMN to a table opened via OpenDatabase.
// It mutates the engine (creating or dropping a sidecar index), refreshes the
// in-memory catalog, and persists the evolved schema so the change survives a
// reopen.
func (e *Executor) execAlterTable(stmt *AlterTableStmt) (int64, error) {
	if e.ddl == nil {
		return 0, fmt.Errorf("%w: ALTER TABLE requires a database opened with OpenDatabase", ErrExec)
	}

	pos, ok := e.ddl.schemaIndex(stmt.Table)
	if !ok {
		return 0, fmt.Errorf("%w: unknown table %q", ErrExec, stmt.Table)
	}
	schema := e.ddl.schemas[pos]

	var (
		newSchema TableSchema
		err       error
	)
	if stmt.Drop {
		newSchema, err = e.alterDropColumn(schema, stmt.Column.Name)
	} else {
		newSchema, err = e.alterAddColumn(schema, stmt.Column)
	}
	if err != nil {
		return 0, err
	}

	if err := e.catalog.ReplaceTable(newSchema); err != nil {
		return 0, err
	}
	e.ddl.schemas[pos] = newSchema
	if err := saveSchemas(e.ddl.dir, e.ddl.schemas); err != nil {
		return 0, err
	}
	return 0, nil
}

// alterAddColumn validates and appends a new column (optionally creating a
// secondary index) to a copy of the schema. The new column starts NULL on every
// existing row; an index on it is empty until subsequent writes populate it.
func (e *Executor) alterAddColumn(schema TableSchema, col ColumnDef) (TableSchema, error) {
	if _, exists := schema.Column(col.Name); exists {
		return TableSchema{}, fmt.Errorf("%w: column %q already exists in table %q", ErrDuplicateColumn, col.Name, schema.Name)
	}
	if col.Primary {
		return TableSchema{}, fmt.Errorf("%w: cannot add a primary key column to table %q", ErrExec, schema.Name)
	}

	ns := cloneSchema(schema)
	ns.Columns = append(ns.Columns, Column{Name: col.Name, Type: col.Type})
	if col.Index {
		if err := e.ddl.tm.AddIndex(schema.Name, storage.Index{Name: col.Name, Type: col.Type}); err != nil {
			return TableSchema{}, fmt.Errorf("%w: add index for %q: %v", ErrExec, col.Name, err)
		}
		ns.Indexes = append(ns.Indexes, IndexDef{Name: col.Name, Column: col.Name})
	}
	return ns, nil
}

// alterDropColumn validates and removes a column (and any secondary index on it)
// from a copy of the schema. The primary key column cannot be dropped. Existing
// heap documents keep the orphaned field bytes until they are rewritten.
func (e *Executor) alterDropColumn(schema TableSchema, name string) (TableSchema, error) {
	if _, exists := schema.Column(name); !exists {
		return TableSchema{}, fmt.Errorf("%w: unknown column %q in table %q", ErrExec, name, schema.Name)
	}
	if pk, ok := schema.PrimaryIndex(); ok && pk.Column == name {
		return TableSchema{}, fmt.Errorf("%w: cannot drop primary key column %q", ErrExec, name)
	}

	if idx, ok := schema.IndexForColumn(name); ok {
		if err := e.ddl.tm.DropIndex(schema.Name, idx.Name); err != nil {
			return TableSchema{}, fmt.Errorf("%w: drop index %q: %v", ErrExec, idx.Name, err)
		}
	}

	ns := TableSchema{Name: schema.Name}
	for _, c := range schema.Columns {
		if c.Name != name {
			ns.Columns = append(ns.Columns, c)
		}
	}
	for _, ix := range schema.Indexes {
		if ix.Column != name {
			ns.Indexes = append(ns.Indexes, ix)
		}
	}
	return ns, nil
}

// cloneSchema returns a deep copy of s so callers can mutate the column and
// index slices without aliasing the original.
func cloneSchema(s TableSchema) TableSchema {
	ns := TableSchema{Name: s.Name}
	ns.Columns = append(ns.Columns, s.Columns...)
	ns.Indexes = append(ns.Indexes, s.Indexes...)
	return ns
}

// schemaIndex returns the position of the named schema in d.schemas.
func (d *ddlManager) schemaIndex(name string) (int, bool) {
	for i := range d.schemas {
		if d.schemas[i].Name == name {
			return i, true
		}
	}
	return -1, false
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
