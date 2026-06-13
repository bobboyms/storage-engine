package sql

import (
	"context"
	"fmt"
)

// migrationStep pairs a migration's SQL text with its parsed statement.
type migrationStep struct {
	sql  string
	stmt Statement
}

// Migrate runs a sequence of DDL statements (CREATE TABLE / ALTER TABLE) as a
// single atomic schema change. Every step is first validated against a shadow
// copy of the schema set, so a logical error in any step — a duplicate column,
// an unknown table, an invalid type — aborts the whole migration before any
// physical or on-disk state is touched: nothing is applied. Only after all
// steps validate does Migrate apply them and persist the evolved schema exactly
// once, which keeps the persisted schema all-or-nothing instead of stopping
// halfway through a multi-step migration.
//
// Migrate requires a database opened with OpenDatabase. It accepts only DDL;
// INSERT/UPDATE/DELETE/SELECT are rejected.
//
// A failure of a physical operation during the apply phase (a rare disk-level
// error, never a logical one, since those are caught in validation) restores
// the in-memory catalog and schema list and leaves the schema file unchanged,
// so a reopen sees the pre-migration schema. Such a failure may leave orphan
// heap/index files for tables created earlier in the same migration; they are
// not referenced by the persisted schema and are ignored on reopen.
func (e *Executor) Migrate(ctx context.Context, statements []string) error {
	if e.ddl == nil {
		return fmt.Errorf("%w: Migrate requires a database opened with OpenDatabase", ErrExec)
	}
	if len(statements) == 0 {
		return nil
	}

	// Parse up front; Migrate accepts only DDL statements. Pairing each SQL with
	// its parsed statement keeps the two together for error messages.
	steps := make([]migrationStep, len(statements))
	for i, sql := range statements {
		stmt, err := Parse(sql)
		if err != nil {
			return fmt.Errorf("migrate step %d (%s): %w", i+1, sql, err)
		}
		switch stmt.(type) {
		case *CreateTableStmt, *AlterTableStmt:
		default:
			return fmt.Errorf("%w: Migrate only accepts CREATE TABLE and ALTER TABLE, got step %d (%s)", ErrExec, i+1, sql)
		}
		steps[i] = migrationStep{sql: sql, stmt: stmt}
	}

	// Phase 1: validate every step against a shadow schema set. No physical,
	// catalog, or disk state is touched, so any error aborts with nothing done.
	shadow := make([]TableSchema, len(e.ddl.schemas))
	for i := range e.ddl.schemas {
		shadow[i] = cloneSchema(e.ddl.schemas[i])
	}
	for i, step := range steps {
		next, err := validateDDL(shadow, step.stmt)
		if err != nil {
			return fmt.Errorf("migrate step %d (%s): %w", i+1, step.sql, err)
		}
		shadow = next
	}

	// Phase 2: apply physically. Snapshot in-memory state so it can be restored
	// if a physical operation fails; the schema file is written once, at the end.
	snapSchemas := make([]TableSchema, len(e.ddl.schemas))
	for i := range e.ddl.schemas {
		snapSchemas[i] = cloneSchema(e.ddl.schemas[i])
	}
	snapCatalog := e.catalog.snapshot()
	rollback := func() {
		e.ddl.schemas = snapSchemas
		e.catalog.restore(snapCatalog)
	}

	for i, step := range steps {
		if err := e.applyDDL(ctx, step.stmt); err != nil {
			rollback()
			return fmt.Errorf("migrate step %d (%s): %w", i+1, step.sql, err)
		}
	}

	if err := saveSchemas(e.ddl.dir, e.ddl.schemas); err != nil {
		rollback()
		return err
	}
	return nil
}

// validateDDL applies a statement's logical effect to a shadow schema set and
// returns the evolved set, without touching engine, catalog, or disk state.
func validateDDL(shadow []TableSchema, stmt Statement) ([]TableSchema, error) {
	switch s := stmt.(type) {
	case *CreateTableStmt:
		schema := schemaFromCreate(s)
		if err := schema.validate(); err != nil {
			return nil, err
		}
		if shadowIndex(shadow, schema.Name) >= 0 {
			if s.IfNotExists {
				return shadow, nil
			}
			return nil, fmt.Errorf("%w: %q", ErrDuplicateTable, schema.Name)
		}
		return append(shadow, schema), nil
	case *AlterTableStmt:
		pos := shadowIndex(shadow, s.Table)
		if pos < 0 {
			return nil, fmt.Errorf("%w: unknown table %q", ErrExec, s.Table)
		}
		if s.noop(shadow[pos]) {
			return shadow, nil // guarded no-op: schema unchanged.
		}
		var (
			ns  TableSchema
			err error
		)
		switch {
		case s.Rename:
			ns, err = evolveRenameColumn(shadow[pos], s.Column.Name, s.NewName)
		case s.Drop:
			ns, err = evolveDropColumn(shadow[pos], s.Column.Name)
		default:
			ns, err = evolveAddColumn(shadow[pos], s.Column)
		}
		if err != nil {
			return nil, err
		}
		shadow[pos] = ns
		return shadow, nil
	default:
		return nil, fmt.Errorf("%w: Migrate only accepts CREATE TABLE and ALTER TABLE", ErrExec)
	}
}

// applyDDL performs the physical and in-memory effects of one DDL statement
// without persisting the schema file (the caller persists once at the end).
func (e *Executor) applyDDL(ctx context.Context, stmt Statement) error {
	switch s := stmt.(type) {
	case *CreateTableStmt:
		_, err := e.applyCreate(s)
		return err
	case *AlterTableStmt:
		_, err := e.applyAlter(ctx, s)
		return err
	default:
		return fmt.Errorf("%w: Migrate only accepts CREATE TABLE and ALTER TABLE", ErrExec)
	}
}

// shadowIndex returns the position of name in shadow, or -1.
func shadowIndex(shadow []TableSchema, name string) int {
	for i := range shadow {
		if shadow[i].Name == name {
			return i
		}
	}
	return -1
}
