package sql

import (
	"context"
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// rowLookup is a point read used by foreign-key checks: it reports whether a
// row with the given key exists in table's index. The Executor reads the
// committed snapshot; a Tx reads through its write transaction so staged
// parents satisfy the constraint.
type rowLookup func(ctx context.Context, table, index string, key types.Comparable) (bool, error)

// tableScan materializes every row of a table, used by the delete-restrict
// check to find children referencing the deleted parent keys.
type tableScan func(ctx context.Context, schema *TableSchema) ([]Row, error)

// walkColumnRefs invokes fn for every column reference inside expr.
func walkColumnRefs(expr Expr, fn func(*ColumnRef)) {
	switch e := expr.(type) {
	case *ColumnRef:
		fn(e)
	case *IsNullExpr:
		walkColumnRefs(e.Operand, fn)
	case *BinaryExpr:
		walkColumnRefs(e.Left, fn)
		walkColumnRefs(e.Right, fn)
	case *ArithExpr:
		walkColumnRefs(e.Left, fn)
		walkColumnRefs(e.Right, fn)
	case *FuncCall:
		for _, a := range e.Args {
			walkColumnRefs(a, fn)
		}
	case *CaseExpr:
		for _, w := range e.Whens {
			walkColumnRefs(w.Cond, fn)
			walkColumnRefs(w.Then, fn)
		}
		if e.Else != nil {
			walkColumnRefs(e.Else, fn)
		}
	case *AggregateExpr:
		if e.Call.Column != nil {
			fn(e.Call.Column)
		}
	}
}

// checkIsUnknown reports whether a CHECK evaluation is UNKNOWN for this row:
// any column the expression references is NULL. SQL CHECK constraints pass
// when the condition is TRUE or UNKNOWN, and the boolean evaluator collapses
// UNKNOWN to false, so NULL operands are detected up front instead.
func checkIsUnknown(chk Expr, row Row) bool {
	unknown := false
	walkColumnRefs(chk, func(c *ColumnRef) {
		if v, ok := row[c.String()]; ok && isNull(v) {
			unknown = true
		}
	})
	return unknown
}

// enforceChecks validates every CHECK constraint of schema against the row,
// returning ErrCheckViolation on the first failing constraint.
func enforceChecks(schema *TableSchema, row Row) error {
	for _, chk := range schema.Checks {
		if checkIsUnknown(chk, row) {
			continue
		}
		ok, err := Evaluate(chk, row)
		if err != nil {
			return fmt.Errorf("%w: CHECK %s: %v", ErrExec, chk.String(), err)
		}
		if !ok {
			return fmt.Errorf("%w: CHECK %s failed", ErrCheckViolation, chk.String())
		}
	}
	return nil
}

// enforceForeignKeys validates every foreign key of schema against the row:
// each non-NULL child value must exist in the parent's primary index.
func enforceForeignKeys(ctx context.Context, catalog *Catalog, schema *TableSchema, row Row, lookup rowLookup) error {
	for _, fk := range schema.ForeignKeys {
		v, ok := row[fk.Column]
		if !ok || isNull(v) {
			continue
		}
		parent, ok := catalog.Table(fk.RefTable)
		if !ok {
			return fmt.Errorf("%w: foreign key on %q references unknown table %q", ErrExec, fk.Column, fk.RefTable)
		}
		pk, ok := parent.PrimaryIndex()
		if !ok {
			return fmt.Errorf("%w: referenced table %q has no primary index", ErrExec, fk.RefTable)
		}
		pcol, _ := parent.Column(fk.RefColumn)
		key := NormalizeValue(v, pcol.Type)
		exists, err := lookup(ctx, fk.RefTable, pk.Name, key)
		if err != nil {
			return fmt.Errorf("%w: foreign key lookup on %q: %v", ErrExec, fk.RefTable, err)
		}
		if !exists {
			return fmt.Errorf("%w: %s.%s = %v has no matching row in %s.%s", ErrForeignKeyViolation, schema.Name, fk.Column, v, fk.RefTable, fk.RefColumn)
		}
	}
	return nil
}

// enforceRowIntegrity runs the CHECK and FOREIGN KEY validations a row must
// satisfy before being written.
func enforceRowIntegrity(ctx context.Context, catalog *Catalog, schema *TableSchema, row Row, lookup rowLookup) error {
	if err := enforceChecks(schema, row); err != nil {
		return err
	}
	return enforceForeignKeys(ctx, catalog, schema, row, lookup)
}

// enforceDeleteRestrict rejects deleting parent rows whose primary keys are
// still referenced by a child row (RESTRICT semantics). For a self-referencing
// table, child rows that are themselves being deleted do not block the delete.
func enforceDeleteRestrict(ctx context.Context, catalog *Catalog, schema *TableSchema, keys []types.Comparable, scan tableScan) error {
	if len(keys) == 0 {
		return nil
	}
	refs := catalog.referencingForeignKeys(schema.Name)
	if len(refs) == 0 {
		return nil
	}
	deleted := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		deleted[comparableKey(k)] = struct{}{}
	}
	pk, _ := schema.PrimaryIndex()
	pcol, _ := schema.Column(pk.Column)

	for _, ref := range refs {
		rows, err := scan(ctx, ref.child)
		if err != nil {
			return err
		}
		childPK, _ := ref.child.PrimaryIndex()
		for _, row := range rows {
			v, ok := row[ref.fk.Column]
			if !ok || isNull(v) {
				continue
			}
			if _, hit := deleted[comparableKey(NormalizeValue(v, pcol.Type))]; !hit {
				continue
			}
			if ref.child.Name == schema.Name {
				// Self-reference: a child row deleted by this same statement
				// does not keep its parent alive.
				if _, gone := deleted[comparableKey(row[childPK.Column])]; gone {
					continue
				}
			}
			return fmt.Errorf("%w: row in %q is referenced by %s.%s", ErrForeignKeyViolation, schema.Name, ref.child.Name, ref.fk.Column)
		}
	}
	return nil
}

// comparableKey renders a typed value as a map key including its concrete
// type, so values that print alike but differ in type are not conflated.
func comparableKey(v types.Comparable) string {
	return fmt.Sprintf("%T:%v", v, v)
}

// fkLookup returns a rowLookup reading the engine's committed snapshot.
func (e *Executor) fkLookup() rowLookup {
	return func(ctx context.Context, table, index string, key types.Comparable) (bool, error) {
		_, ok, err := e.engine.GetBytes(ctx, table, index, key)
		return ok, err
	}
}

// fkLookup returns a rowLookup reading through the write transaction, so rows
// staged earlier in the same transaction satisfy foreign keys.
func (t *Tx) fkLookup() rowLookup {
	return func(ctx context.Context, table, index string, key types.Comparable) (bool, error) {
		_, ok, err := t.wtx.GetBytes(ctx, table, index, key)
		return ok, err
	}
}

// integrityScan returns a tableScan over the engine's committed snapshot.
func (e *Executor) integrityScan() tableScan {
	return func(ctx context.Context, schema *TableSchema) ([]Row, error) {
		pk, ok := schema.PrimaryIndex()
		if !ok {
			return nil, fmt.Errorf("%w: table %q has no primary index", ErrExec, schema.Name)
		}
		it, err := e.engine.NewIterator(ctx, schema.Name, pk.Name, storage.IterOptions{})
		if err != nil {
			return nil, fmt.Errorf("%w: open iterator: %v", ErrExec, err)
		}
		defer func() { _ = it.Close() }()
		var rows []Row
		for it.Next() {
			row, err := decodeRow(e.codec, schema, "", it.Value())
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
		if err := it.Err(); err != nil {
			return nil, fmt.Errorf("%w: scan: %v", ErrExec, err)
		}
		return rows, nil
	}
}

// integrityScan returns a tableScan through the write transaction.
func (t *Tx) integrityScan() tableScan {
	return func(ctx context.Context, schema *TableSchema) ([]Row, error) {
		return t.scanRows(ctx, schema, "", nil)
	}
}
