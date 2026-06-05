package sql

import "fmt"

// catalogMigration upgrades the persisted catalog from one format version to the
// next. apply receives the tables decoded at version `from` and returns them in
// the `to` representation. Migrations must be deterministic and idempotent in
// effect, since a catalog that is not re-stamped is migrated again on each open.
type catalogMigration struct {
	from  int
	to    int
	apply func([]persistedTable) ([]persistedTable, error)
}

// catalogMigrations is the ordered, contiguous chain of catalog upgrades, one
// per format version bump. It is empty while CatalogFormatVersion is 1; when the
// layout changes, bump CatalogFormatVersion and append the {from, to} step that
// rewrites old data into the new shape.
var catalogMigrations []catalogMigration

// migrateCatalog upgrades tables decoded at version `from` up to version `to`,
// applying the registered migrations in version order. It is a no-op when
// already at the target, errors when no step continues the chain, and refuses a
// version newer than the target (which an older build cannot understand).
func migrateCatalog(tables []persistedTable, from, to int, migrations []catalogMigration) ([]persistedTable, error) {
	if from == to {
		return tables, nil
	}
	if from > to {
		return nil, fmt.Errorf("%w: data is v%d, newer than supported v%d", ErrUnsupportedCatalogVersion, from, to)
	}
	for cur := from; cur < to; {
		m, ok := findCatalogMigration(migrations, cur)
		if !ok {
			return nil, fmt.Errorf("sql: no catalog migration from v%d toward v%d", cur, to)
		}
		next, err := m.apply(tables)
		if err != nil {
			return nil, fmt.Errorf("sql: catalog migration v%d->v%d: %w", m.from, m.to, err)
		}
		tables = next
		cur = m.to
	}
	return tables, nil
}

func findCatalogMigration(migrations []catalogMigration, from int) (catalogMigration, bool) {
	for _, m := range migrations {
		if m.from == from {
			return m, true
		}
	}
	return catalogMigration{}, false
}
