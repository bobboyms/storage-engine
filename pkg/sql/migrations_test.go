package sql

import (
	"errors"
	"reflect"
	"testing"
)

func TestMigrateCatalogAppliesStepsInOrder(t *testing.T) {
	var order []string
	migs := []catalogMigration{
		{from: 1, to: 2, apply: func(ts []persistedTable) ([]persistedTable, error) {
			order = append(order, "1->2")
			return ts, nil
		}},
		{from: 2, to: 3, apply: func(ts []persistedTable) ([]persistedTable, error) {
			order = append(order, "2->3")
			return ts, nil
		}},
	}
	if _, err := migrateCatalog(nil, 1, 3, migs); err != nil {
		t.Fatalf("migrateCatalog: %v", err)
	}
	if !reflect.DeepEqual(order, []string{"1->2", "2->3"}) {
		t.Fatalf("order = %v, want [1->2 2->3]", order)
	}
}

func TestMigrateCatalogNoopWhenCurrent(t *testing.T) {
	called := false
	migs := []catalogMigration{{from: 1, to: 2, apply: func(ts []persistedTable) ([]persistedTable, error) {
		called = true
		return ts, nil
	}}}
	out, err := migrateCatalog([]persistedTable{{Name: "x"}}, 2, 2, migs)
	if err != nil {
		t.Fatalf("migrateCatalog: %v", err)
	}
	if called {
		t.Fatal("no migration should run when already at the target version")
	}
	if len(out) != 1 {
		t.Fatalf("tables = %d, want 1", len(out))
	}
}

func TestMigrateCatalogErrorsWithoutPath(t *testing.T) {
	migs := []catalogMigration{{from: 1, to: 2, apply: func(ts []persistedTable) ([]persistedTable, error) {
		return ts, nil
	}}}
	// Target v3 but no v2->v3 step registered.
	if _, err := migrateCatalog(nil, 1, 3, migs); err == nil {
		t.Fatal("expected an error for a missing migration step")
	}
}

func TestMigrateCatalogTransformsData(t *testing.T) {
	migs := []catalogMigration{{from: 1, to: 2, apply: func(ts []persistedTable) ([]persistedTable, error) {
		for i := range ts {
			ts[i].Name = "v2_" + ts[i].Name
		}
		return ts, nil
	}}}
	out, err := migrateCatalog([]persistedTable{{Name: "users"}}, 1, 2, migs)
	if err != nil {
		t.Fatalf("migrateCatalog: %v", err)
	}
	if out[0].Name != "v2_users" {
		t.Fatalf("name = %q, want v2_users", out[0].Name)
	}
}

func TestMigrateCatalogRejectsNewerThanTarget(t *testing.T) {
	if _, err := migrateCatalog(nil, 5, 2, nil); !errors.Is(err, ErrUnsupportedCatalogVersion) {
		t.Fatalf("expected ErrUnsupportedCatalogVersion for from > to, got %v", err)
	}
}
