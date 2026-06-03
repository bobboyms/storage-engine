package sql

import (
	"errors"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func plannerSchema() *TableSchema {
	c := NewCatalog()
	_ = c.AddTable(sampleSchema()) // users(id pk, name, age idx_age)
	ts, _ := c.Table("users")
	return ts
}

func planQuery(t *testing.T, query string) *QueryPlan {
	t.Helper()
	sel := parseSelect(t, query)
	plan, err := Plan(sel, plannerSchema())
	if err != nil {
		t.Fatalf("Plan(%q) error: %v", query, err)
	}
	return plan
}

func TestPlanPrimaryEquality(t *testing.T) {
	p := planQuery(t, "SELECT * FROM users WHERE id = 5")
	if p.IndexName != "pk_users" || p.Column != "id" {
		t.Fatalf("index = %q on %q, want pk_users on id", p.IndexName, p.Column)
	}
	if p.Lower != types.IntKey(5) || p.Upper != types.IntKey(5) {
		t.Fatalf("bounds = [%v, %v], want [5, 5]", p.Lower, p.Upper)
	}
	if p.Residual == nil {
		t.Fatal("Residual should be the WHERE expression")
	}
}

func TestPlanSecondaryRange(t *testing.T) {
	p := planQuery(t, "SELECT * FROM users WHERE age >= 18 AND age <= 30")
	if p.IndexName != "idx_age" || p.Column != "age" {
		t.Fatalf("index = %q on %q, want idx_age on age", p.IndexName, p.Column)
	}
	if p.Lower != types.IntKey(18) || p.Upper != types.IntKey(30) {
		t.Fatalf("bounds = [%v, %v], want [18, 30]", p.Lower, p.Upper)
	}
}

func TestPlanExclusiveLowerKeepsInclusiveBound(t *testing.T) {
	p := planQuery(t, "SELECT * FROM users WHERE age > 18")
	if p.IndexName != "idx_age" {
		t.Fatalf("index = %q, want idx_age", p.IndexName)
	}
	// Inclusive bound at 18; the residual predicate excludes age == 18.
	if p.Lower != types.IntKey(18) {
		t.Fatalf("Lower = %v, want inclusive 18", p.Lower)
	}
	if p.Upper != nil {
		t.Fatalf("Upper = %v, want nil", p.Upper)
	}
}

func TestPlanNoWhereFullPrimaryScan(t *testing.T) {
	p := planQuery(t, "SELECT * FROM users")
	if p.IndexName != "pk_users" {
		t.Fatalf("index = %q, want pk_users", p.IndexName)
	}
	if p.Lower != nil || p.Upper != nil {
		t.Fatalf("bounds = [%v, %v], want unbounded", p.Lower, p.Upper)
	}
	if p.Residual != nil {
		t.Fatalf("Residual = %v, want nil", p.Residual)
	}
}

func TestPlanNonIndexedColumnFallsBackToPrimary(t *testing.T) {
	p := planQuery(t, "SELECT * FROM users WHERE name = 'bob'")
	if p.IndexName != "pk_users" {
		t.Fatalf("index = %q, want pk_users (full scan)", p.IndexName)
	}
	if p.Lower != nil || p.Upper != nil {
		t.Fatalf("bounds = [%v, %v], want unbounded", p.Lower, p.Upper)
	}
	if p.Residual == nil {
		t.Fatal("Residual should carry the name predicate")
	}
}

func TestPlanOrderByIndexedAscNoSort(t *testing.T) {
	p := planQuery(t, "SELECT * FROM users ORDER BY age")
	if p.IndexName != "idx_age" {
		t.Fatalf("index = %q, want idx_age to satisfy ordering", p.IndexName)
	}
	if p.NeedsSort {
		t.Fatal("NeedsSort = true, want false (scan already ordered)")
	}
}

func TestPlanOrderByDescNeedsSort(t *testing.T) {
	p := planQuery(t, "SELECT * FROM users ORDER BY age DESC")
	if !p.NeedsSort {
		t.Fatal("NeedsSort = false, want true (reverse not supported by scan)")
	}
	if p.Sort == nil || p.Sort.Column != "age" || !p.Sort.Desc {
		t.Fatalf("Sort = %+v, want age DESC", p.Sort)
	}
}

func TestPlanWhereIndexDiffersFromOrderByNeedsSort(t *testing.T) {
	p := planQuery(t, "SELECT * FROM users WHERE id = 5 ORDER BY age")
	if p.IndexName != "pk_users" {
		t.Fatalf("index = %q, want pk_users (driven by WHERE)", p.IndexName)
	}
	if !p.NeedsSort {
		t.Fatal("NeedsSort = false, want true (ordering column differs from scan)")
	}
}

func TestPlanTopLevelOrNoBounds(t *testing.T) {
	p := planQuery(t, "SELECT * FROM users WHERE id = 5 OR age = 9")
	if p.IndexName != "pk_users" {
		t.Fatalf("index = %q, want pk_users full scan", p.IndexName)
	}
	if p.Lower != nil || p.Upper != nil {
		t.Fatalf("bounds = [%v, %v], want unbounded for top-level OR", p.Lower, p.Upper)
	}
}

func TestPlanUnknownColumnError(t *testing.T) {
	sel := parseSelect(t, "SELECT * FROM users WHERE missing = 1")
	if _, err := Plan(sel, plannerSchema()); !errors.Is(err, ErrPlan) {
		t.Fatalf("Plan error = %v, want ErrPlan", err)
	}
}
