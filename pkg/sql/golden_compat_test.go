package sql

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestGoldenLegacyCatalogOpens loads a catalog written in the original
// pre-envelope format (a frozen fixture) with the current binary, proving that
// data from an older engine version still opens and stays queryable. If a future
// format change breaks this, regenerate the fixture deliberately and add a
// migration — never edit the frozen file to make the test pass.
func TestGoldenLegacyCatalogOpens(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	golden, err := os.ReadFile(filepath.Join("testdata", "legacy_catalog", "schema.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, schemaFileName), golden, 0o600); err != nil {
		t.Fatalf("seed catalog: %v", err)
	}

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("open legacy database: %v", err)
	}
	defer db.Close()

	schema, ok := db.Describe("users")
	if !ok {
		t.Fatal("users table not loaded from legacy catalog")
	}
	if got := columnNames(schema.Columns); !reflect.DeepEqual(got, []string{"id", "email"}) {
		t.Fatalf("columns = %v, want [id email]", got)
	}
	if _, ok := schema.PrimaryIndex(); !ok {
		t.Fatal("primary index missing after legacy load")
	}
	if _, ok := schema.IndexForColumn("email"); !ok {
		t.Fatal("secondary index on email missing after legacy load")
	}

	// The recovered table is fully usable.
	if _, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 1, "a@x.com"); err != nil {
		t.Fatalf("insert into legacy table: %v", err)
	}
	rs, err := db.Query(ctx, "SELECT id FROM users WHERE email = ?", "a@x.com")
	if err != nil {
		t.Fatalf("query legacy table: %v", err)
	}
	if got := intColumn(t, rs, "id"); len(got) != 1 || got[0] != 1 {
		t.Fatalf("ids = %v, want [1]", got)
	}
}
