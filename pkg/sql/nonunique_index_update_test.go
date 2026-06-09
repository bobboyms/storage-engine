package sql

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestNonUniqueIndexSurvivesRepeatedNonIndexedUpdates reproduces the bug where a
// non-unique secondary index "loses" sibling rows after repeated UPDATEs that
// touch only a non-indexed column. The full table scan still sees every row,
// but the index lookup degrades without any restart.
//
// The schema mirrors the report: a UUID primary key, a UNIQUE slug, and a
// non-unique UUID secondary index whose three rows share the same value.
func TestNonUniqueIndexSurvivesRepeatedNonIndexedUpdates(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabase(ctx, dir)
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE environments (id UUID PRIMARY KEY, slug VARCHAR, owner_operator_id UUID INDEX, token_delivery VARCHAR, updated_at VARCHAR, UNIQUE (slug))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	const owner = "11111111-1111-1111-1111-111111111111"
	ids := []string{
		"aaaaaaaa-0000-0000-0000-000000000001",
		"bbbbbbbb-0000-0000-0000-000000000002",
		"cccccccc-0000-0000-0000-000000000003",
	}
	const rows = 3
	for i, id := range ids {
		stmt := fmt.Sprintf("INSERT INTO environments (id, slug, owner_operator_id, token_delivery, updated_at) VALUES ('%s', 'slug-%d', '%s', 'header', 't0')", id, i, owner)
		if _, err := db.Exec(ctx, stmt); err != nil {
			t.Fatalf("INSERT %s: %v", id, err)
		}
	}

	countByIndex := func() int {
		rs, qerr := db.Query(ctx, fmt.Sprintf("SELECT id FROM environments WHERE owner_operator_id = '%s'", owner))
		if qerr != nil {
			t.Fatalf("index query: %v", qerr)
		}
		return len(rs.Rows)
	}
	countByScan := func() int {
		rs, qerr := db.Query(ctx, "SELECT id FROM environments")
		if qerr != nil {
			t.Fatalf("scan query: %v", qerr)
		}
		return len(rs.Rows)
	}

	if got := countByIndex(); got != rows {
		t.Fatalf("baseline index lookup = %d rows, want %d", got, rows)
	}

	// Hammer one row with updates to non-indexed columns, like a user toggling
	// token_delivery many times in quick succession.
	for cycle := range 50 {
		mode := "header"
		if cycle%2 == 1 {
			mode = "cookie"
		}
		stmt := fmt.Sprintf("UPDATE environments SET token_delivery = '%s', updated_at = 't%d' WHERE id = '%s'", mode, cycle+1, ids[0])
		if _, err := db.Exec(ctx, stmt); err != nil {
			t.Fatalf("UPDATE cycle %d: %v", cycle, err)
		}

		// Background maintenance (vacuum + fuzzy checkpoint) runs on a timer in a
		// live database; drive it deterministically here.
		if _, err := db.RunMaintenance(ctx); err != nil {
			t.Fatalf("RunMaintenance cycle %d: %v", cycle, err)
		}

		scan := countByScan()
		idx := countByIndex()
		if idx != scan {
			t.Fatalf("after update cycle %d: index lookup = %d rows, full scan = %d rows", cycle, idx, scan)
		}
		if idx != rows {
			t.Fatalf("after update cycle %d: index lookup = %d rows, want %d", cycle, idx, rows)
		}
	}
}

// TestNonUniqueIndexSurvivesUpdatesWithTDE repeats the repro with Transparent
// Data Encryption enabled (the report's configuration) plus a reopen, to cover
// the encrypted index-page round-trip through checkpoint/eviction and recovery.
func TestNonUniqueIndexSurvivesUpdatesWithTDE(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	opts := OpenOptions{
		MaintenanceInterval: time.Millisecond,
		Encryption:          &EncryptionOptions{MasterKey: testMasterKey},
	}

	db, err := OpenDatabaseWithOptions(ctx, dir, opts)
	if err != nil {
		t.Fatalf("OpenDatabaseWithOptions: %v", err)
	}

	if _, err := db.Exec(ctx, "CREATE TABLE environments (id UUID PRIMARY KEY, slug VARCHAR, owner_operator_id UUID INDEX, token_delivery VARCHAR, updated_at VARCHAR, UNIQUE (slug))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	const owner = "11111111-1111-1111-1111-111111111111"
	ids := []string{
		"aaaaaaaa-0000-0000-0000-000000000001",
		"bbbbbbbb-0000-0000-0000-000000000002",
		"cccccccc-0000-0000-0000-000000000003",
	}
	const rows = 3
	for i, id := range ids {
		stmt := fmt.Sprintf("INSERT INTO environments (id, slug, owner_operator_id, token_delivery, updated_at) VALUES ('%s', 'slug-%d', '%s', 'header', 't0')", id, i, owner)
		if _, err := db.Exec(ctx, stmt); err != nil {
			t.Fatalf("INSERT %s: %v", id, err)
		}
	}

	countByIndex := func(d *Executor) int {
		rs, qerr := d.Query(ctx, fmt.Sprintf("SELECT id FROM environments WHERE owner_operator_id = '%s'", owner))
		if qerr != nil {
			t.Fatalf("index query: %v", qerr)
		}
		return len(rs.Rows)
	}

	if got := countByIndex(db); got != rows {
		t.Fatalf("baseline index lookup = %d rows, want %d", got, rows)
	}

	for cycle := range 80 {
		mode := "header"
		if cycle%2 == 1 {
			mode = "cookie"
		}
		stmt := fmt.Sprintf("UPDATE environments SET token_delivery = '%s', updated_at = 't%d' WHERE id = '%s'", mode, cycle+1, ids[0])
		if _, err := db.Exec(ctx, stmt); err != nil {
			t.Fatalf("UPDATE cycle %d: %v", cycle, err)
		}
		if _, err := db.RunMaintenance(ctx); err != nil {
			t.Fatalf("RunMaintenance cycle %d: %v", cycle, err)
		}
		if got := countByIndex(db); got != rows {
			t.Fatalf("after update cycle %d (TDE): index lookup = %d rows, want %d", cycle, got, rows)
		}
	}

	// Reopen ("restart"): the index must still hold all rows.
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db2, err := OpenDatabaseWithOptions(ctx, dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	if got := countByIndex(db2); got != rows {
		t.Fatalf("after reopen (TDE): index lookup = %d rows, want %d", got, rows)
	}
}
