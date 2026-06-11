package sql

import (
	"context"
	"fmt"
	"testing"
)

// TestMaintenanceAfterReopen_WriteKeepsOtherTablesVisible reproduces the
// consumer-reported bug: on a reopened database, a DELETE followed by an idle
// maintenance pass (vacuum + checkpoint) poisoned the WAL with a MaxUint64
// LSN. The next open adopted it as the current LSN and the first write
// wrapped the counter to 0, making every previously committed row in every
// table invisible until the database was reopened again.
func TestMaintenanceAfterReopen_WriteKeepsOtherTablesVisible(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	open := func() *Executor {
		db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
		if err != nil {
			t.Fatalf("open database: %v", err)
		}
		return db
	}
	countRows := func(db *Executor, table string) int {
		t.Helper()
		rs, err := db.Query(ctx, "SELECT id FROM "+table)
		if err != nil {
			t.Fatalf("select from %s: %v", table, err)
		}
		return len(rs.Rows)
	}

	// First process: create the schema and some rows.
	db := open()
	for _, stmt := range []string{
		"CREATE TABLE operators (id VARCHAR PRIMARY KEY, name VARCHAR)",
		"CREATE TABLE api_keys (id VARCHAR PRIMARY KEY, name VARCHAR)",
		"CREATE TABLE refresh_tokens (id VARCHAR PRIMARY KEY, name VARCHAR)",
	} {
		if _, err := db.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	for i := 0; i < 5; i++ {
		for _, table := range []string{"operators", "refresh_tokens"} {
			q := fmt.Sprintf("INSERT INTO %s (id, name) VALUES ('%s-%d', 'x')", table, table, i)
			if _, err := db.Exec(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Restart: tables now load from the persisted schema, arming the engine's
	// page-redo flush hooks. A token cleanup DELETE marks garbage, then a
	// maintenance pass runs while the database is otherwise idle.
	db = open()
	if _, err := db.Exec(ctx, "DELETE FROM refresh_tokens WHERE id = 'refresh_tokens-0'"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := db.RunMaintenance(ctx); err != nil {
		t.Fatalf("maintenance: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close after maintenance: %v", err)
	}

	// Next restart: a single write must not hide the other tables.
	db = open()
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(ctx, "INSERT INTO api_keys (id, name) VALUES ('ak-1', 'k')"); err != nil {
		t.Fatalf("insert after reopen: %v", err)
	}
	if got := countRows(db, "operators"); got != 5 {
		t.Fatalf("operators has %d rows after a write to api_keys, want 5", got)
	}
	if got := countRows(db, "refresh_tokens"); got != 4 {
		t.Fatalf("refresh_tokens has %d rows, want 4", got)
	}
	if got := countRows(db, "api_keys"); got != 1 {
		t.Fatalf("api_keys has %d rows, want 1", got)
	}
}
