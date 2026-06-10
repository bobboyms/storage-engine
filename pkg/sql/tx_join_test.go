package sql

import (
	"context"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// openTxJoinDB creates a database with users and orders tables and some
// committed rows, shared by the transactional JOIN tests.
func openTxJoinDB(t *testing.T) (*Executor, context.Context) {
	t.Helper()
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	stmts := []string{
		"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)",
		"CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total INT)",
		"INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')",
		"INSERT INTO orders (id, user_id, total) VALUES (10, 1, 100)",
	}
	for _, s := range stmts {
		if _, err := db.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	return db, ctx
}

func TestTxJoinSeesStagedWrites(t *testing.T) {
	db, ctx := openTxJoinDB(t)

	tx := db.Begin()
	// Stage a new order inside the transaction; the JOIN must see it before
	// commit (read-your-writes) alongside committed rows.
	if _, err := tx.Exec(ctx, "INSERT INTO orders (id, user_id, total) VALUES (11, 2, 50)"); err != nil {
		t.Fatalf("tx INSERT: %v", err)
	}

	rs, err := tx.Query(ctx, "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.total")
	if err != nil {
		t.Fatalf("tx JOIN query: %v", err)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("tx join rows = %d, want 2", len(rs.Rows))
	}
	names := strColumn(t, rs, "name")
	if names[0] != "bob" || names[1] != "alice" {
		t.Fatalf("names = %v, want [bob alice]", names)
	}

	// Outside the transaction the staged order is invisible.
	rsOut, err := db.Query(ctx, "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id")
	if err != nil {
		t.Fatalf("outside JOIN query: %v", err)
	}
	if len(rsOut.Rows) != 1 {
		t.Fatalf("outside join rows = %d, want 1", len(rsOut.Rows))
	}

	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
}

func TestTxLeftJoinProducesNulls(t *testing.T) {
	db, ctx := openTxJoinDB(t)

	tx := db.Begin()
	defer func() { _ = tx.Rollback(ctx) }()

	rs, err := tx.Query(ctx, "SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id")
	if err != nil {
		t.Fatalf("tx LEFT JOIN: %v", err)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rs.Rows))
	}
	// bob (id 2) has no committed order: total must be NULL.
	totalIdx := -1
	for i, c := range rs.Columns {
		if c == "total" {
			totalIdx = i
		}
	}
	if totalIdx < 0 {
		t.Fatalf("total column missing: %v", rs.Columns)
	}
	if _, ok := rs.Rows[1][totalIdx].(types.NullKey); !ok {
		t.Fatalf("bob total = %T, want NullKey", rs.Rows[1][totalIdx])
	}
}

func TestTxFromSubquerySeesStagedWrites(t *testing.T) {
	db, ctx := openTxJoinDB(t)

	tx := db.Begin()
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, "INSERT INTO users (id, name) VALUES (3, 'carol')"); err != nil {
		t.Fatalf("tx INSERT: %v", err)
	}

	rs, err := tx.Query(ctx, "SELECT name FROM (SELECT name FROM users) u ORDER BY name")
	if err != nil {
		t.Fatalf("tx FROM subquery: %v", err)
	}
	got := strColumn(t, rs, "name")
	want := []string{"alice", "bob", "carol"}
	if len(got) != len(want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestTxJoinWithAggregation(t *testing.T) {
	db, ctx := openTxJoinDB(t)

	tx := db.Begin()
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, "INSERT INTO orders (id, user_id, total) VALUES (12, 1, 40)"); err != nil {
		t.Fatalf("tx INSERT: %v", err)
	}

	rs, err := tx.Query(ctx, "SELECT u.name, SUM(o.total) AS spent FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name")
	if err != nil {
		t.Fatalf("tx JOIN GROUP BY: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("groups = %d, want 1", len(rs.Rows))
	}
	spent := rs.Rows[0][1]
	iv, ok := spent.(types.IntKey)
	if !ok || int64(iv) != 140 {
		t.Fatalf("spent = %v (%T), want 140", spent, spent)
	}
}

func TestTxJoinForUpdateRejected(t *testing.T) {
	db, ctx := openTxJoinDB(t)

	tx := db.Begin()
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Query(ctx, "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id FOR UPDATE"); err == nil {
		t.Fatal("JOIN ... FOR UPDATE inside a transaction succeeded, want error")
	}
}
