package sql

import (
	"context"
	"fmt"
	"testing"
)

// TestDeleteOneDuplicateKeepsSiblingsAndOtherTables reproduces the report that a
// DELETE of one row in a non-unique secondary-index group makes the index lookup
// return 0 instead of N-1, and corrupts an unrelated table's index in the same
// process. Restarting the process reads everything back correctly, so the defect
// (if present) is in the in-memory index state during the write.
func TestDeleteOneDuplicateKeepsSiblingsAndOtherTables(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabase(ctx, dir)
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE environments (id UUID PRIMARY KEY, slug VARCHAR, owner_operator_id UUID INDEX, UNIQUE (slug))"); err != nil {
		t.Fatalf("CREATE environments: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE operators (id UUID PRIMARY KEY, email VARCHAR INDEX)"); err != nil {
		t.Fatalf("CREATE operators: %v", err)
	}

	const owner = "11111111-1111-1111-1111-111111111111"
	ids := []string{
		"aaaaaaaa-0000-0000-0000-000000000001",
		"bbbbbbbb-0000-0000-0000-000000000002",
		"cccccccc-0000-0000-0000-000000000003",
	}
	for i, id := range ids {
		stmt := fmt.Sprintf("INSERT INTO environments (id, slug, owner_operator_id) VALUES ('%s', 'slug-%d', '%s')", id, i, owner)
		if _, err := db.Exec(ctx, stmt); err != nil {
			t.Fatalf("INSERT env %s: %v", id, err)
		}
	}

	const opID = "99999999-0000-0000-0000-000000000009"
	const email = "operator@example.com"
	if _, err := db.Exec(ctx, fmt.Sprintf("INSERT INTO operators (id, email) VALUES ('%s', '%s')", opID, email)); err != nil {
		t.Fatalf("INSERT operator: %v", err)
	}

	countEnvByOwner := func() int {
		rs, qerr := db.Query(ctx, fmt.Sprintf("SELECT id FROM environments WHERE owner_operator_id = '%s'", owner))
		if qerr != nil {
			t.Fatalf("env index query: %v", qerr)
		}
		return len(rs.Rows)
	}
	countOpByEmail := func() int {
		rs, qerr := db.Query(ctx, fmt.Sprintf("SELECT id FROM operators WHERE email = '%s'", email))
		if qerr != nil {
			t.Fatalf("operator index query: %v", qerr)
		}
		return len(rs.Rows)
	}

	if got := countEnvByOwner(); got != 3 {
		t.Fatalf("baseline env-by-owner = %d, want 3", got)
	}
	if got := countOpByEmail(); got != 1 {
		t.Fatalf("baseline operator-by-email = %d, want 1", got)
	}

	// Delete one of the three duplicate-key rows by primary key.
	if _, err := db.Exec(ctx, fmt.Sprintf("DELETE FROM environments WHERE id = '%s'", ids[0])); err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	if got := countEnvByOwner(); got != 2 {
		t.Fatalf("after DELETE: env-by-owner index = %d rows, want 2 (siblings must survive)", got)
	}
	if got := countOpByEmail(); got != 1 {
		t.Fatalf("after DELETE: unrelated operators-by-email index = %d rows, want 1 (cross-table corruption)", got)
	}
}

// TestDeleteThenReinsertSamePrimaryKey covers the user-facing flow: a row is
// deleted by primary key, then a new row with the same primary key is inserted.
// The DELETE frees the key, so the INSERT must succeed and the new row must be
// the one returned.
func TestDeleteThenReinsertSamePrimaryKey(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabase(ctx, dir)
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE environments (id UUID PRIMARY KEY, slug VARCHAR, owner_operator_id UUID INDEX, UNIQUE (slug))"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	const id = "aaaaaaaa-0000-0000-0000-000000000001"
	const owner = "11111111-1111-1111-1111-111111111111"
	if _, err := db.Exec(ctx, fmt.Sprintf("INSERT INTO environments (id, slug, owner_operator_id) VALUES ('%s', 'slug-a', '%s')", id, owner)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.Exec(ctx, fmt.Sprintf("DELETE FROM environments WHERE id = '%s'", id)); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Re-insert the same primary key with a fresh slug.
	if _, err := db.Exec(ctx, fmt.Sprintf("INSERT INTO environments (id, slug, owner_operator_id) VALUES ('%s', 'slug-b', '%s')", id, owner)); err != nil {
		t.Fatalf("re-insert same primary key after delete should succeed: %v", err)
	}
	rs, err := db.Query(ctx, fmt.Sprintf("SELECT slug FROM environments WHERE id = '%s'", id))
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rs.Rows))
	}
	if got := strColumn(t, rs, "slug"); got[0] != "slug-b" {
		t.Fatalf("slug = %q, want slug-b", got[0])
	}
}

// TestInsertLiveDuplicatePrimaryKeyStillRejected guards that the visibility-aware
// duplicate check did not weaken rejection of a genuine live duplicate: inserting
// the same primary key while a visible row exists must still fail.
func TestInsertLiveDuplicatePrimaryKeyStillRejected(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := OpenDatabase(ctx, dir)
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t (id, v) VALUES (1, 'a')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t (id, v) VALUES (1, 'b')"); err == nil {
		t.Fatal("expected duplicate primary key insert to be rejected")
	}
	rs, _ := db.Query(ctx, "SELECT v FROM t WHERE id = 1")
	if got := strColumn(t, rs, "v"); len(got) != 1 || got[0] != "a" {
		t.Fatalf("row after rejected duplicate = %v, want [a]", got)
	}
}
