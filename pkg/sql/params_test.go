package sql

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func openParamsDB(t *testing.T) (*Executor, context.Context) {
	t.Helper()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, t.TempDir(), OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	return db, ctx
}

func TestLexPlaceholder(t *testing.T) {
	toks, err := Lex("?")
	if err != nil {
		t.Fatalf("Lex: %v", err)
	}
	if toks[0].Type != TokenPlaceholder {
		t.Fatalf("token type = %v, want TokenPlaceholder", toks[0].Type)
	}
}

func TestExecInsertWithPlaceholders(t *testing.T) {
	db, ctx := openParamsDB(t)

	n, err := db.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)", 1, "alice", 30)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if n != 1 {
		t.Fatalf("affected = %d, want 1", n)
	}

	rs, err := db.Query(ctx, "SELECT id, name, age FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if got := intColumn(t, rs, "id"); len(got) != 1 || got[0] != 1 {
		t.Fatalf("id = %v, want [1]", got)
	}
	if got := strColumn(t, rs, "name"); got[0] != "alice" {
		t.Fatalf("name = %v, want alice", got)
	}
	if got := intColumn(t, rs, "age"); got[0] != 30 {
		t.Fatalf("age = %v, want 30", got)
	}
}

func TestPlaceholderPreventsInjection(t *testing.T) {
	db, ctx := openParamsDB(t)

	// A classic injection payload must be stored verbatim as data, not parsed.
	payload := "x'); DROP TABLE users; --"
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)", 1, payload, 0); err != nil {
		t.Fatalf("INSERT payload: %v", err)
	}

	rs, err := db.Query(ctx, "SELECT name FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if got := strColumn(t, rs, "name"); got[0] != payload {
		t.Fatalf("name = %q, want %q", got[0], payload)
	}
	// The table must still exist and be queryable.
	if _, err := db.Query(ctx, "SELECT id FROM users"); err != nil {
		t.Fatalf("table no longer queryable: %v", err)
	}
}

func TestPlaceholderArgCountMismatch(t *testing.T) {
	db, ctx := openParamsDB(t)

	_, err := db.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)", 1, "alice")
	if err == nil {
		t.Fatal("expected error for too few arguments")
	}
	if !errors.Is(err, ErrBind) {
		t.Fatalf("expected ErrBind, got %v", err)
	}
}

func TestPlaceholderNullArg(t *testing.T) {
	db, ctx := openParamsDB(t)

	// A nil argument must bind exactly like a literal NULL: insert one row each
	// way and assert their stored name values are identical.
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)", 1, nil, 30); err != nil {
		t.Fatalf("INSERT with nil arg: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (2, NULL, 30)"); err != nil {
		t.Fatalf("INSERT with literal NULL: %v", err)
	}

	rs, err := db.Query(ctx, "SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	idIdx, nameIdx := colIndex(rs, "id"), colIndex(rs, "name")
	names := map[int64]any{}
	for _, row := range rs.Rows {
		names[int64(row[idIdx].(types.IntKey))] = row[nameIdx]
	}
	if !reflect.DeepEqual(names[1], names[2]) {
		t.Fatalf("nil arg name %#v != literal NULL name %#v", names[1], names[2])
	}
}

func TestPlaceholderUpdateAndDelete(t *testing.T) {
	db, ctx := openParamsDB(t)
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)", 1, "alice", 30); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	if _, err := db.Exec(ctx, "UPDATE users SET age = ? WHERE id = ?", 31, 1); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	rs, _ := db.Query(ctx, "SELECT age FROM users WHERE id = ?", 1)
	if got := intColumn(t, rs, "age"); got[0] != 31 {
		t.Fatalf("age = %v, want 31", got)
	}

	if _, err := db.Exec(ctx, "DELETE FROM users WHERE id = ?", 1); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	rs, _ = db.Query(ctx, "SELECT id FROM users")
	if len(rs.Rows) != 0 {
		t.Fatalf("rows = %d, want 0 after delete", len(rs.Rows))
	}
}

func TestPlaceholderArgTypes(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, t.TempDir(), OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY, active BOOL, score FLOAT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// int64, bool, and float64 args.
	if _, err := db.Exec(ctx, "INSERT INTO t (id, active, score) VALUES (?, ?, ?)", int64(1), true, 9.5); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	// int32 and float32 args bind to the same literal kinds.
	if _, err := db.Exec(ctx, "INSERT INTO t (id, active, score) VALUES (?, ?, ?)", int32(2), false, float32(1.5)); err != nil {
		t.Fatalf("INSERT int32/float32: %v", err)
	}

	rs, err := db.Query(ctx, "SELECT id, active, score FROM t WHERE id = ?", 1)
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	row := rs.Rows[0]
	if got := bool(row[colIndex(rs, "active")].(types.BoolKey)); got != true {
		t.Fatalf("active = %v, want true", got)
	}
	if got := float64(row[colIndex(rs, "score")].(types.FloatKey)); got != 9.5 {
		t.Fatalf("score = %v, want 9.5", got)
	}
}

func TestPlaceholderUnsupportedArgType(t *testing.T) {
	db, ctx := openParamsDB(t)
	_, err := db.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)", 1, []string{"x"}, 30)
	if err == nil {
		t.Fatal("expected error for unsupported argument type")
	}
	if !errors.Is(err, ErrBind) {
		t.Fatalf("expected ErrBind, got %v", err)
	}
}

func TestPlaceholderInWhereListAndRange(t *testing.T) {
	db, ctx := openParamsDB(t)
	for i, name := range []string{"a", "b", "c"} {
		if _, err := db.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)", i+1, name, (i+1)*10); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}

	// Placeholders inside an IN list (desugars to OR'd comparisons).
	rs, err := db.Query(ctx, "SELECT id FROM users WHERE id IN (?, ?)", 1, 3)
	if err != nil {
		t.Fatalf("IN query: %v", err)
	}
	if got := intColumn(t, rs, "id"); len(got) != 2 {
		t.Fatalf("IN ids = %v, want 2 rows", got)
	}

	// Placeholders as BETWEEN bounds (desugars to >= AND <=).
	rs, err = db.Query(ctx, "SELECT id FROM users WHERE age BETWEEN ? AND ?", 15, 25)
	if err != nil {
		t.Fatalf("BETWEEN query: %v", err)
	}
	if got := intColumn(t, rs, "id"); len(got) != 1 || got[0] != 2 {
		t.Fatalf("BETWEEN ids = %v, want [2]", got)
	}
}

func TestPlaceholderInSubquery(t *testing.T) {
	db, ctx := openParamsDB(t)
	for i, name := range []string{"a", "b", "c"} {
		if _, err := db.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)", i+1, name, (i+1)*10); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}

	// A placeholder inside a predicate subquery exercises the recursive bind
	// walk into nested SELECTs.
	rs, err := db.Query(ctx,
		"SELECT id FROM users WHERE id IN (SELECT id FROM users WHERE age > ?)", 15)
	if err != nil {
		t.Fatalf("subquery query: %v", err)
	}
	if got := intColumn(t, rs, "id"); len(got) != 2 {
		t.Fatalf("subquery ids = %v, want 2 rows (age 20 and 30)", got)
	}
}

func TestPlaceholderInTransaction(t *testing.T) {
	db, ctx := openParamsDB(t)
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)", 1, "alice", 30); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	tx := db.Begin()
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "UPDATE users SET age = ? WHERE id = ?", 99, 1); err != nil {
		t.Fatalf("tx UPDATE: %v", err)
	}
	rs, err := tx.Query(ctx, "SELECT age FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatalf("tx SELECT: %v", err)
	}
	if got := intColumn(t, rs, "age"); got[0] != 99 {
		t.Fatalf("age = %v, want 99", got)
	}
}
