package sql

import (
	"context"
	"testing"
)

func TestAutoIncrementAssignsSequentialIDs(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE logs (id INT PRIMARY KEY AUTO_INCREMENT, msg VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	for _, msg := range []string{"a", "b", "c"} {
		if _, err := db.Exec(ctx, "INSERT INTO logs (msg) VALUES (?)", msg); err != nil {
			t.Fatalf("INSERT %q: %v", msg, err)
		}
	}
	rs, err := db.Query(ctx, "SELECT id, msg FROM logs ORDER BY id")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	ids := intColumn(t, rs, "id")
	if len(ids) != 3 || ids[0] != 1 || ids[1] != 2 || ids[2] != 3 {
		t.Fatalf("ids = %v, want [1 2 3]", ids)
	}
}

func TestAutoIncrementExplicitValueBumpsCounter(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE logs (id INT PRIMARY KEY AUTO_INCREMENT, msg VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO logs (id, msg) VALUES (10, 'manual')"); err != nil {
		t.Fatalf("explicit INSERT: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO logs (msg) VALUES ('auto')"); err != nil {
		t.Fatalf("auto INSERT: %v", err)
	}
	rs, err := db.Query(ctx, "SELECT id FROM logs WHERE msg = 'auto'")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := intColumn(t, rs, "id"); got[0] != 11 {
		t.Fatalf("auto id after explicit 10 = %d, want 11", got[0])
	}
}

func TestAutoIncrementNullValueAllocates(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE logs (id INT PRIMARY KEY AUTO_INCREMENT, msg VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO logs (id, msg) VALUES (NULL, 'x')"); err != nil {
		t.Fatalf("INSERT NULL id: %v", err)
	}
	rs, err := db.Query(ctx, "SELECT id FROM logs WHERE msg = 'x'")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := intColumn(t, rs, "id"); got[0] != 1 {
		t.Fatalf("id = %d, want 1", got[0])
	}
}

func TestAutoIncrementMultiRowInsert(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE logs (id INT PRIMARY KEY AUTO_INCREMENT, msg VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO logs (msg) VALUES ('a'), ('b')"); err != nil {
		t.Fatalf("multi-row INSERT: %v", err)
	}
	rs, err := db.Query(ctx, "SELECT id FROM logs ORDER BY id")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	ids := intColumn(t, rs, "id")
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("ids = %v, want [1 2]", ids)
	}
}

func TestAutoIncrementInTransaction(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE logs (id INT PRIMARY KEY AUTO_INCREMENT, msg VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	tx := db.Begin()
	if _, err := tx.Exec(ctx, "INSERT INTO logs (msg) VALUES ('tx')"); err != nil {
		t.Fatalf("tx INSERT: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	rs, err := db.Query(ctx, "SELECT id FROM logs WHERE msg = 'tx'")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := intColumn(t, rs, "id"); got[0] != 1 {
		t.Fatalf("id = %d, want 1", got[0])
	}
}

func TestAutoIncrementResumesAfterReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE logs (id INT PRIMARY KEY AUTO_INCREMENT, msg VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO logs (msg) VALUES ('one'), ('two')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db2.Close() }()
	if _, err := db2.Exec(ctx, "INSERT INTO logs (msg) VALUES ('three')"); err != nil {
		t.Fatalf("INSERT after reopen: %v", err)
	}
	rs, err := db2.Query(ctx, "SELECT id FROM logs WHERE msg = 'three'")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := intColumn(t, rs, "id"); got[0] != 3 {
		t.Fatalf("id after reopen = %d, want 3", got[0])
	}
}

func TestAutoIncrementRequiresIntPrimaryKey(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE bad1 (id VARCHAR PRIMARY KEY AUTO_INCREMENT)"); err == nil {
		t.Fatal("AUTO_INCREMENT on VARCHAR succeeded, want error")
	}
	if _, err := db.Exec(ctx, "CREATE TABLE bad2 (id INT PRIMARY KEY, n INT AUTO_INCREMENT)"); err == nil {
		t.Fatal("AUTO_INCREMENT on non-primary column succeeded, want error")
	}
}

func TestAutoIncrementPersistsInSchema(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE logs (id INT PRIMARY KEY AUTO_INCREMENT, msg VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	schemas, err := loadSchemas(dir)
	if err != nil {
		t.Fatalf("loadSchemas: %v", err)
	}
	if len(schemas) != 1 {
		t.Fatalf("schemas = %d, want 1", len(schemas))
	}
	col, ok := schemas[0].Column("id")
	if !ok || !col.AutoIncrement {
		t.Fatalf("persisted id column = %+v, want AutoIncrement=true", col)
	}
}
