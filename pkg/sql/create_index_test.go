package sql

import (
	"context"
	"errors"
	"testing"
)

func TestParseCreateIndex(t *testing.T) {
	stmt, err := Parse("CREATE INDEX ON users (age)")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ci, ok := stmt.(*CreateIndexStmt)
	if !ok {
		t.Fatalf("stmt = %T, want *CreateIndexStmt", stmt)
	}
	if ci.Table != "users" || ci.Name != "" || len(ci.Columns) != 1 || ci.Columns[0] != "age" || ci.Unique || ci.IfNotExists {
		t.Fatalf("got %+v", ci)
	}

	stmt, err = Parse("CREATE UNIQUE INDEX IF NOT EXISTS idx_email ON users (email)")
	if err != nil {
		t.Fatalf("Parse unique: %v", err)
	}
	ci = stmt.(*CreateIndexStmt)
	if !ci.Unique || !ci.IfNotExists || ci.Name != "idx_email" || ci.Columns[0] != "email" {
		t.Fatalf("got %+v", ci)
	}

	stmt, err = Parse("CREATE INDEX ON users (city, age)")
	if err != nil {
		t.Fatalf("Parse composite: %v", err)
	}
	ci = stmt.(*CreateIndexStmt)
	if len(ci.Columns) != 2 || ci.Columns[0] != "city" || ci.Columns[1] != "age" {
		t.Fatalf("got %+v", ci)
	}

	if _, err := Parse("CREATE INDEX ON users ()"); !errors.Is(err, ErrParse) {
		t.Fatalf("empty columns err = %v, want ErrParse", err)
	}

	stmt, err = Parse("DROP INDEX IF EXISTS age ON users")
	if err != nil {
		t.Fatalf("Parse drop index: %v", err)
	}
	di := stmt.(*DropIndexStmt)
	if di.Table != "users" || di.Name != "age" || !di.IfExists {
		t.Fatalf("got %+v", di)
	}
}

func TestCreateIndexBackfillsExistingRows(t *testing.T) {
	db, ctx := openConstraintDB(t)
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, age INT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, age) VALUES (1, 30), (2, 40), (3, 30)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	if _, err := db.Exec(ctx, "CREATE INDEX ON users (age)"); err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	// The catalog now knows the index, so the planner will use it for this
	// equality predicate; pre-existing rows must be reachable through it.
	schema, _ := db.catalog.Table("users")
	if _, ok := schema.IndexForColumn("age"); !ok {
		t.Fatal("catalog has no index on age after CREATE INDEX")
	}
	rs, err := db.Query(ctx, "SELECT id FROM users WHERE age = 30 ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT via index: %v", err)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("rows = %d, want 2 (backfill missing?)", len(rs.Rows))
	}

	// Rows written after the index exists are maintained too.
	if _, err := db.Exec(ctx, "INSERT INTO users (id, age) VALUES (4, 30)"); err != nil {
		t.Fatalf("INSERT after index: %v", err)
	}
	rs, _ = db.Query(ctx, "SELECT id FROM users WHERE age = 30")
	if len(rs.Rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rs.Rows))
	}
}

func TestCreateUniqueIndexRejectsExistingDuplicates(t *testing.T) {
	db, ctx := openConstraintDB(t)
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (1, 'a@b.c'), (2, 'a@b.c')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	if _, err := db.Exec(ctx, "CREATE UNIQUE INDEX ON users (email)"); err == nil {
		t.Fatal("CREATE UNIQUE INDEX over duplicate data succeeded, want error")
	}

	// The failed creation must leave no trace: catalog unchanged and no
	// phantom uniqueness enforced.
	schema, _ := db.catalog.Table("users")
	if _, ok := schema.IndexForColumn("email"); ok {
		t.Fatal("catalog still lists the email index after failed CREATE UNIQUE INDEX")
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (3, 'a@b.c')"); err != nil {
		t.Fatalf("INSERT after failed index: %v", err)
	}
}

func TestCreateUniqueIndexEnforcesAfterwards(t *testing.T) {
	db, ctx := openConstraintDB(t)
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (1, 'a@b.c')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE UNIQUE INDEX ON users (email)"); err != nil {
		t.Fatalf("CREATE UNIQUE INDEX: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (2, 'a@b.c')"); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("duplicate insert err = %v, want ErrUniqueViolation", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (2, 'x@y.z')"); err != nil {
		t.Fatalf("distinct insert: %v", err)
	}
}

func TestCreateIndexIfNotExistsAndErrors(t *testing.T) {
	db, ctx := openConstraintDB(t)
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, age INT INDEX)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	// age is already indexed (same derived name): plain create errors, the
	// guarded form is a no-op.
	if _, err := db.Exec(ctx, "CREATE INDEX ON users (age)"); !errors.Is(err, ErrExec) {
		t.Fatalf("duplicate index err = %v, want ErrExec", err)
	}
	if _, err := db.Exec(ctx, "CREATE INDEX IF NOT EXISTS ON users (age)"); err != nil {
		t.Fatalf("IF NOT EXISTS err = %v, want nil", err)
	}

	if _, err := db.Exec(ctx, "CREATE INDEX ON missing (age)"); !errors.Is(err, ErrExec) {
		t.Fatalf("unknown table err = %v, want ErrExec", err)
	}
	if _, err := db.Exec(ctx, "CREATE INDEX ON users (nope)"); !errors.Is(err, ErrExec) {
		t.Fatalf("unknown column err = %v, want ErrExec", err)
	}
}

func TestCreateIndexSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, age INT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, age) VALUES (1, 30), (2, 40)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE INDEX ON users (age)"); err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	schema, _ := db2.catalog.Table("users")
	if _, ok := schema.IndexForColumn("age"); !ok {
		t.Fatal("index on age lost after reopen")
	}
	rs, err := db2.Query(ctx, "SELECT id FROM users WHERE age = 30")
	if err != nil {
		t.Fatalf("SELECT after reopen: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rs.Rows))
	}
	if _, err := db2.Exec(ctx, "INSERT INTO users (id, age) VALUES (3, 30)"); err != nil {
		t.Fatalf("INSERT after reopen: %v", err)
	}
	rs, _ = db2.Query(ctx, "SELECT id FROM users WHERE age = 30")
	if len(rs.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rs.Rows))
	}
}

func TestCreateCompositeIndexOnExistingTable(t *testing.T) {
	db, ctx := openConstraintDB(t)
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, city VARCHAR, age INT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, city, age) VALUES (1, 'porto', 30), (2, 'porto', 40), (3, 'faro', 30)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE INDEX ON users (city, age)"); err != nil {
		t.Fatalf("CREATE composite INDEX: %v", err)
	}
	rs, err := db.Query(ctx, "SELECT id FROM users WHERE city = 'porto' AND age = 40")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rs.Rows))
	}
}

func TestDropIndex(t *testing.T) {
	db, ctx := openConstraintDB(t)
	if _, err := db.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, age INT INDEX)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, age) VALUES (1, 30)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	if _, err := db.Exec(ctx, "DROP INDEX age ON users"); err != nil {
		t.Fatalf("DROP INDEX: %v", err)
	}
	schema, _ := db.catalog.Table("users")
	if _, ok := schema.IndexForColumn("age"); ok {
		t.Fatal("catalog still lists the age index after DROP INDEX")
	}
	// The table remains fully queryable without the index.
	rs, err := db.Query(ctx, "SELECT id FROM users WHERE age = 30")
	if err != nil {
		t.Fatalf("SELECT after drop: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rs.Rows))
	}

	if _, err := db.Exec(ctx, "DROP INDEX age ON users"); !errors.Is(err, ErrExec) {
		t.Fatalf("dropping missing index err = %v, want ErrExec", err)
	}
	if _, err := db.Exec(ctx, "DROP INDEX IF EXISTS age ON users"); err != nil {
		t.Fatalf("DROP INDEX IF EXISTS err = %v, want nil", err)
	}
	// The primary index backs the table and cannot be dropped.
	if _, err := db.Exec(ctx, "DROP INDEX id ON users"); !errors.Is(err, ErrExec) {
		t.Fatalf("dropping primary err = %v, want ErrExec", err)
	}
}
