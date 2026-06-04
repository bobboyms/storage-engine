// Command sql demonstrates the SQL layer (pkg/sql) built on top of the storage
// engine: schema catalog, SELECT with the supported operators, aggregates and
// GROUP BY/HAVING, INNER/LEFT JOINs, FROM and predicate subqueries, DML, and an
// explicit transaction with read-your-writes.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/bobboyms/storage-engine/pkg/codec/bsoncodec"
	"github.com/bobboyms/storage-engine/pkg/sql"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func main() {
	dir, err := os.MkdirTemp("", "sql-example-*")
	if err != nil {
		log.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	engine := setupEngine(dir)
	defer engine.Close()

	catalog := setupCatalog()
	exec := sql.NewExecutor(engine, catalog, bsoncodec.New())
	ctx := context.Background()

	seed(ctx, exec)

	fmt.Println("=== SELECT with WHERE operators ===")
	run(ctx, exec, "SELECT id, name, age FROM users WHERE age >= 30 ORDER BY age")
	run(ctx, exec, "SELECT name FROM users WHERE age BETWEEN 25 AND 35")
	run(ctx, exec, "SELECT name FROM users WHERE name LIKE '%a%' ORDER BY name")
	run(ctx, exec, "SELECT name FROM users WHERE id IN (1, 3)")

	fmt.Println("\n=== Aggregates, GROUP BY, HAVING ===")
	run(ctx, exec, "SELECT COUNT(*), AVG(age) FROM users")
	run(ctx, exec, "SELECT age, COUNT(*) AS n FROM users GROUP BY age ORDER BY age")
	run(ctx, exec, "SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id HAVING SUM(amount) > 100 ORDER BY user_id")

	fmt.Println("\n=== JOINs ===")
	run(ctx, exec, "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.amount")
	run(ctx, exec, "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.name")

	fmt.Println("\n=== Subqueries ===")
	run(ctx, exec, "SELECT s.user_id, s.c FROM (SELECT user_id, COUNT(*) AS c FROM orders GROUP BY user_id) s WHERE s.c >= 2")
	run(ctx, exec, "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders) ORDER BY name")
	run(ctx, exec, "SELECT name FROM users u WHERE NOT EXISTS (SELECT o.id FROM orders o WHERE o.user_id = u.id)")

	fmt.Println("\n=== DML ===")
	exec.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (4, 'dave', 28)")
	exec.Exec(ctx, "UPDATE users SET age = 41 WHERE name = 'carol'")
	exec.Exec(ctx, "DELETE FROM users WHERE id = 2")
	run(ctx, exec, "SELECT id, name, age FROM users ORDER BY id")

	transactionDemo(ctx, exec)
}

// transactionDemo shows an explicit transaction: staged writes are visible to
// reads inside the transaction (read-your-writes) and become durable on Commit.
func transactionDemo(ctx context.Context, exec *sql.Executor) {
	fmt.Println("\n=== Transaction (read-your-writes) ===")
	tx := exec.Begin()
	if _, err := tx.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (5, 'erin', 22)"); err != nil {
		log.Fatalf("tx insert: %v", err)
	}

	inside, _ := tx.Query(ctx, "SELECT name FROM users WHERE id = 5")
	fmt.Printf("inside tx, id=5 visible: %d row(s)\n", len(inside.Rows))

	outside, _ := exec.Query(ctx, "SELECT name FROM users WHERE id = 5")
	fmt.Printf("outside tx (uncommitted), id=5 visible: %d row(s)\n", len(outside.Rows))

	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("commit: %v", err)
	}
	committed, _ := exec.Query(ctx, "SELECT name FROM users WHERE id = 5")
	fmt.Printf("after commit, id=5 visible: %d row(s)\n", len(committed.Rows))
}

func setupEngine(dir string) *storage.StorageEngine {
	usersHeap, err := storage.NewHeapForTable(storage.HeapFormatV2, filepath.Join(dir, "users.heap"))
	if err != nil {
		log.Fatalf("users heap: %v", err)
	}
	ordersHeap, err := storage.NewHeapForTable(storage.HeapFormatV2, filepath.Join(dir, "orders.heap"))
	if err != nil {
		log.Fatalf("orders heap: %v", err)
	}

	tm := storage.NewTableMenager()
	if err := tm.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
		{Name: "age", Type: storage.TypeInt},
	}, 3, usersHeap); err != nil {
		log.Fatalf("create users: %v", err)
	}
	if err := tm.NewTable("orders", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
		{Name: "user_id", Type: storage.TypeInt},
	}, 3, ordersHeap); err != nil {
		log.Fatalf("create orders: %v", err)
	}

	walWriter, err := wal.NewWALWriter(filepath.Join(dir, "data.wal"), wal.DefaultOptions())
	if err != nil {
		log.Fatalf("wal: %v", err)
	}
	engine, err := storage.NewStorageEngine(tm, walWriter)
	if err != nil {
		log.Fatalf("engine: %v", err)
	}
	return engine
}

// setupCatalog describes the tables to the SQL layer so column names and types
// resolve to engine indexes and key types.
func setupCatalog() *sql.Catalog {
	c := sql.NewCatalog()
	must(c.AddTable(sql.TableSchema{
		Name: "users",
		Columns: []sql.Column{
			{Name: "id", Type: storage.TypeInt},
			{Name: "name", Type: storage.TypeVarchar},
			{Name: "age", Type: storage.TypeInt},
		},
		Indexes: []sql.IndexDef{
			{Name: "id", Column: "id", Primary: true},
			{Name: "age", Column: "age"},
		},
	}))
	must(c.AddTable(sql.TableSchema{
		Name: "orders",
		Columns: []sql.Column{
			{Name: "id", Type: storage.TypeInt},
			{Name: "user_id", Type: storage.TypeInt},
			{Name: "amount", Type: storage.TypeInt},
		},
		Indexes: []sql.IndexDef{
			{Name: "id", Column: "id", Primary: true},
			{Name: "user_id", Column: "user_id"},
		},
	}))
	return c
}

func seed(ctx context.Context, exec *sql.Executor) {
	users := []string{
		"INSERT INTO users (id, name, age) VALUES (1, 'alice', 30)",
		"INSERT INTO users (id, name, age) VALUES (2, 'bob', 25)",
		"INSERT INTO users (id, name, age) VALUES (3, 'carol', 40)",
	}
	orders := []string{
		"INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100)",
		"INSERT INTO orders (id, user_id, amount) VALUES (2, 1, 50)",
		"INSERT INTO orders (id, user_id, amount) VALUES (3, 2, 200)",
	}
	for _, q := range append(users, orders...) {
		if _, err := exec.Exec(ctx, q); err != nil {
			log.Fatalf("seed %q: %v", q, err)
		}
	}
}

// run executes a query and prints the result set as a simple table.
func run(ctx context.Context, exec *sql.Executor, query string) {
	rs, err := exec.Query(ctx, query)
	if err != nil {
		fmt.Printf("\n> %s\n  ERROR: %v\n", query, err)
		return
	}
	fmt.Printf("\n> %s\n", query)
	fmt.Printf("  %s\n", strings.Join(rs.Columns, " | "))
	if len(rs.Rows) == 0 {
		fmt.Println("  (no rows)")
		return
	}
	for _, row := range rs.Rows {
		cells := make([]string, len(row))
		for i, v := range row {
			cells[i] = fmt.Sprintf("%v", v)
		}
		fmt.Printf("  %s\n", strings.Join(cells, " | "))
	}
}

func must(err error) {
	if err != nil {
		log.Fatalf("setup: %v", err)
	}
}
