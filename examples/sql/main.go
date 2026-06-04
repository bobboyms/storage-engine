// Command sql demonstrates the SQL layer (pkg/sql) built on top of the storage
// engine: a database opened on a directory, schema defined with CREATE TABLE,
// SELECT with the supported operators, aggregates and GROUP BY/HAVING,
// INNER/LEFT JOINs, FROM and predicate subqueries, DML, and an explicit
// transaction with read-your-writes.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/bobboyms/storage-engine/pkg/sql"
)

func main() {
	dir, err := os.MkdirTemp("", "sql-example-*")
	if err != nil {
		log.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()
	exec, err := sql.OpenDatabase(ctx, dir)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer exec.Close()

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

// seed defines the schema with CREATE TABLE and inserts the sample rows, all
// through SQL — no Go table setup is needed.
func seed(ctx context.Context, exec *sql.Executor) {
	stmts := []string{
		"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT INDEX)",
		"CREATE TABLE orders (id INT PRIMARY KEY, user_id INT INDEX, amount INT)",
		"INSERT INTO users (id, name, age) VALUES (1, 'alice', 30)",
		"INSERT INTO users (id, name, age) VALUES (2, 'bob', 25)",
		"INSERT INTO users (id, name, age) VALUES (3, 'carol', 40)",
		"INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100)",
		"INSERT INTO orders (id, user_id, amount) VALUES (2, 1, 50)",
		"INSERT INTO orders (id, user_id, amount) VALUES (3, 2, 200)",
	}
	for _, q := range stmts {
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
