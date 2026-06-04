// Command sql_ddl demonstrates CREATE TABLE and schema persistence: a database
// is opened on a directory, a table is created and populated entirely via SQL
// (no Go table setup), then the database is closed and reopened on the same
// directory to show that the schema and data come back automatically.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/bobboyms/storage-engine/pkg/sql"
)

func main() {
	dir, err := os.MkdirTemp("", "sql-ddl-example-*")
	if err != nil {
		log.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)
	ctx := context.Background()

	// --- First open: create the table and insert data via SQL only. ---
	db, err := sql.OpenDatabase(ctx, dir)
	if err != nil {
		log.Fatalf("open: %v", err)
	}

	mustExec(ctx, db, "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR, category VARCHAR INDEX, price FLOAT)")
	mustExec(ctx, db, "INSERT INTO products (id, name, category, price) VALUES (1, 'pen', 'office', 1.5)")
	mustExec(ctx, db, "INSERT INTO products (id, name, category, price) VALUES (2, 'desk', 'furniture', 199.0)")
	mustExec(ctx, db, "INSERT INTO products (id, name, category, price) VALUES (3, 'stapler', 'office', 9.0)")

	fmt.Println("=== After CREATE TABLE + INSERT ===")
	show(ctx, db, "SELECT id, name, price FROM products ORDER BY id")
	show(ctx, db, "SELECT category, COUNT(*) AS n FROM products GROUP BY category ORDER BY category")

	if err := db.Close(); err != nil {
		log.Fatalf("close: %v", err)
	}
	fmt.Printf("\nschema file written: %s/schema.json\n", dir)

	// --- Reopen the same directory: no Go setup, schema/data restored. ---
	db2, err := sql.OpenDatabase(ctx, dir)
	if err != nil {
		log.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	fmt.Println("\n=== After reopening the database ===")
	// The secondary index on category, declared in CREATE TABLE, is restored.
	show(ctx, db2, "SELECT name FROM products WHERE category = 'office' ORDER BY name")
}

func mustExec(ctx context.Context, db *sql.Executor, query string) {
	if _, err := db.Exec(ctx, query); err != nil {
		log.Fatalf("exec %q: %v", query, err)
	}
}

func show(ctx context.Context, db *sql.Executor, query string) {
	rs, err := db.Query(ctx, query)
	if err != nil {
		fmt.Printf("\n> %s\n  ERROR: %v\n", query, err)
		return
	}
	fmt.Printf("\n> %s\n", query)
	for _, col := range rs.Columns {
		fmt.Printf("%-12s", col)
	}
	fmt.Println()
	for _, row := range rs.Rows {
		for _, v := range row {
			fmt.Printf("%-12v", v)
		}
		fmt.Println()
	}
}
