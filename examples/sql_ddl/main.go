// Command sql_ddl demonstrates CREATE TABLE and schema persistence: a database
// is opened on a directory, a table is created and populated entirely via SQL
// (no Go table setup), then the database is closed and reopened on the same
// directory to show that the schema and data come back automatically. It also
// shows scheduled/on-demand maintenance (checkpoint + temp-file cleanup).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

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
	// Background maintenance (a write-gated checkpoint + temp-file cleanup)
	// runs automatically on the given interval; here we set it explicitly.
	db, err := sql.OpenDatabaseWithOptions(ctx, dir, sql.OpenOptions{MaintenanceInterval: time.Minute})
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

	maintenanceDemo(ctx, db, dir)

	// Close stops scheduled maintenance and runs a final checkpoint that
	// compacts the WAL before releasing the engine.
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

// maintenanceDemo runs an on-demand maintenance pass. To make the temp-file
// cleanup visible, it first drops a stale orphan ".tmp" file (as an interrupted
// atomic write would leave behind) and ages it past the safety threshold.
func maintenanceDemo(ctx context.Context, db *sql.Executor, dir string) {
	fmt.Println("\n=== Maintenance (checkpoint + temp-file cleanup) ===")
	orphan := filepath.Join(dir, "products.heap.tmp")
	if err := os.WriteFile(orphan, []byte("leftover"), 0o600); err != nil {
		log.Fatalf("write orphan: %v", err)
	}
	old := time.Now().Add(-2 * time.Hour)
	_ = os.Chtimes(orphan, old, old)

	removed, err := db.RunMaintenance(ctx)
	if err != nil {
		log.Fatalf("maintenance: %v", err)
	}
	fmt.Printf("RunMaintenance: checkpoint done, %d orphan temp file(s) removed\n", removed)
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
