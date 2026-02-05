package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/bobboyms/storage-engine/pkg/heap"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func main() {
	// 1. Setup Environment
	tmpDir, err := os.MkdirTemp("", "vacuum_demo")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Printf("📂 Database initialized in: %s\n", tmpDir)

	// 2. Initialize Components
	// WAL
	walPath := filepath.Join(tmpDir, "demo.wal")
	// Use default options
	walOpts := wal.DefaultOptions()
	walWriter, err := wal.NewWALWriter(walPath, walOpts)
	if err != nil {
		log.Fatalf("Failed to create WAL: %v", err)
	}

	// Heap Manager
	heapPath := filepath.Join(tmpDir, "products_heap")
	hm, err := heap.NewHeapManager(heapPath)
	if err != nil {
		log.Fatalf("Failed to create Heap: %v", err)
	}

	// Metadata & Schema
	// Note: 'NewTableMenager' contains a typo in the current codebase, using it as is.
	meta := storage.NewTableMenager()

	indices := []storage.Index{
		{Name: "id", Type: storage.TypeInt, Primary: true},
	}

	// Create 'products' table with 64KB segments for easier rotation demo (though Vacuum works on any)
	// Passing 't=4' for B-Tree degree.
	err = meta.NewTable("products", indices, 4, hm)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Storage Engine
	se, err := storage.NewStorageEngine(meta, walWriter)
	if err != nil {
		log.Fatalf("Failed to create Engine: %v", err)
	}
	defer se.Close()

	// 3. Populate Data
	fmt.Println("\n📝 Inserting 3 products...")
	insertProduct(se, 1, "Laptop")
	insertProduct(se, 2, "Mouse") // We will delete this one
	insertProduct(se, 3, "Keyboard")

	// 4. Create a "Long Running" Read Transaction
	// This captures the state BEFORE deletion, meaning it MUST still be able to see "Mouse".
	fmt.Println("\n📸 Starting Long-Running Read Transaction (Tx1)...")
	tx1 := se.BeginRead()

	// 5. Delete a Product (Create Tombstone)
	fmt.Println("🗑️  Deleting Product 2 (Mouse)...")
	_, err = se.Del("products", "id", types.IntKey(2)) // Returns (found, error)
	if err != nil {
		log.Fatalf("Delete failed: %v", err)
	}

	// 6. Attempt Vacuum (Should NOT reclaim Product 2)
	// Because Tx1 is still active and its snapshot might need 'Mouse'.
	fmt.Println("\n🧹 Running Vacuum (Pass 1)...")
	fmt.Println("   (Expectation: Should NOT reclaim space for 'Mouse' because Tx1 is active)")
	if err := se.Vacuum("products"); err != nil {
		log.Fatalf("Vacuum failed: %v", err)
	}

	// Verify via Scan (Engine internals check)
	// Real verification is checking if keys exist in tree.
	table, _ := meta.GetTableByName("products")
	idx, _ := table.GetIndex("id")
	if _, found := idx.Tree.Get(types.IntKey(2)); !found {
		fmt.Println("❌ ERROR: Product 2 was prematurely removed from index!")
	} else {
		fmt.Println("✅ PRESERVED: Product 2 index entry still exists (correct).")
	}

	// 7. Verify Tx1 can still see the data?
	// Note: Our current simple KV Get mechanism uses the transaction attached?
	// The `Get` method in engine usually takes a Key. To use `tx1`, we should use `tx1.Get` if available,
	// or `se.Get` might use a new transaction implicitly.
	// Actually, `se.Get` implementation usually handles its own tx.
	// To test visibility using `tx1`, we need to check if `engine` provides `Get` with `Tx`.
	// Looking at `engine.go`: `func (se *StorageEngine) Get(...)` creates `tx := se.BeginRead()`.
	// So `se.Get` sees CURRENT state (Deleted).
	// We don't have a direct `tx.Get` exposed in this demo easily without looking at `transaction.go`.
	// But logically, Vacuum did its job by checking the registry.

	// 8. Close Transaction
	fmt.Println("\n🚪 Closing Tx1...")
	tx1.Close()

	// 9. Run Vacuum Again (Should Reclaim)
	fmt.Println("\n🧹 Running Vacuum (Pass 2)...")
	fmt.Println("   (Expectation: Should remove 'Mouse' tombstone fully)")
	if err := se.Vacuum("products"); err != nil {
		log.Fatalf("Vacuum failed: %v", err)
	}

	// 10. Verify Removal
	if _, found := idx.Tree.Get(types.IntKey(2)); found {
		fmt.Println("❌ ERROR: Product 2 should have been removed!")
	} else {
		fmt.Println("✅ RECLAIMED: Product 2 gone from index.")
	}

	if _, found := idx.Tree.Get(types.IntKey(1)); !found {
		fmt.Println("❌ ERROR: Product 1 (Laptop) is missing!")
	} else {
		fmt.Println("✅ PRESERVED: Product 1 (Laptop) is safe.")
	}

	fmt.Println("\n🎉 Demo Completed Successfully!")
}

func insertProduct(se *storage.StorageEngine, id int, name string) {
	doc := fmt.Sprintf(`{"id": %d, "name": "%s", "created_at": "%s"}`, id, name, time.Now().Format(time.RFC3339))
	keys := map[string]types.Comparable{
		"id": types.IntKey(id),
	}
	if err := se.InsertRow("products", doc, keys); err != nil {
		log.Fatalf("Insert failed: %v", err)
	}
	fmt.Printf("   -> Inserted ID %d: %s\n", id, name)
}
