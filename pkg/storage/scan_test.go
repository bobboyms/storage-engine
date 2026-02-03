package storage_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/heap"
	"github.com/bobboyms/storage-engine/pkg/query"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// =============================================
// TESTES PARA SCAN COM OPERADORES
// =============================================

func TestScan_Equal(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	err := tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))
	se, _ := storage.NewStorageEngine(tableMgr, "", hm)

	// Insere dados
	se.Put("users", "id", types.IntKey(10), "user_10")
	se.Put("users", "id", types.IntKey(20), "user_20")
	se.Put("users", "id", types.IntKey(30), "user_30")

	// WHERE id = 20
	results, err := se.Scan("users", "id", query.Equal(types.IntKey(20)))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(results) != 1 || results[0] != "user_20" {
		t.Fatalf("Expected [user_20], got %v", results)
	}
}

func TestScan_GreaterThan(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "age", Primary: true, Type: storage.TypeInt},
	}, 3)

	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))
	se, _ := storage.NewStorageEngine(tableMgr, "", hm)

	// Insere dados
	se.Put("users", "age", types.IntKey(15), "age_15")
	se.Put("users", "age", types.IntKey(18), "age_18")
	se.Put("users", "age", types.IntKey(25), "age_25")
	se.Put("users", "age", types.IntKey(30), "age_30")

	// WHERE age > 18
	results, err := se.Scan("users", "age", query.GreaterThan(types.IntKey(18)))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Deve retornar 25 e 30
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d: %v", len(results), results)
	}
	if results[0] != "age_25" || results[1] != "age_30" {
		t.Fatalf("Expected [age_25 age_30], got %v", results)
	}
}

func TestScan_GreaterOrEqual(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "age", Primary: true, Type: storage.TypeInt},
	}, 3)

	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))
	se, _ := storage.NewStorageEngine(tableMgr, "", hm)

	se.Put("users", "age", types.IntKey(15), "age_15")
	se.Put("users", "age", types.IntKey(18), "age_18")
	se.Put("users", "age", types.IntKey(25), "age_25")

	// WHERE age >= 18
	results, err := se.Scan("users", "age", query.GreaterOrEqual(types.IntKey(18)))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Deve retornar 18 e 25
	if len(results) != 2 || results[0] != "age_18" || results[1] != "age_25" {
		t.Fatalf("Expected [age_18 age_25], got %v", results)
	}
}

func TestScan_LessThan(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "age", Primary: true, Type: storage.TypeInt},
	}, 3)

	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))
	se, _ := storage.NewStorageEngine(tableMgr, "", hm)

	se.Put("users", "age", types.IntKey(15), "age_15")
	se.Put("users", "age", types.IntKey(18), "age_18")
	se.Put("users", "age", types.IntKey(25), "age_25")

	// WHERE age < 18
	results, err := se.Scan("users", "age", query.LessThan(types.IntKey(18)))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Deve retornar apenas 15
	if len(results) != 1 || results[0] != "age_15" {
		t.Fatalf("Expected [age_15], got %v", results)
	}
}

func TestScan_LessOrEqual(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "age", Primary: true, Type: storage.TypeInt},
	}, 3)

	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))
	se, _ := storage.NewStorageEngine(tableMgr, "", hm)

	se.Put("users", "age", types.IntKey(15), "age_15")
	se.Put("users", "age", types.IntKey(18), "age_18")
	se.Put("users", "age", types.IntKey(25), "age_25")

	// WHERE age <= 18
	results, err := se.Scan("users", "age", query.LessOrEqual(types.IntKey(18)))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Deve retornar 15 e 18
	if len(results) != 2 || results[0] != "age_15" || results[1] != "age_18" {
		t.Fatalf("Expected [age_15 age_18], got %v", results)
	}
}

func TestScan_Between(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "age", Primary: true, Type: storage.TypeInt},
	}, 3)

	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))
	se, _ := storage.NewStorageEngine(tableMgr, "", hm)

	se.Put("users", "age", types.IntKey(15), "age_15")
	se.Put("users", "age", types.IntKey(18), "age_18")
	se.Put("users", "age", types.IntKey(25), "age_25")
	se.Put("users", "age", types.IntKey(30), "age_30")

	// WHERE age BETWEEN 18 AND 25
	results, err := se.Scan("users", "age", query.Between(types.IntKey(18), types.IntKey(25)))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Deve retornar 18 e 25
	if len(results) != 2 || results[0] != "age_18" || results[1] != "age_25" {
		t.Fatalf("Expected [age_18 age_25], got %v", results)
	}
}

func TestScan_NotEqual(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "status", Primary: true, Type: storage.TypeInt},
	}, 3)

	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))
	se, _ := storage.NewStorageEngine(tableMgr, "", hm)

	se.Put("users", "status", types.IntKey(1), "status_10")
	se.Put("users", "status", types.IntKey(2), "status_20")
	se.Put("users", "status", types.IntKey(3), "status_30")

	// WHERE status != 2
	results, err := se.Scan("users", "status", query.NotEqual(types.IntKey(2)))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Deve retornar 1 e 3
	if len(results) != 2 || results[0] != "status_10" || results[1] != "status_30" {
		t.Fatalf("Expected [status_10 status_30], got %v", results)
	}
}

func TestScan_WithVarchar(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("products", []storage.Index{
		{Name: "name", Primary: true, Type: storage.TypeVarchar},
	}, 3)

	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))
	se, _ := storage.NewStorageEngine(tableMgr, "", hm)

	se.Put("products", "name", types.VarcharKey("apple"), "id_1")
	se.Put("products", "name", types.VarcharKey("banana"), "id_2")
	se.Put("products", "name", types.VarcharKey("cherry"), "id_3")
	se.Put("products", "name", types.VarcharKey("date"), "id_4")

	// WHERE name >= 'banana' AND name <= 'cherry'
	results, err := se.Scan("products", "name", query.Between(types.VarcharKey("banana"), types.VarcharKey("cherry")))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Deve retornar banana e cherry
	if len(results) != 2 || results[0] != "id_2" || results[1] != "id_3" {
		t.Fatalf("Expected [id_2 id_3], got %v", results)
	}
}

func TestScan_WithDate(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("events", []storage.Index{
		{Name: "date", Primary: true, Type: storage.TypeDate},
	}, 3)

	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))
	se, _ := storage.NewStorageEngine(tableMgr, "", hm)

	jan1 := types.DateKey(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	jan15 := types.DateKey(time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC))
	jan31 := types.DateKey(time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC))
	feb1 := types.DateKey(time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC))

	se.Put("events", "date", jan1, "event_1")
	se.Put("events", "date", jan15, "event_2")
	se.Put("events", "date", jan31, "event_3")
	se.Put("events", "date", feb1, "event_4")

	// WHERE date >= '2025-01-15' AND date <= '2025-01-31'
	results, err := se.Scan("events", "date", query.Between(jan15, jan31))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Deve retornar jan15 e jan31
	if len(results) != 2 || results[0] != "event_2" || results[1] != "event_3" {
		t.Fatalf("Expected [event_2 event_3], got %v", results)
	}
}

func TestRangeScan_BackwardCompatibility(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)

	tmpDir := t.TempDir()
	hm, _ := heap.NewHeapManager(filepath.Join(tmpDir, "heap.data"))
	se, _ := storage.NewStorageEngine(tableMgr, "", hm)

	se.Put("users", "id", types.IntKey(10), "user_100")
	se.Put("users", "id", types.IntKey(20), "user_200")
	se.Put("users", "id", types.IntKey(30), "user_300")

	// Teste do método legado RangeScan
	results, err := se.RangeScan("users", "id", types.IntKey(10), types.IntKey(30))
	if err != nil {
		t.Fatalf("RangeScan failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}
}
