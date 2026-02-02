package storage_test

import (
	"testing"
	"time"

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

	se := storage.NewStorageEngine(tableMgr)

	// Insere dados
	se.Put("users", "id", types.IntKey(10), 100)
	se.Put("users", "id", types.IntKey(20), 200)
	se.Put("users", "id", types.IntKey(30), 300)

	// WHERE id = 20
	results, err := se.Scan("users", "id", query.Equal(types.IntKey(20)))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(results) != 1 || results[0] != 200 {
		t.Fatalf("Expected [200], got %v", results)
	}
}

func TestScan_GreaterThan(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "age", Primary: true, Type: storage.TypeInt},
	}, 3)

	se := storage.NewStorageEngine(tableMgr)

	// Insere dados
	se.Put("users", "age", types.IntKey(15), 1)
	se.Put("users", "age", types.IntKey(18), 2)
	se.Put("users", "age", types.IntKey(25), 3)
	se.Put("users", "age", types.IntKey(30), 4)

	// WHERE age > 18
	results, err := se.Scan("users", "age", query.GreaterThan(types.IntKey(18)))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Deve retornar 25 e 30
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d: %v", len(results), results)
	}
	if results[0] != 3 || results[1] != 4 {
		t.Fatalf("Expected [3 4], got %v", results)
	}
}

func TestScan_GreaterOrEqual(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "age", Primary: true, Type: storage.TypeInt},
	}, 3)

	se := storage.NewStorageEngine(tableMgr)

	se.Put("users", "age", types.IntKey(15), 1)
	se.Put("users", "age", types.IntKey(18), 2)
	se.Put("users", "age", types.IntKey(25), 3)

	// WHERE age >= 18
	results, err := se.Scan("users", "age", query.GreaterOrEqual(types.IntKey(18)))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Deve retornar 18 e 25
	if len(results) != 2 || results[0] != 2 || results[1] != 3 {
		t.Fatalf("Expected [2 3], got %v", results)
	}
}

func TestScan_LessThan(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "age", Primary: true, Type: storage.TypeInt},
	}, 3)

	se := storage.NewStorageEngine(tableMgr)

	se.Put("users", "age", types.IntKey(15), 1)
	se.Put("users", "age", types.IntKey(18), 2)
	se.Put("users", "age", types.IntKey(25), 3)

	// WHERE age < 18
	results, err := se.Scan("users", "age", query.LessThan(types.IntKey(18)))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Deve retornar apenas 15
	if len(results) != 1 || results[0] != 1 {
		t.Fatalf("Expected [1], got %v", results)
	}
}

func TestScan_LessOrEqual(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "age", Primary: true, Type: storage.TypeInt},
	}, 3)

	se := storage.NewStorageEngine(tableMgr)

	se.Put("users", "age", types.IntKey(15), 1)
	se.Put("users", "age", types.IntKey(18), 2)
	se.Put("users", "age", types.IntKey(25), 3)

	// WHERE age <= 18
	results, err := se.Scan("users", "age", query.LessOrEqual(types.IntKey(18)))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Deve retornar 15 e 18
	if len(results) != 2 || results[0] != 1 || results[1] != 2 {
		t.Fatalf("Expected [1 2], got %v", results)
	}
}

func TestScan_Between(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "age", Primary: true, Type: storage.TypeInt},
	}, 3)

	se := storage.NewStorageEngine(tableMgr)

	se.Put("users", "age", types.IntKey(15), 1)
	se.Put("users", "age", types.IntKey(18), 2)
	se.Put("users", "age", types.IntKey(25), 3)
	se.Put("users", "age", types.IntKey(30), 4)

	// WHERE age BETWEEN 18 AND 25
	results, err := se.Scan("users", "age", query.Between(types.IntKey(18), types.IntKey(25)))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Deve retornar 18 e 25
	if len(results) != 2 || results[0] != 2 || results[1] != 3 {
		t.Fatalf("Expected [2 3], got %v", results)
	}
}

func TestScan_NotEqual(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "status", Primary: true, Type: storage.TypeInt},
	}, 3)

	se := storage.NewStorageEngine(tableMgr)

	se.Put("users", "status", types.IntKey(1), 10)
	se.Put("users", "status", types.IntKey(2), 20)
	se.Put("users", "status", types.IntKey(3), 30)

	// WHERE status != 2
	results, err := se.Scan("users", "status", query.NotEqual(types.IntKey(2)))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Deve retornar 1 e 3
	if len(results) != 2 || results[0] != 10 || results[1] != 30 {
		t.Fatalf("Expected [10 30], got %v", results)
	}
}

func TestScan_WithVarchar(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("products", []storage.Index{
		{Name: "name", Primary: true, Type: storage.TypeVarchar},
	}, 3)

	se := storage.NewStorageEngine(tableMgr)

	se.Put("products", "name", types.VarcharKey("apple"), 1)
	se.Put("products", "name", types.VarcharKey("banana"), 2)
	se.Put("products", "name", types.VarcharKey("cherry"), 3)
	se.Put("products", "name", types.VarcharKey("date"), 4)

	// WHERE name >= 'banana' AND name <= 'cherry'
	results, err := se.Scan("products", "name", query.Between(types.VarcharKey("banana"), types.VarcharKey("cherry")))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Deve retornar banana e cherry
	if len(results) != 2 || results[0] != 2 || results[1] != 3 {
		t.Fatalf("Expected [2 3], got %v", results)
	}
}

func TestScan_WithDate(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("events", []storage.Index{
		{Name: "date", Primary: true, Type: storage.TypeDate},
	}, 3)

	se := storage.NewStorageEngine(tableMgr)

	jan1 := types.DateKey(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	jan15 := types.DateKey(time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC))
	jan31 := types.DateKey(time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC))
	feb1 := types.DateKey(time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC))

	se.Put("events", "date", jan1, 1)
	se.Put("events", "date", jan15, 2)
	se.Put("events", "date", jan31, 3)
	se.Put("events", "date", feb1, 4)

	// WHERE date >= '2025-01-15' AND date <= '2025-01-31'
	results, err := se.Scan("events", "date", query.Between(jan15, jan31))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Deve retornar jan15 e jan31
	if len(results) != 2 || results[0] != 2 || results[1] != 3 {
		t.Fatalf("Expected [2 3], got %v", results)
	}
}

func TestRangeScan_BackwardCompatibility(t *testing.T) {
	tableMgr := storage.NewTableMenager()
	tableMgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)

	se := storage.NewStorageEngine(tableMgr)

	se.Put("users", "id", types.IntKey(10), 100)
	se.Put("users", "id", types.IntKey(20), 200)
	se.Put("users", "id", types.IntKey(30), 300)

	// Teste do método legado RangeScan
	results, err := se.RangeScan("users", "id", types.IntKey(10), types.IntKey(30))
	if err != nil {
		t.Fatalf("RangeScan failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}
}
