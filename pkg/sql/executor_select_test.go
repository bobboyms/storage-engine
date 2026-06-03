package sql

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/codec/bsoncodec"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// execTestRow mirrors the seed data inserted into the users table.
type execTestRow struct {
	id   int
	name string
	age  int
}

var execSeed = []execTestRow{
	{1, "alice", 30},
	{2, "bob", 25},
	{3, "carol", 40},
	{4, "dave", 30},
}

func newExecutor(t *testing.T) *Executor {
	t.Helper()
	tmp := t.TempDir()
	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, filepath.Join(tmp, "heap.data"))
	if err != nil {
		t.Fatalf("heap: %v", err)
	}
	tm := storage.NewTableMenager()
	if err := tm.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
		{Name: "age", Type: storage.TypeInt},
	}, 3, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	ww, err := wal.NewWALWriter(filepath.Join(tmp, "wal.log"), wal.DefaultOptions())
	if err != nil {
		t.Fatalf("wal: %v", err)
	}
	se, err := storage.NewStorageEngine(tm, ww)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	t.Cleanup(func() { _ = se.Close() })

	for _, r := range execSeed {
		doc := fmt.Sprintf(`{"id":%d,"name":%q,"age":%d}`, r.id, r.name, r.age)
		if err := se.InsertRow(context.Background(), "users", doc, map[string]types.Comparable{
			"id":  types.IntKey(r.id),
			"age": types.IntKey(r.age),
		}); err != nil {
			t.Fatalf("InsertRow %d: %v", r.id, err)
		}
	}

	cat := NewCatalog()
	if err := cat.AddTable(TableSchema{
		Name: "users",
		Columns: []Column{
			{Name: "id", Type: storage.TypeInt},
			{Name: "name", Type: storage.TypeVarchar},
			{Name: "age", Type: storage.TypeInt},
		},
		Indexes: []IndexDef{
			{Name: "id", Column: "id", Primary: true},
			{Name: "age", Column: "age"},
		},
	}); err != nil {
		t.Fatalf("AddTable: %v", err)
	}

	return NewExecutor(se, cat, bsoncodec.New())
}

func colIndex(rs *ResultSet, name string) int {
	for i, c := range rs.Columns {
		if c == name {
			return i
		}
	}
	return -1
}

func intColumn(t *testing.T, rs *ResultSet, name string) []int64 {
	t.Helper()
	idx := colIndex(rs, name)
	if idx < 0 {
		t.Fatalf("column %q not in result %v", name, rs.Columns)
	}
	out := make([]int64, len(rs.Rows))
	for i, row := range rs.Rows {
		v, ok := row[idx].(types.IntKey)
		if !ok {
			t.Fatalf("row %d column %q = %T, want IntKey", i, name, row[idx])
		}
		out[i] = int64(v)
	}
	return out
}

func strColumn(t *testing.T, rs *ResultSet, name string) []string {
	t.Helper()
	idx := colIndex(rs, name)
	if idx < 0 {
		t.Fatalf("column %q not in result %v", name, rs.Columns)
	}
	out := make([]string, len(rs.Rows))
	for i, row := range rs.Rows {
		v, ok := row[idx].(types.VarcharKey)
		if !ok {
			t.Fatalf("row %d column %q = %T, want VarcharKey", i, name, row[idx])
		}
		out[i] = string(v)
	}
	return out
}

func TestQueryPointLookup(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT id, name FROM users WHERE id = 2")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rs.Rows))
	}
	if got := strColumn(t, rs, "name"); got[0] != "bob" {
		t.Fatalf("name = %q, want bob", got[0])
	}
}

func TestQuerySelectStarColumns(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT * FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	want := []string{"id", "name", "age"}
	if len(rs.Columns) != len(want) {
		t.Fatalf("columns = %v, want %v", rs.Columns, want)
	}
	for i, c := range want {
		if rs.Columns[i] != c {
			t.Fatalf("columns = %v, want %v", rs.Columns, want)
		}
	}
}

func TestQueryRangeOrdered(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT id, age FROM users WHERE age >= 30 ORDER BY age")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	ages := intColumn(t, rs, "age")
	want := []int64{30, 30, 40}
	if len(ages) != len(want) {
		t.Fatalf("ages = %v, want %v", ages, want)
	}
	for i := range want {
		if ages[i] != want[i] {
			t.Fatalf("ages = %v, want ascending %v", ages, want)
		}
	}
}

func TestQueryResidualFilter(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT id FROM users WHERE age = 30 AND name = 'dave'")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	ids := intColumn(t, rs, "id")
	if len(ids) != 1 || ids[0] != 4 {
		t.Fatalf("ids = %v, want [4]", ids)
	}
}

func TestQueryOrderByDesc(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT age FROM users ORDER BY age DESC")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	ages := intColumn(t, rs, "age")
	want := []int64{40, 30, 30, 25}
	for i := range want {
		if ages[i] != want[i] {
			t.Fatalf("ages = %v, want descending %v", ages, want)
		}
	}
}

func TestQueryLimitOffset(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 1")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	ids := intColumn(t, rs, "id")
	want := []int64{2, 3}
	if len(ids) != len(want) {
		t.Fatalf("ids = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("ids = %v, want %v", ids, want)
		}
	}
}

func TestQueryErrors(t *testing.T) {
	e := newExecutor(t)
	// Unknown table.
	if _, err := e.Query(context.Background(), "SELECT * FROM ghosts"); err == nil {
		t.Fatal("expected error for unknown table")
	}
	// Not a SELECT.
	if _, err := e.Query(context.Background(), "DELETE FROM users"); err == nil {
		t.Fatal("expected error for non-SELECT in Query")
	}
}
