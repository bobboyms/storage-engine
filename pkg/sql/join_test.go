package sql

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/codec/bsoncodec"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// newJoinExecutor builds an executor with two tables:
//
//	users(id PK, name, age)         seeded: 1 alice 30, 2 bob 25, 3 carol 40
//	orders(id PK, user_id, amount)  seeded: 1->u1 100, 2->u1 50, 3->u2 200
func newJoinExecutor(t *testing.T) *Executor {
	t.Helper()
	tmp := t.TempDir()
	usersHeap, err := storage.NewHeapForTable(storage.HeapFormatV2, filepath.Join(tmp, "users.heap"))
	if err != nil {
		t.Fatalf("users heap: %v", err)
	}
	ordersHeap, err := storage.NewHeapForTable(storage.HeapFormatV2, filepath.Join(tmp, "orders.heap"))
	if err != nil {
		t.Fatalf("orders heap: %v", err)
	}
	tm := storage.NewTableMenager()
	if err := tm.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
		{Name: "age", Type: storage.TypeInt},
	}, 3, usersHeap); err != nil {
		t.Fatalf("NewTable users: %v", err)
	}
	if err := tm.NewTable("orders", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
		{Name: "user_id", Type: storage.TypeInt},
	}, 3, ordersHeap); err != nil {
		t.Fatalf("NewTable orders: %v", err)
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

	cat := NewCatalog()
	mustAdd(t, cat, TableSchema{
		Name: "users",
		Columns: []Column{
			{Name: "id", Type: storage.TypeInt},
			{Name: "name", Type: storage.TypeVarchar},
			{Name: "age", Type: storage.TypeInt},
		},
		Indexes: []IndexDef{{Name: "id", Column: "id", Primary: true}, {Name: "age", Column: "age"}},
	})
	mustAdd(t, cat, TableSchema{
		Name: "orders",
		Columns: []Column{
			{Name: "id", Type: storage.TypeInt},
			{Name: "user_id", Type: storage.TypeInt},
			{Name: "amount", Type: storage.TypeInt},
		},
		Indexes: []IndexDef{{Name: "id", Column: "id", Primary: true}, {Name: "user_id", Column: "user_id"}},
	})

	e := NewExecutor(se, cat, bsoncodec.New())
	ctx := context.Background()
	seedUsers := []struct {
		id, age int
		name    string
	}{{1, 30, "alice"}, {2, 25, "bob"}, {3, 40, "carol"}}
	for _, u := range seedUsers {
		if _, err := e.Exec(ctx, fmt.Sprintf("INSERT INTO users (id, name, age) VALUES (%d, '%s', %d)", u.id, u.name, u.age)); err != nil {
			t.Fatalf("seed user %d: %v", u.id, err)
		}
	}
	seedOrders := []struct{ id, userID, amount int }{{1, 1, 100}, {2, 1, 50}, {3, 2, 200}}
	for _, o := range seedOrders {
		if _, err := e.Exec(ctx, fmt.Sprintf("INSERT INTO orders (id, user_id, amount) VALUES (%d, %d, %d)", o.id, o.userID, o.amount)); err != nil {
			t.Fatalf("seed order %d: %v", o.id, err)
		}
	}
	return e
}

func mustAdd(t *testing.T, c *Catalog, s TableSchema) {
	t.Helper()
	if err := c.AddTable(s); err != nil {
		t.Fatalf("AddTable %s: %v", s.Name, err)
	}
}

func TestParseInnerJoin(t *testing.T) {
	sel := parseSelect(t, "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id")
	if len(sel.Joins) != 1 {
		t.Fatalf("joins = %d, want 1", len(sel.Joins))
	}
	j := sel.Joins[0]
	if j.Table != "orders" || j.Alias != "o" || j.Left {
		t.Fatalf("join = %+v, want INNER orders o", j)
	}
	if j.On == nil || j.On.String() != "(u.id = o.user_id)" {
		t.Fatalf("on = %v, want (u.id = o.user_id)", j.On)
	}
}

func TestQueryInnerJoin(t *testing.T) {
	e := newJoinExecutor(t)
	rs, err := e.Query(context.Background(),
		"SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.amount")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	names := strColumn(t, rs, "name")
	amounts := intColumn(t, rs, "amount")
	wantNames := []string{"alice", "alice", "bob"}
	wantAmounts := []int64{50, 100, 200}
	if len(names) != 3 {
		t.Fatalf("rows = %d, want 3", len(names))
	}
	for i := range wantNames {
		if names[i] != wantNames[i] || amounts[i] != wantAmounts[i] {
			t.Fatalf("got (%v,%v), want (%v,%v)", names, amounts, wantNames, wantAmounts)
		}
	}
}

func TestQueryInnerJoinWithWhere(t *testing.T) {
	e := newJoinExecutor(t)
	rs, err := e.Query(context.Background(),
		"SELECT o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE u.name = 'alice' ORDER BY o.amount")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	amounts := intColumn(t, rs, "amount")
	if len(amounts) != 2 || amounts[0] != 50 || amounts[1] != 100 {
		t.Fatalf("amounts = %v, want [50 100]", amounts)
	}
}

func TestQueryJoinAggregate(t *testing.T) {
	e := newJoinExecutor(t)
	rs, err := e.Query(context.Background(),
		"SELECT u.name, SUM(o.amount) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY u.name")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	names := strColumn(t, rs, "name")
	sums := intColumn(t, rs, "sum(o.amount)")
	want := map[string]int64{"alice": 150, "bob": 200}
	if len(names) != 2 {
		t.Fatalf("groups = %d, want 2", len(names))
	}
	for i, n := range names {
		if sums[i] != want[n] {
			t.Fatalf("SUM for %s = %d, want %d", n, sums[i], want[n])
		}
	}
}

func TestQueryJoinStar(t *testing.T) {
	e := newJoinExecutor(t)
	rs, err := e.Query(context.Background(),
		"SELECT * FROM users u JOIN orders o ON u.id = o.user_id")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rs.Rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rs.Rows))
	}
	// users(3 cols) + orders(3 cols).
	if len(rs.Columns) != 6 {
		t.Fatalf("columns = %v, want 6", rs.Columns)
	}
}

func TestQueryUnknownJoinQualifierError(t *testing.T) {
	e := newJoinExecutor(t)
	if _, err := e.Query(context.Background(),
		"SELECT z.name FROM users u JOIN orders o ON u.id = o.user_id"); err == nil {
		t.Fatal("expected error for unknown qualifier z")
	}
}
