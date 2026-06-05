package sql

import (
	"context"
	"errors"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestParseWindowFunction(t *testing.T) {
	sel := parseSelect(t, "SELECT id, ROW_NUMBER() OVER (PARTITION BY age ORDER BY id DESC) AS rn FROM users")
	if len(sel.Items) != 2 {
		t.Fatalf("items = %d, want 2", len(sel.Items))
	}
	w := sel.Items[1].Window
	if w == nil {
		t.Fatalf("item[1].Window = nil, want a window call")
	}
	if w.Func != "ROW_NUMBER" {
		t.Fatalf("Func = %q, want ROW_NUMBER", w.Func)
	}
	if len(w.Partition) != 1 || w.Partition[0] != "age" {
		t.Fatalf("Partition = %v, want [age]", w.Partition)
	}
	if w.Order == nil || w.Order.Column != "id" || !w.Order.Desc {
		t.Fatalf("Order = %+v, want id DESC", w.Order)
	}
	if sel.Items[1].OutputName() != "rn" {
		t.Fatalf("OutputName = %q, want rn", sel.Items[1].OutputName())
	}
}

func TestParseWindowRankRequiresOver(t *testing.T) {
	if _, err := Parse("SELECT RANK() FROM users"); !errors.Is(err, ErrParse) {
		t.Fatalf("err = %v, want ErrParse (RANK without OVER)", err)
	}
}

// windowByID maps each row's id to its window-column value (as int64).
func windowByID(t *testing.T, rs *ResultSet, name string) map[int64]int64 {
	t.Helper()
	idIdx := colIndex(rs, "id")
	vIdx := colIndex(rs, name)
	if idIdx < 0 || vIdx < 0 {
		t.Fatalf("missing column id=%d %s=%d in %v", idIdx, name, vIdx, rs.Columns)
	}
	out := make(map[int64]int64, len(rs.Rows))
	for i, row := range rs.Rows {
		id, ok := row[idIdx].(types.IntKey)
		if !ok {
			t.Fatalf("row %d id = %T, want IntKey", i, row[idIdx])
		}
		v, ok := row[vIdx].(types.IntKey)
		if !ok {
			t.Fatalf("row %d %s = %T, want IntKey", i, name, row[vIdx])
		}
		out[int64(id)] = int64(v)
	}
	return out
}

func TestQueryRowNumberOverOrderBy(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT id, ROW_NUMBER() OVER (ORDER BY age) AS rn FROM users")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// age order (stable on ties): bob(25,id2), alice(30,id1), dave(30,id4), carol(40,id3)
	want := map[int64]int64{2: 1, 1: 2, 4: 3, 3: 4}
	got := windowByID(t, rs, "rn")
	for id, w := range want {
		if got[id] != w {
			t.Fatalf("row_number id=%d = %d, want %d (all=%v)", id, got[id], w, got)
		}
	}
}

func TestQueryRankAndDenseRank(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT id, RANK() OVER (ORDER BY age) AS r, DENSE_RANK() OVER (ORDER BY age) AS dr FROM users")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// ages: id2=25, id1=30, id4=30, id3=40
	wantRank := map[int64]int64{2: 1, 1: 2, 4: 2, 3: 4}
	wantDense := map[int64]int64{2: 1, 1: 2, 4: 2, 3: 3}
	gotRank := windowByID(t, rs, "r")
	gotDense := windowByID(t, rs, "dr")
	for id := range wantRank {
		if gotRank[id] != wantRank[id] {
			t.Fatalf("rank id=%d = %d, want %d (all=%v)", id, gotRank[id], wantRank[id], gotRank)
		}
		if gotDense[id] != wantDense[id] {
			t.Fatalf("dense_rank id=%d = %d, want %d (all=%v)", id, gotDense[id], wantDense[id], gotDense)
		}
	}
}

func TestQueryWindowPartitionCount(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT id, COUNT(*) OVER (PARTITION BY age) AS c FROM users")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// age 30 -> {id1,id4} count 2; age 25 -> {id2} count 1; age 40 -> {id3} count 1
	want := map[int64]int64{1: 2, 4: 2, 2: 1, 3: 1}
	got := windowByID(t, rs, "c")
	for id, w := range want {
		if got[id] != w {
			t.Fatalf("count id=%d = %d, want %d (all=%v)", id, got[id], w, got)
		}
	}
}

func TestQueryWindowSumOverAll(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT id, SUM(age) OVER () AS total FROM users")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	got := windowByID(t, rs, "total")
	for id, v := range got {
		if v != 125 { // 30+25+40+30
			t.Fatalf("sum over () id=%d = %d, want 125", id, v)
		}
	}
}

func TestWindowDefaultOutputNames(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(),
		"SELECT id, ROW_NUMBER() OVER (ORDER BY id), SUM(age) OVER (), COUNT(*) OVER () FROM users")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	for _, name := range []string{"row_number()", "sum(age)", "count(*)"} {
		if colIndex(rs, name) < 0 {
			t.Fatalf("missing default window column %q in %v", name, rs.Columns)
		}
	}
}

func TestQueryWindowWithStarProjection(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT *, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM users")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	for _, name := range []string{"id", "name", "age", "rn"} {
		if colIndex(rs, name) < 0 {
			t.Fatalf("missing column %q in %v", name, rs.Columns)
		}
	}
}

func TestQueryWindowRejectedWithGroupBy(t *testing.T) {
	e := newExecutor(t)
	if _, err := e.Query(context.Background(), "SELECT age, ROW_NUMBER() OVER (), COUNT(*) FROM users GROUP BY age"); err == nil {
		t.Fatal("expected error combining window functions with GROUP BY/aggregates")
	}
}

func TestQueryWindowRunningSum(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT id, SUM(age) OVER (ORDER BY id) AS run FROM users")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// running sum by id order: id1=30, id2=55, id3=95, id4=125
	want := map[int64]int64{1: 30, 2: 55, 3: 95, 4: 125}
	got := windowByID(t, rs, "run")
	for id, w := range want {
		if got[id] != w {
			t.Fatalf("running sum id=%d = %d, want %d (all=%v)", id, got[id], w, got)
		}
	}
}
