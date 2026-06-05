package sql

import (
	"context"
	"reflect"
	"testing"
)

// openSeqDB creates a table t(id INT PRIMARY KEY, n INT) seeded with rows
// id = 1..count (n = id*10) and returns the executor with a decode counter.
func openSeqDB(t *testing.T, count int) (*Executor, context.Context, *int) {
	t.Helper()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, t.TempDir(), OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY, n INT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	for i := 1; i <= count; i++ {
		if _, err := db.Exec(ctx, "INSERT INTO t (id, n) VALUES (?, ?)", i, i*10); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	var decoded int
	db.decodeHook = func() { decoded++ }
	return db, ctx, &decoded
}

func ids(t *testing.T, rs *ResultSet) []int64 { return intColumn(t, rs, "id") }

func TestLimitPushdownStopsScanEarly(t *testing.T) {
	db, ctx, decoded := openSeqDB(t, 200)

	rs, err := db.Query(ctx, "SELECT id FROM t ORDER BY id LIMIT 5")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := ids(t, rs); !reflect.DeepEqual(got, []int64{1, 2, 3, 4, 5}) {
		t.Fatalf("ids = %v, want [1 2 3 4 5]", got)
	}
	// The scan must stop after the limit instead of decoding all 200 rows.
	if *decoded > 5 {
		t.Fatalf("decoded %d rows, want at most 5 (LIMIT push-down)", *decoded)
	}
}

func TestLimitPushdownWithOffset(t *testing.T) {
	db, ctx, decoded := openSeqDB(t, 200)

	rs, err := db.Query(ctx, "SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 10")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := ids(t, rs); !reflect.DeepEqual(got, []int64{11, 12, 13, 14, 15}) {
		t.Fatalf("ids = %v, want [11..15]", got)
	}
	// Offset pagination must scan offset+limit rows, not the whole table.
	if *decoded > 15 {
		t.Fatalf("decoded %d rows, want at most 15", *decoded)
	}
}

func TestKeysetPaginationSeeksAndStops(t *testing.T) {
	db, ctx, decoded := openSeqDB(t, 200)

	// Keyset pagination: the cursor (id > last) becomes the scan lower bound, so
	// the engine seeks past skipped rows instead of counting them.
	rs, err := db.Query(ctx, "SELECT id FROM t WHERE id > ? ORDER BY id LIMIT 5", 100)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := ids(t, rs); !reflect.DeepEqual(got, []int64{101, 102, 103, 104, 105}) {
		t.Fatalf("ids = %v, want [101..105]", got)
	}
	// The seek jumps to the cursor: at most the inclusive boundary row (id=100,
	// filtered by the residual) plus the 5 page rows -- not the skipped 100.
	if *decoded > 6 {
		t.Fatalf("decoded %d rows, want at most 6 (keyset seek + early stop)", *decoded)
	}
}

func TestLimitPushdownRespectsResidual(t *testing.T) {
	db, ctx, decoded := openSeqDB(t, 200)

	// Only even ids; LIMIT must count rows that pass the predicate, so the scan
	// reads more than `limit` rows but still stops well before the end.
	rs, err := db.Query(ctx, "SELECT id FROM t WHERE n > ? ORDER BY id LIMIT 3", 0)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := ids(t, rs); !reflect.DeepEqual(got, []int64{1, 2, 3}) {
		t.Fatalf("ids = %v, want [1 2 3]", got)
	}
	if *decoded > 3 {
		t.Fatalf("decoded %d rows, want at most 3", *decoded)
	}
}

func TestSortPreventsPushdownButStaysCorrect(t *testing.T) {
	db, ctx, _ := openSeqDB(t, 50)

	// ORDER BY a non-indexed direction needs an in-memory sort, so push-down is
	// disabled; the result must still be the correct top of the sorted order.
	rs, err := db.Query(ctx, "SELECT id FROM t ORDER BY n DESC LIMIT 3")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := ids(t, rs); !reflect.DeepEqual(got, []int64{50, 49, 48}) {
		t.Fatalf("ids = %v, want [50 49 48]", got)
	}
}
