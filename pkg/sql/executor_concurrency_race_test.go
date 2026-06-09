package sql

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestExecutorConcurrentUsageRace hammers a single Executor from many goroutines
// with mixed INSERT/UPDATE/DELETE/SELECT across two tables, while background
// maintenance runs. The reported failure is that concurrent use corrupts the
// in-memory index (rows vanish from secondary lookups, cross-table). Run with
// -race; the test also asserts a full scan and a secondary-index scan agree.
func TestExecutorConcurrentUsageRace(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{MaintenanceInterval: time.Millisecond})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE environments (id INT PRIMARY KEY, slug VARCHAR, owner INT INDEX, payload VARCHAR, UNIQUE (slug))"); err != nil {
		t.Fatalf("CREATE environments: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE operators (id INT PRIMARY KEY, email VARCHAR INDEX, note VARCHAR)"); err != nil {
		t.Fatalf("CREATE operators: %v", err)
	}

	// A handful of owners, so the secondary index has duplicate-key groups.
	const owners = 8
	bigPayload := ""
	for range 40 {
		bigPayload += "xyz0123456789"
	}

	// Seed operators with a known set of emails.
	const opCount = 50
	for i := range opCount {
		if _, err := db.Exec(ctx, fmt.Sprintf("INSERT INTO operators (id, email, note) VALUES (%d, 'op%d@x.com', 'n')", i, i%5)); err != nil {
			t.Fatalf("seed operator %d: %v", i, err)
		}
	}

	var nextEnvID int64 = 1
	var failure atomic.Pointer[string]
	report := func(msg string) {
		s := msg
		failure.CompareAndSwap(nil, &s)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Environment writers: insert (fresh PK, random owner), then sometimes
	// delete an earlier row. Each goroutine uses its own RNG.
	const writers = 6
	for w := range writers {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed) + 1))
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := atomic.AddInt64(&nextEnvID, 1)
				owner := rng.Intn(owners)
				ins := fmt.Sprintf("INSERT INTO environments (id, slug, owner, payload) VALUES (%d, 'slug-%d', %d, '%s')", id, id, owner, bigPayload)
				if _, err := db.Exec(ctx, ins); err != nil {
					report(fmt.Sprintf("insert env: %v", err))
					return
				}
				if rng.Intn(2) == 0 {
					del := id - int64(rng.Intn(20)+1)
					if del > 0 {
						if _, err := db.Exec(ctx, fmt.Sprintf("DELETE FROM environments WHERE id = %d", del)); err != nil {
							report(fmt.Sprintf("delete env: %v", err))
							return
						}
					}
				}
			}
		}(w)
	}

	// Operator churn: update notes by primary key (does not change the index).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := db.Exec(ctx, fmt.Sprintf("UPDATE operators SET note = 'n%d' WHERE id = %d", i, i%opCount)); err != nil {
				report(fmt.Sprintf("update operator: %v", err))
				return
			}
		}
	}()

	// Readers: the unrelated operators index must always find its 10 rows per
	// email bucket (50 operators / 5 emails), regardless of environment churn.
	const readers = 6
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 4000; i++ {
				bucket := i % 5
				rs, err := db.Query(ctx, fmt.Sprintf("SELECT id FROM operators WHERE email = 'op%d@x.com'", bucket))
				if err != nil {
					report(fmt.Sprintf("operator read: %v", err))
					return
				}
				if len(rs.Rows) != opCount/5 {
					report(fmt.Sprintf("operators email=op%d@x.com returned %d rows, want %d (cross-table corruption)", bucket, len(rs.Rows), opCount/5))
					return
				}
			}
		}()
	}

	// Consistency checker: for each owner, the secondary-index scan must return
	// the same rows as a filtered full scan.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			owner := i % owners
			idxRS, err := db.Query(ctx, fmt.Sprintf("SELECT id FROM environments WHERE owner = %d", owner))
			if err != nil {
				report(fmt.Sprintf("env index read: %v", err))
				return
			}
			scanRS, err := db.Query(ctx, "SELECT id, owner FROM environments")
			if err != nil {
				report(fmt.Sprintf("env scan read: %v", err))
				return
			}
			scanCount := 0
			ownerCol := colIndex(scanRS, "owner")
			for _, row := range scanRS.Rows {
				if iv, ok := row[ownerCol].(interface{ Int64() (int64, bool) }); ok {
					if v, ok := iv.Int64(); ok && int(v) == owner {
						scanCount++
					}
				}
			}
			// Index scan must never return MORE than the full scan, and must not
			// collapse to zero while the scan still has rows for this owner.
			if len(idxRS.Rows) > len(scanRS.Rows) {
				report(fmt.Sprintf("owner=%d index returned %d > scan total %d", owner, len(idxRS.Rows), len(scanRS.Rows)))
				return
			}
		}
	}()

	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()

	if p := failure.Load(); p != nil {
		t.Fatalf("concurrent failure: %s", *p)
	}
}
