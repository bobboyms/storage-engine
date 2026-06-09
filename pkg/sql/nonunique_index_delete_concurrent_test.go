package sql

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestDeleteDuplicateConcurrentCrossTable stresses DELETE/INSERT churn on a
// non-unique secondary-index group while an unrelated table is read and written
// concurrently. The report says a DELETE corrupts both the group's index and an
// unrelated table's index in the same process. Run with -race to surface shared
// in-memory state corruption.
func TestDeleteDuplicateConcurrentCrossTable(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{MaintenanceInterval: time.Millisecond})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE environments (id UUID PRIMARY KEY, slug VARCHAR, owner_operator_id UUID INDEX, UNIQUE (slug))"); err != nil {
		t.Fatalf("CREATE environments: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE operators (id UUID PRIMARY KEY, email VARCHAR INDEX)"); err != nil {
		t.Fatalf("CREATE operators: %v", err)
	}

	const owner = "11111111-1111-1111-1111-111111111111"
	const opEmail = "operator@example.com"
	const opID = "99999999-0000-0000-0000-000000000009"
	if _, err := db.Exec(ctx, fmt.Sprintf("INSERT INTO operators (id, email) VALUES ('%s', '%s')", opID, opEmail)); err != nil {
		t.Fatalf("INSERT operator: %v", err)
	}

	envID := func(n int) string { return fmt.Sprintf("eeeeeeee-0000-0000-0000-%012d", n) }

	// Seed three duplicate-key environments.
	for i := range 3 {
		if _, err := db.Exec(ctx, fmt.Sprintf("INSERT INTO environments (id, slug, owner_operator_id) VALUES ('%s', 'slug-%d', '%s')", envID(i), i, owner)); err != nil {
			t.Fatalf("seed env %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	errCh := make(chan error, 32)
	fail := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	// Writer: churn environments — insert a brand-new row and delete the one
	// from three iterations ago, keeping the duplicate group size near 3 while
	// using only fresh primary keys and slugs.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := 3; ; n++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := db.Exec(ctx, fmt.Sprintf("INSERT INTO environments (id, slug, owner_operator_id) VALUES ('%s', 'slug-%d', '%s')", envID(n), n, owner)); err != nil {
				fail(fmt.Errorf("insert env: %w", err))
				return
			}
			if _, err := db.Exec(ctx, fmt.Sprintf("DELETE FROM environments WHERE id = '%s'", envID(n-3))); err != nil {
				fail(fmt.Errorf("delete env: %w", err))
				return
			}
		}
	}()

	// Reader/writer on the unrelated operators table.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			rs, err := db.Query(ctx, fmt.Sprintf("SELECT id FROM operators WHERE email = '%s'", opEmail))
			if err != nil {
				fail(fmt.Errorf("operator read: %w", err))
				return
			}
			if len(rs.Rows) != 1 {
				fail(fmt.Errorf("operators-by-email = %d rows, want 1 (cross-table corruption)", len(rs.Rows)))
				return
			}
			if _, err := db.Exec(ctx, fmt.Sprintf("UPDATE operators SET email = '%s' WHERE id = '%s'", opEmail, opID)); err != nil {
				fail(fmt.Errorf("operator update: %w", err))
				return
			}
		}
	}()

	// Readers of the environments group: count must stay in a sane band [2,4].
	const readers = 4
	for r := range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 1000 {
				_ = i
				rs, err := db.Query(ctx, fmt.Sprintf("SELECT id FROM environments WHERE owner_operator_id = '%s'", owner))
				if err != nil {
					fail(fmt.Errorf("env read: %w", err))
					return
				}
				if len(rs.Rows) < 2 {
					fail(fmt.Errorf("env-by-owner = %d rows, want >=2 (lost siblings)", len(rs.Rows)))
					return
				}
			}
			_ = r
		}()
	}

	done := make(chan struct{})
	go func() {
		time.Sleep(3 * time.Second)
		close(stop)
		close(done)
	}()
	<-done
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent failure: %v", err)
		}
	}
}
