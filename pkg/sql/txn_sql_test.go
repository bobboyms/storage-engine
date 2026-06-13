package sql

import (
	"context"
	"errors"
	"testing"
)

func TestParseBeginCommitRollback(t *testing.T) {
	cases := []struct {
		sql  string
		kind TxControl
	}{
		{"BEGIN", TxBegin},
		{"BEGIN TRANSACTION", TxBegin},
		{"START TRANSACTION", TxBegin},
		{"COMMIT", TxCommit},
		{"COMMIT TRANSACTION", TxCommit},
		{"ROLLBACK", TxRollback},
		{"ROLLBACK TRANSACTION", TxRollback},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			stmt, err := Parse(c.sql)
			if err != nil {
				t.Fatalf("Parse(%q): %v", c.sql, err)
			}
			tc, ok := stmt.(*TxControlStmt)
			if !ok {
				t.Fatalf("stmt = %T, want *TxControlStmt", stmt)
			}
			if tc.Action != c.kind {
				t.Fatalf("action = %v, want %v", tc.Action, c.kind)
			}
		})
	}
}

func TestExecBeginCommitFlow(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO accounts (id, balance) VALUES (1, 100)"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if _, err := db.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	if _, err := db.Exec(ctx, "UPDATE accounts SET balance = balance - 30 WHERE id = 1"); err != nil {
		t.Fatalf("UPDATE in tx: %v", err)
	}
	if _, err := db.Exec(ctx, "COMMIT"); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}

	rs, err := db.Query(ctx, "SELECT balance FROM accounts WHERE id = 1")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := intColumn(t, rs, "balance"); got[0] != 70 {
		t.Fatalf("balance after commit = %d, want 70", got[0])
	}
}

func TestExecBeginRollbackFlow(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO accounts (id, balance) VALUES (1, 100)"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if _, err := db.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	if _, err := db.Exec(ctx, "UPDATE accounts SET balance = 0 WHERE id = 1"); err != nil {
		t.Fatalf("UPDATE in tx: %v", err)
	}
	if _, err := db.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("ROLLBACK: %v", err)
	}

	rs, err := db.Query(ctx, "SELECT balance FROM accounts WHERE id = 1")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := intColumn(t, rs, "balance"); got[0] != 100 {
		t.Fatalf("balance after rollback = %d, want 100 (unchanged)", got[0])
	}
}

func TestQueryInsideSessionTransactionSeesStagedWrites(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY, v INT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t (id, v) VALUES (1, 42)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	// Read-your-writes: the open session transaction sees the staged row.
	rs, err := db.Query(ctx, "SELECT v FROM t WHERE id = 1")
	if err != nil {
		t.Fatalf("Query in tx: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %d, want 1 (read-your-writes)", len(rs.Rows))
	}
	if _, err := db.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("ROLLBACK: %v", err)
	}
	// After rollback the row is gone.
	rs, err = db.Query(ctx, "SELECT v FROM t WHERE id = 1")
	if err != nil {
		t.Fatalf("Query after rollback: %v", err)
	}
	if len(rs.Rows) != 0 {
		t.Fatalf("rows = %d, want 0 after rollback", len(rs.Rows))
	}
}

func TestBeginWhileTransactionOpenFails(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	defer func() { _, _ = db.Exec(ctx, "ROLLBACK") }()
	_, err := db.Exec(ctx, "BEGIN")
	if !errors.Is(err, ErrNoTransaction) && err == nil {
		t.Fatalf("nested BEGIN err = %v, want an error", err)
	}
	if err == nil {
		t.Fatal("nested BEGIN succeeded, want error")
	}
}

func TestCommitWithoutTransactionFails(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	_, err := db.Exec(ctx, "COMMIT")
	if !errors.Is(err, ErrNoTransaction) {
		t.Fatalf("COMMIT without tx err = %v, want ErrNoTransaction", err)
	}
}

func TestRollbackWithoutTransactionFails(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	_, err := db.Exec(ctx, "ROLLBACK")
	if !errors.Is(err, ErrNoTransaction) {
		t.Fatalf("ROLLBACK without tx err = %v, want ErrNoTransaction", err)
	}
}

func TestDDLInsideSessionTransactionFails(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	defer func() { _, _ = db.Exec(ctx, "ROLLBACK") }()
	// DDL is not transactional here; it must be rejected inside a session tx.
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY)"); err == nil {
		t.Fatal("CREATE TABLE inside session transaction succeeded, want error")
	}
}

func TestCloseRollsBackOpenSessionTransaction(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY, v INT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t (id, v) VALUES (1, 9)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	// Close with the transaction still open must roll its staged writes back.
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := OpenDatabaseWithOptions(ctx, dir, OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db2.Close() }()
	rs, err := db2.Query(ctx, "SELECT v FROM t WHERE id = 1")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rs.Rows) != 0 {
		t.Fatalf("rows = %d, want 0 (uncommitted tx discarded)", len(rs.Rows))
	}
}

func TestSessionTransactionAutoIncrement(t *testing.T) {
	db, _ := openIntegrityDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE logs (id INT PRIMARY KEY AUTO_INCREMENT, msg VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO logs (msg) VALUES ('a')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := db.Exec(ctx, "COMMIT"); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}
	rs, err := db.Query(ctx, "SELECT id FROM logs WHERE msg = 'a'")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := intColumn(t, rs, "id"); got[0] != 1 {
		t.Fatalf("id = %d, want 1", got[0])
	}
}
