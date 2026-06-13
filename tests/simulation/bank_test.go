//go:build simulation

// Package simulation runs production-shaped workloads against the SQL layer
// to surface bugs that targeted unit tests miss. The flagship is a banking
// model whose oracle is a conservation law: money is only ever moved between
// accounts inside a single atomic transaction, so the sum of every balance is
// invariant. Any bug that violates atomicity, isolation, or durability — in
// the engine, the indexes, MVCC, or crash recovery — breaks that sum, no
// matter where it originates.
//
// Build-tagged (like chaos/stress/faults) so it never runs in the default
// `go test ./...`. Run with:
//
//	make test-simulation
//	go test ./tests/simulation -tags simulation -count=1 -v
//
// Volume scales with STORAGE_ENGINE_SIM_SCALE (default 1). CI sets a modest
// scale; crank it locally to stress harder.
package simulation

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/sql"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// --- world parameters ---------------------------------------------------

// bankAccounts are the funding accounts that hold the initial mint; spreading
// the mint across several of them keeps the deposit/withdrawal path from
// serializing on a single hot row while still exercising contention.
const bankAccounts = 16

// mintPerBankAccount is the notional share of the mint each bank account
// would hold before users are funded. The total money in the world is
// bankAccounts * mintPerBankAccount and must stay constant for the life of
// the database.
const mintPerBankAccount = 100_000_000

// userOpening is the balance every user account is funded with at mint time.
// Funding happens by deducting it from the bank accounts inside the same
// INSERT values (not via runtime transfers), so the books balance from the
// first row and the mint stays cheap and batched.
const userOpening = 1_000

// scale reads STORAGE_ENGINE_SIM_SCALE (default 1, minimum 1). Volume knobs
// multiply by it.
func scale() int {
	if raw := os.Getenv("STORAGE_ENGINE_SIM_SCALE"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return 1
}

func totalMoney() int64 { return int64(bankAccounts) * mintPerBankAccount }

// bankOpening returns the opening balance of bank account i once the user
// accounts have been funded out of the bank pool. The total deducted equals
// userCount*userOpening, spread across the bank accounts (remainder on the
// lowest ids), so sum(bank openings) + userCount*userOpening == totalMoney().
func bankOpening(i, userCount int) int64 {
	totalUser := int64(userCount) * userOpening
	deduction := totalUser / bankAccounts
	remainder := totalUser % bankAccounts
	bal := int64(mintPerBankAccount) - deduction
	if int64(i) < remainder {
		bal--
	}
	return bal
}

// retryableConflict reports whether err is the engine's signal that a
// transaction lost a race and should be retried, rather than a real failure.
func retryableConflict(err error) bool {
	return errors.Is(err, storage.ErrSerializationConflict) ||
		errors.Is(err, storage.ErrDeadlockVictim) ||
		errors.Is(err, storage.ErrLockWaitTimeout)
}

// --- bank harness -------------------------------------------------------

type bank struct {
	db        *sql.Executor
	userBase  int // first user account id (bank accounts occupy [0, bankAccounts))
	userCount int
	// model, when non-nil, tracks the expected balance of every account and
	// is updated on each committed transfer. The concurrent test enables it
	// for a per-account cross-check; the crash child leaves it nil.
	model *ledgerModel
}

// ledgerModel is the reference accounting the test maintains in parallel with
// the database. Updates are serialized and applied only for committed
// transfers, so at quiescence it equals the database account for account.
type ledgerModel struct {
	mu  sync.Mutex
	bal map[int]int64
}

func newLedgerModel() *ledgerModel { return &ledgerModel{bal: make(map[int]int64)} }

func (m *ledgerModel) set(id int, v int64) {
	m.mu.Lock()
	m.bal[id] = v
	m.mu.Unlock()
}

func (m *ledgerModel) apply(src, dst int, amount int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.bal[src] -= amount
	m.bal[dst] += amount
	m.mu.Unlock()
}

// openBank opens (or reopens) the database at dir and returns a handle. It
// does not create the schema; callers either mint a fresh world or reopen an
// existing one.
func openBank(tb testing.TB, dir string, userCount int) *bank {
	tb.Helper()
	db, err := sql.OpenDatabaseWithOptions(context.Background(), dir, sql.OpenOptions{
		// The simulation drives checkpoints explicitly at known points so the
		// crash window is well-defined; background maintenance would blur it.
		DisableMaintenance: true,
	})
	if err != nil {
		tb.Fatalf("open database: %v", err)
	}
	return &bank{db: db, userBase: bankAccounts, userCount: userCount}
}

func (b *bank) close() error { return b.db.Close() }

// mint creates the schema and the world: bank accounts holding the entire
// mint, plus userCount user accounts opened at zero balance. After mint the
// conservation invariant SUM(balance) == totalMoney() already holds.
func (b *bank) mint(ctx context.Context) error {
	const ddl = "CREATE TABLE accounts (id INT PRIMARY KEY, owner VARCHAR INDEX, balance INT)"
	if _, err := b.db.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	// Bank accounts carry the mint minus what the users are funded with. These
	// INSERTs are the only steps that establish the world's money; they
	// complete before any crash window opens, and the books already balance.
	for id := 0; id < bankAccounts; id++ {
		if _, err := b.db.Exec(ctx, "INSERT INTO accounts (id, owner, balance) VALUES (?, ?, ?)",
			id, "bank", bankOpening(id, b.userCount)); err != nil {
			return fmt.Errorf("mint bank %d: %w", id, err)
		}
	}
	// User accounts open already funded, in batched transactions (no per-row
	// fsync), so mint stays fast even for hundreds of thousands of accounts.
	const batch = 500
	for lo := 0; lo < b.userCount; lo += batch {
		tx := b.db.Begin()
		for i := lo; i < lo+batch && i < b.userCount; i++ {
			id := b.userBase + i
			if _, err := tx.Exec(ctx, "INSERT INTO accounts (id, owner, balance) VALUES (?, ?, ?)",
				id, fmt.Sprintf("owner-%d", id%1024), userOpening); err != nil {
				_ = tx.Rollback(ctx)
				return fmt.Errorf("open user %d: %w", id, err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("open users commit at %d: %w", lo, err)
		}
	}
	return nil
}

// seedModel sets the reference model to the exact post-mint balances.
func (b *bank) seedModel() {
	b.model = newLedgerModel()
	for id := 0; id < bankAccounts; id++ {
		b.model.set(id, bankOpening(id, b.userCount))
	}
	for i := 0; i < b.userCount; i++ {
		b.model.set(b.userBase+i, userOpening)
	}
}

// transfer atomically moves amount from src to dst, retrying on conflicts. It
// returns (true, nil) when money moved, (false, nil) when the transfer was a
// legitimate no-op (insufficient funds, or src == dst), and an error only on
// a real failure. Both rows are locked FOR UPDATE in ascending id order so
// concurrent transfers can never deadlock on lock-acquire ordering.
func (b *bank) transfer(ctx context.Context, src, dst int, amount int64) (bool, error) {
	if src == dst || amount <= 0 {
		return false, nil
	}
	const maxRetries = 1000
	for attempt := 0; ; attempt++ {
		moved, err := b.tryTransfer(ctx, src, dst, amount)
		if err == nil {
			if moved {
				b.model.apply(src, dst, amount)
			}
			return moved, nil
		}
		if !retryableConflict(err) {
			return false, err
		}
		if attempt >= maxRetries {
			return false, fmt.Errorf("transfer %d->%d livelocked after %d retries: %w", src, dst, attempt, err)
		}
		// Tiny randomized backoff to break up contention storms.
		time.Sleep(time.Duration(attempt%8) * 50 * time.Microsecond)
	}
}

func (b *bank) tryTransfer(ctx context.Context, src, dst int, amount int64) (moved bool, err error) {
	tx := b.db.Begin()
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	lockOrder := []int{src, dst}
	sort.Ints(lockOrder)
	bal := make(map[int]int64, 2)
	for _, id := range lockOrder {
		v, rerr := readBalanceForUpdate(ctx, tx, id)
		if rerr != nil {
			return false, rerr
		}
		bal[id] = v
	}

	if bal[src] < amount {
		return false, nil // insufficient funds: a legitimate no-op.
	}

	if _, err := tx.Exec(ctx, "UPDATE accounts SET balance = ? WHERE id = ?", bal[src]-amount, src); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, "UPDATE accounts SET balance = ? WHERE id = ?", bal[dst]+amount, dst); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	committed = true
	return true, nil
}

func readBalanceForUpdate(ctx context.Context, tx *sql.Tx, id int) (int64, error) {
	rs, err := tx.Query(ctx, "SELECT balance FROM accounts WHERE id = ? FOR UPDATE", id)
	if err != nil {
		return 0, err
	}
	if len(rs.Rows) != 1 {
		return 0, fmt.Errorf("account %d: expected 1 row, got %d", id, len(rs.Rows))
	}
	v, ok := rs.Rows[0][0].(types.IntKey)
	if !ok {
		return 0, fmt.Errorf("account %d: balance is %T, want IntKey", id, rs.Rows[0][0])
	}
	return int64(v), nil
}

// totalBalance reads SUM(balance) over a single consistent snapshot. Because
// every transfer is atomic, a correct snapshot must see exactly totalMoney()
// regardless of how many transfers are in flight — so this doubles as a
// snapshot-isolation check when called mid-workload.
func (b *bank) totalBalance(ctx context.Context) (int64, error) {
	rs, err := b.db.Query(ctx, "SELECT SUM(balance) FROM accounts")
	if err != nil {
		return 0, err
	}
	if len(rs.Rows) != 1 {
		return 0, fmt.Errorf("SUM returned %d rows, want 1", len(rs.Rows))
	}
	v, ok := rs.Rows[0][0].(types.IntKey)
	if !ok {
		return 0, fmt.Errorf("SUM is %T, want IntKey", rs.Rows[0][0])
	}
	return int64(v), nil
}

func (b *bank) randomAccount(r *rand.Rand) int { return b.userBase + r.Intn(b.userCount) }
func randomBank(r *rand.Rand) int              { return r.Intn(bankAccounts) }

// --- the concurrent conservation stress test ----------------------------

// TestBankConcurrentConservation drives a high-volume, highly concurrent
// banking workload and asserts that money is conserved both mid-flight (a
// live snapshot-isolation oracle) and at quiescence, then scrubs the
// reopened database for structural integrity.
func TestBankConcurrentConservation(t *testing.T) {
	s := scale()
	// Account count scales with volume so the buffer pools always evict;
	// concurrency is fixed and the per-worker op count grows modestly so the
	// fsync-bound runtime stays bounded.
	userCount := 8_000 * s
	workers := 48
	opsPerWorker := 200 * s

	ctx := context.Background()
	dir := t.TempDir()

	b := openBank(t, dir, userCount)
	if err := b.mint(ctx); err != nil {
		_ = b.close()
		t.Fatalf("mint world: %v", err)
	}
	b.seedModel()
	t.Logf("world minted: %d bank + %d user accounts, total money %d",
		bankAccounts, userCount, totalMoney())

	// A background auditor reads the total over independent snapshots while
	// the workers run: it must never observe a value other than totalMoney().
	stopAudit := make(chan struct{})
	auditDone := make(chan struct{})
	var auditFail atomic.Pointer[string]
	go func() {
		defer close(auditDone)
		r := rand.New(rand.NewSource(0xA0D17))
		for {
			select {
			case <-stopAudit:
				return
			default:
			}
			got, err := b.totalBalance(ctx)
			if err != nil {
				msg := fmt.Sprintf("audit query failed: %v", err)
				auditFail.CompareAndSwap(nil, &msg)
				return
			}
			if got != totalMoney() {
				msg := fmt.Sprintf("conservation violated mid-flight: SUM(balance)=%d, want %d", got, totalMoney())
				auditFail.CompareAndSwap(nil, &msg)
				return
			}
			// Each audit is a full-table SUM scan — O(accounts) — so sample at
			// a relaxed cadence: it stays an oracle for any persistent
			// conservation violation without dominating the workload at scale.
			time.Sleep(time.Duration(250+r.Intn(500)) * time.Millisecond)
		}
	}()

	var commits, noops int64
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < opsPerWorker; i++ {
				src, dst, amount := b.pickOperation(r)
				moved, err := b.transfer(ctx, src, dst, amount)
				if err != nil {
					errCh <- err
					return
				}
				if moved {
					atomic.AddInt64(&commits, 1)
				} else {
					atomic.AddInt64(&noops, 1)
				}
			}
		}(int64(w)*1_000_003 + 7)
	}
	wg.Wait()
	close(stopAudit)
	<-auditDone

	select {
	case err := <-errCh:
		_ = b.close()
		t.Fatalf("worker failed: %v", err)
	default:
	}
	if msg := auditFail.Load(); msg != nil {
		_ = b.close()
		t.Fatal(*msg)
	}
	t.Logf("workload done: %d committed transfers, %d no-ops", commits, noops)

	// Quiescent conservation: the live total must equal the mint.
	got, err := b.totalBalance(ctx)
	if err != nil {
		_ = b.close()
		t.Fatalf("final total: %v", err)
	}
	if got != totalMoney() {
		_ = b.close()
		t.Fatalf("final conservation violated: SUM(balance)=%d, want %d", got, totalMoney())
	}

	// Per-account agreement: the database must match the reference model
	// exactly. This catches a bug that conserves the total but moves money to
	// the wrong account (e.g. an index pointing at the wrong row).
	verifyModel(t, ctx, b)

	if err := b.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen (replays any unflushed tail) and re-check.
	b2 := openBank(t, dir, userCount)
	got2, err := b2.totalBalance(ctx)
	if err != nil {
		_ = b2.close()
		t.Fatalf("post-reopen total: %v", err)
	}
	if got2 != totalMoney() {
		_ = b2.close()
		t.Fatalf("conservation violated after reopen: SUM(balance)=%d, want %d", got2, totalMoney())
	}
	// Close before scrubbing: VerifyDir takes the directory lock, so the
	// database must not be open.
	if err := b2.close(); err != nil {
		t.Fatalf("close before scrub: %v", err)
	}
	scrub(t, dir)
}

// pickOperation chooses a transfer shape: mostly user->user payments (low
// contention at scale), with a minority of deposits and withdrawals through
// the bank pool (high-contention hot rows).
func (b *bank) pickOperation(r *rand.Rand) (src, dst int, amount int64) {
	amount = int64(1 + r.Intn(500))
	switch n := r.Intn(100); {
	case n < 80: // user -> user payment
		return b.randomAccount(r), b.randomAccount(r), amount
	case n < 90: // deposit: bank -> user
		return randomBank(r), b.randomAccount(r), amount
	default: // withdrawal: user -> bank
		return b.randomAccount(r), randomBank(r), amount
	}
}

// verifyModel scans every account once and compares its database balance to
// the reference model.
func verifyModel(t *testing.T, ctx context.Context, b *bank) {
	t.Helper()
	rs, err := b.db.Query(ctx, "SELECT id, balance FROM accounts")
	if err != nil {
		t.Fatalf("model check scan: %v", err)
	}
	seen := 0
	mismatches := 0
	b.model.mu.Lock()
	defer b.model.mu.Unlock()
	for _, row := range rs.Rows {
		id := int(row[0].(types.IntKey))
		got := int64(row[1].(types.IntKey))
		want, ok := b.model.bal[id]
		if !ok {
			t.Fatalf("database has account %d that the model never created", id)
		}
		seen++
		if got != want {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("account %d: database balance %d, model expects %d", id, got, want)
			}
		}
	}
	if seen != len(b.model.bal) {
		t.Fatalf("scan returned %d accounts, model has %d", seen, len(b.model.bal))
	}
	if mismatches > 0 {
		t.Fatalf("%d accounts diverged from the reference model", mismatches)
	}
}

func scrub(t *testing.T, dir string) {
	t.Helper()
	report, err := sql.VerifyDir(context.Background(), dir, sql.VerifyOptions{})
	if err != nil {
		t.Fatalf("scrub VerifyDir: %v", err)
	}
	if report.HasErrors() {
		t.Fatalf("scrub found structural errors: %v", report.Findings)
	}
	for _, f := range report.Findings {
		t.Logf("scrub warning: %v", f)
	}
}
