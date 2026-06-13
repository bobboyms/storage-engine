//go:build simulation

package simulation

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestBankCrashRecoveryConservation is the durability oracle: a child process
// runs the banking workload and is killed with SIGKILL at a random in-flight
// point, after a checkpoint has bounded the on-disk state so the crash falls
// in the unflushed/evicted tail — the exact shape that broke recovery before.
// The parent reopens the database (replaying the WAL) and asserts that money
// is still conserved and the structures scrub clean. A half-applied transfer
// would create or destroy money and fail the sum.
func TestBankCrashRecoveryConservation(t *testing.T) {
	if os.Getenv("STORAGE_ENGINE_SIM_CHILD") == "1" {
		runCrashChild(t)
		return
	}

	s := scale()
	userCount := 8_000 * s
	parentTmp := t.TempDir()
	dir := filepath.Join(parentTmp, "bankdb")
	progress := filepath.Join(parentTmp, "progress")

	cmd := exec.Command(os.Args[0], "-test.run", "^TestBankCrashRecoveryConservation$", "-test.v")
	cmd.Env = append(os.Environ(),
		"STORAGE_ENGINE_SIM_CHILD=1",
		"STORAGE_ENGINE_SIM_DIR="+dir,
		"STORAGE_ENGINE_SIM_PROGRESS="+progress,
		"STORAGE_ENGINE_SIM_USERS="+strconv.Itoa(userCount),
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}

	// Wait until the child has committed enough post-checkpoint transfers that
	// the WAL tail is non-trivial and the buffer pools have evicted.
	threshold := 400 * s
	if err := waitForProgress(progress, threshold, 90*time.Second); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("waiting for child progress: %v", err)
	}

	// Kill -9 mid-flight. The child holds the directory lock via flock, which
	// the OS releases on process death, so the parent can reopen afterwards.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill child: %v", err)
	}
	_ = cmd.Wait()

	// Reopen: recovery replays the unflushed transfer tail.
	ctx := context.Background()
	b := openBank(t, dir, userCount)
	defer func() { _ = b.close() }()

	got, err := b.totalBalance(ctx)
	if err != nil {
		t.Fatalf("post-crash total: %v", err)
	}
	if got != totalMoney() {
		t.Fatalf("conservation violated after crash recovery: SUM(balance)=%d, want %d (a transfer was half-applied)",
			got, totalMoney())
	}
	t.Logf("crash recovery preserved conservation: SUM(balance)=%d", got)

	// No account may hold a negative balance: that too would prove a transfer
	// debited without crediting (or recovery resurrected a stale version).
	assertNoNegativeBalances(t, ctx, b)

	if err := b.close(); err != nil {
		t.Fatalf("close after crash recovery: %v", err)
	}
	scrub(t, dir)
}

// runCrashchild is the helper-process body: build the world, checkpoint to
// bound on-disk state, then transfer forever, reporting committed counts via
// the progress file until the parent kills it.
func runCrashChild(t *testing.T) {
	dir := os.Getenv("STORAGE_ENGINE_SIM_DIR")
	progress := os.Getenv("STORAGE_ENGINE_SIM_PROGRESS")
	userCount, _ := strconv.Atoi(os.Getenv("STORAGE_ENGINE_SIM_USERS"))
	if dir == "" || progress == "" || userCount <= 0 {
		t.Fatalf("child missing env: dir=%q progress=%q users=%d", dir, progress, userCount)
	}

	ctx := context.Background()
	b := openBank(t, dir, userCount)
	if err := b.mint(ctx); err != nil {
		t.Fatalf("child mint: %v", err)
	}

	// Checkpoint so the mint (already funded) is durably flushed: everything
	// the crash window covers from here on is the never-checkpointed transfer
	// tail (the state index recovery must rebuild).
	if _, err := b.db.RunMaintenance(ctx); err != nil {
		t.Fatalf("child checkpoint: %v", err)
	}

	// Drive transfers across many goroutines, counting commits. The process
	// is expected to be killed; it never returns on its own.
	workers := 32
	var committed atomic.Int64
	for w := 0; w < workers; w++ {
		go func(seed int64) {
			r := rand.New(rand.NewSource(seed))
			for {
				src, dst, amount := b.pickOperation(r)
				moved, err := b.transfer(ctx, src, dst, amount)
				if err != nil {
					// A transient error here would only matter if it corrupted
					// state; surface it loudly via a poison progress value and
					// print the cause so failures are diagnosable.
					fmt.Fprintf(os.Stderr, "child transfer %d->%d error: %v\n", src, dst, err)
					writeProgress(progress, -1)
					return
				}
				if moved {
					committed.Add(1)
				}
			}
		}(int64(w)*2_654_435_761 + 11)
	}

	// Periodically publish the commit count for the parent to poll.
	for {
		writeProgress(progress, committed.Load())
		time.Sleep(10 * time.Millisecond)
	}
}

func writeProgress(path string, n int64) {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatInt(n, 10)), 0o600); err != nil {
		return
	}
	_ = os.Rename(tmp, path)
}

func waitForProgress(path string, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			if n, perr := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64); perr == nil {
				if n < 0 {
					return fmt.Errorf("child reported a transfer error (progress=-1)")
				}
				if int(n) >= want {
					return nil
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("child did not reach %d committed transfers before timeout", want)
}

func assertNoNegativeBalances(t *testing.T, ctx context.Context, b *bank) {
	t.Helper()
	rs, err := b.db.Query(ctx, "SELECT id, balance FROM accounts WHERE balance < ?", 0)
	if err != nil {
		t.Fatalf("negative-balance query: %v", err)
	}
	if len(rs.Rows) != 0 {
		t.Fatalf("%d accounts hold a negative balance after recovery (first: id=%v balance=%v)",
			len(rs.Rows), rs.Rows[0][0], rs.Rows[0][1])
	}
}
