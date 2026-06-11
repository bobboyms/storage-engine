package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func requireAccount(t *testing.T, se *StorageEngine, id int64, wantFound bool) {
	t.Helper()
	_, found, err := se.GetBytes(context.Background(), "accounts", "id", types.IntKey(id))
	if err != nil {
		t.Fatalf("GetBytes account %d: %v", id, err)
	}
	if found != wantFound {
		t.Fatalf("account %d: found=%v, want %v", id, found, wantFound)
	}
}

// TestPointInTimeRecovery_RestoreToLSN exercises the full PITR story: a
// backup taken early, more commits afterwards, then a logical disaster.
// Restoring the backup and replaying the original WAL bounded at the LSN
// just before the disaster must yield exactly the committed state at that
// point — later commits are discarded — and the restored engine must keep
// working with a fresh WAL (no LSN collision with the discarded tail).
func TestPointInTimeRecovery_RestoreToLSN(t *testing.T) {
	ctx := context.Background()
	src := filepath.Join(t.TempDir(), "db")
	db := newBackupTestDB(t, src)

	putAccount(t, db.engine, 1, "one@example.com")

	backupDir := filepath.Join(t.TempDir(), "backup")
	if _, err := db.engine.BackupOnline(ctx, backupDir); err != nil {
		t.Fatalf("BackupOnline: %v", err)
	}

	putAccount(t, db.engine, 2, "two@example.com")
	targetLSN := db.engine.Stats().CurrentLSN

	// The disaster to be discarded by PITR.
	putAccount(t, db.engine, 3, "three@example.com")

	if err := db.engine.Close(); err != nil {
		t.Fatalf("close source engine: %v", err)
	}

	// Restore the backup into a new directory.
	restoreDir := filepath.Join(t.TempDir(), "restored")
	if _, err := RestoreBackup(ctx, backupDir, restoreDir); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}

	// Build an engine over the restored files with a FRESH WAL: the
	// discarded tail stays in the old log, so new writes cannot collide
	// with LSNs beyond the target.
	hm, err := NewHeapForTable(HeapFormatV2, filepath.Join(restoreDir, "accounts.heap"))
	if err != nil {
		t.Fatalf("NewHeapForTable restored: %v", err)
	}
	meta := NewTableMenager()
	if err := meta.NewTable("accounts", []Index{
		{Name: "id", Primary: true, Type: TypeInt},
		{Name: "email", Type: TypeVarchar},
	}, 0, hm); err != nil {
		t.Fatalf("NewTable restored: %v", err)
	}
	ww, err := wal.NewWALWriter(filepath.Join(restoreDir, "accounts_pitr.wal"), wal.DefaultOptions())
	if err != nil {
		t.Fatalf("NewWALWriter restored: %v", err)
	}
	se, err := NewStorageEngine(meta, ww)
	if err != nil {
		t.Fatalf("NewStorageEngine restored: %v", err)
	}
	defer func() { _ = ww.Close() }()

	// Bounded replay of the ORIGINAL WAL up to the pre-disaster LSN.
	if err := se.RecoverToLSN(ctx, db.walPath, targetLSN); err != nil {
		t.Fatalf("RecoverToLSN: %v", err)
	}

	requireAccount(t, se, 1, true)
	requireAccount(t, se, 2, true)
	requireAccount(t, se, 3, false)

	// The restored engine must accept new commits and they must be readable.
	putAccount(t, se, 4, "four@example.com")
	requireAccount(t, se, 4, true)

	if se.Stats().CurrentLSN <= targetLSN {
		t.Fatalf("post-PITR LSN must advance past the target: current=%d target=%d",
			se.Stats().CurrentLSN, targetLSN)
	}
}

// TestPointInTimeRecovery_UnboundedMatchesFullReplay pins the contract
// that RecoverToLSN with the max LSN of the log behaves like a full
// recovery (nothing is lost by going through the bounded path).
func TestPointInTimeRecovery_UnboundedMatchesFullReplay(t *testing.T) {
	ctx := context.Background()
	src := filepath.Join(t.TempDir(), "db")
	db := newBackupTestDB(t, src)

	for i := int64(1); i <= 3; i++ {
		putAccount(t, db.engine, i, fmt.Sprintf("user-%d@example.com", i))
	}
	maxLSN := db.engine.Stats().CurrentLSN
	if err := db.engine.Close(); err != nil {
		t.Fatalf("close source engine: %v", err)
	}

	hm, err := NewHeapForTable(HeapFormatV2, filepath.Join(t.TempDir(), "fresh.heap"))
	if err != nil {
		t.Fatalf("NewHeapForTable: %v", err)
	}
	meta := NewTableMenager()
	if err := meta.NewTable("accounts", []Index{
		{Name: "id", Primary: true, Type: TypeInt},
		{Name: "email", Type: TypeVarchar},
	}, 0, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	ww, err := wal.NewWALWriter(filepath.Join(t.TempDir(), "fresh.wal"), wal.DefaultOptions())
	if err != nil {
		t.Fatalf("NewWALWriter: %v", err)
	}
	se, err := NewStorageEngine(meta, ww)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	defer func() { _ = ww.Close() }()

	if err := se.RecoverToLSN(ctx, db.walPath, maxLSN); err != nil {
		t.Fatalf("RecoverToLSN: %v", err)
	}
	for i := int64(1); i <= 3; i++ {
		requireAccount(t, se, i, true)
	}
}
