package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

type backupTestDB struct {
	dir      string
	heapPath string
	walPath  string
	engine   *StorageEngine
}

func newBackupTestDB(t *testing.T, dir string) *backupTestDB {
	t.Helper()

	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatalf("MkdirAll db dir: %v", err)
	}

	heapPath := filepath.Join(dir, "accounts.heap")
	walPath := filepath.Join(dir, "accounts.wal")

	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
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
	opts := wal.DefaultOptions()
	opts.MaxSegmentBytes = 4 * 1024
	ww, err := wal.NewWALWriter(walPath, opts)
	if err != nil {
		t.Fatalf("NewWALWriter: %v", err)
	}
	se, err := NewProductionStorageEngine(meta, ww)
	if err != nil {
		t.Fatalf("NewProductionStorageEngine: %v", err)
	}
	return &backupTestDB{dir: dir, heapPath: heapPath, walPath: walPath, engine: se}
}

func reopenBackupTestDB(t *testing.T, dir string) *StorageEngine {
	t.Helper()

	hm, err := NewHeapForTable(HeapFormatV2, filepath.Join(dir, "accounts.heap"))
	if err != nil {
		t.Fatalf("NewHeapForTable restore: %v", err)
	}
	meta := NewTableMenager()
	if err := meta.NewTable("accounts", []Index{
		{Name: "id", Primary: true, Type: TypeInt},
		{Name: "email", Type: TypeVarchar},
	}, 0, hm); err != nil {
		t.Fatalf("NewTable restore: %v", err)
	}
	ww, err := wal.NewWALWriter(filepath.Join(dir, "accounts.wal"), wal.DefaultOptions())
	if err != nil {
		t.Fatalf("NewWALWriter restore: %v", err)
	}
	se, err := NewProductionStorageEngine(meta, ww)
	if err != nil {
		t.Fatalf("NewProductionStorageEngine restore: %v", err)
	}
	return se
}

func putAccount(t *testing.T, se *StorageEngine, id int64, email string) {
	t.Helper()

	doc := fmt.Sprintf(`{"id":%d,"email":%q,"balance":%d}`, id, email, id*10)
	if err := se.Put("accounts", "id", types.IntKey(id), doc); err != nil {
		t.Fatalf("Put account %d: %v", id, err)
	}
}

func TestOnlineBackupRestoreRoundTrip(t *testing.T) {
	src := filepath.Join(t.TempDir(), "db")
	db := newBackupTestDB(t, src)
	defer db.engine.Close()

	for i := int64(1); i <= 5; i++ {
		putAccount(t, db.engine, i, fmt.Sprintf("user-%d@example.com", i))
	}

	backupDir := filepath.Join(t.TempDir(), "backup")
	manifest, err := db.engine.BackupOnline(backupDir)
	if err != nil {
		t.Fatalf("BackupOnline: %v", err)
	}
	if manifest.CheckpointLSN == 0 {
		t.Fatal("BackupOnline não registrou LSN de checkpoint")
	}
	if len(manifest.Files) < 3 {
		t.Fatalf("backup deveria conter heap, índices e WAL; arquivos=%d", len(manifest.Files))
	}

	putAccount(t, db.engine, 6, "after-backup@example.com")

	if _, err := VerifyBackup(backupDir); err != nil {
		t.Fatalf("VerifyBackup: %v", err)
	}

	restoreDir := filepath.Join(t.TempDir(), "restore")
	if _, err := RestoreBackup(backupDir, restoreDir); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}

	restored := reopenBackupTestDB(t, restoreDir)
	defer restored.Close()

	for i := int64(1); i <= 5; i++ {
		got, ok, err := restored.Get("accounts", "id", types.IntKey(i))
		if err != nil {
			t.Fatalf("Get restored %d: %v", i, err)
		}
		if !ok || !strings.Contains(got, fmt.Sprintf("user-%d@example.com", i)) {
			t.Fatalf("registro %d ausente no restore: ok=%v got=%s", i, ok, got)
		}
	}
	if got, ok, err := restored.Get("accounts", "id", types.IntKey(6)); err != nil {
		t.Fatalf("Get post-backup: %v", err)
	} else if ok {
		t.Fatalf("restore incluiu escrita posterior ao backup: %s", got)
	}
}

func TestVerifyBackupDetectsCorruption(t *testing.T) {
	db := newBackupTestDB(t, filepath.Join(t.TempDir(), "db"))
	defer db.engine.Close()
	putAccount(t, db.engine, 1, "corrupt@example.com")

	backupDir := filepath.Join(t.TempDir(), "backup")
	manifest, err := db.engine.BackupOnline(backupDir)
	if err != nil {
		t.Fatalf("BackupOnline: %v", err)
	}
	if len(manifest.Files) == 0 {
		t.Fatal("manifest sem arquivos")
	}

	first := filepath.Join(backupDir, backupFilesDirName, filepath.FromSlash(manifest.Files[0].Path))
	f, err := os.OpenFile(first, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatalf("OpenFile corrupt: %v", err)
	}
	if _, err := f.Write([]byte("corruption")); err != nil {
		f.Close()
		t.Fatalf("Write corrupt: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close corrupt: %v", err)
	}

	if _, err := VerifyBackup(backupDir); err == nil {
		t.Fatal("VerifyBackup deveria detectar corrupção")
	}
	if _, err := RestoreBackup(backupDir, filepath.Join(t.TempDir(), "restore")); err == nil {
		t.Fatal("RestoreBackup deveria rejeitar backup corrompido")
	}
}

func TestOnlineBackupWhileWritesAreRunning(t *testing.T) {
	db := newBackupTestDB(t, filepath.Join(t.TempDir(), "db"))
	defer db.engine.Close()

	var nextID int64
	var writeErrors int64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			id := atomic.AddInt64(&nextID, 1)
			doc := fmt.Sprintf(`{"id":%d,"email":"live-%d@example.com","balance":%d}`, id, id, id)
			if err := db.engine.Put("accounts", "id", types.IntKey(id), doc); err != nil {
				atomic.AddInt64(&writeErrors, 1)
				return
			}
		}
	}()

	for atomic.LoadInt64(&nextID) < 20 {
		time.Sleep(time.Millisecond)
	}

	backupDir := filepath.Join(t.TempDir(), "backup")
	if _, err := db.engine.BackupOnline(backupDir); err != nil {
		close(stop)
		<-done
		t.Fatalf("BackupOnline com writes concorrentes: %v", err)
	}

	close(stop)
	<-done
	if n := atomic.LoadInt64(&writeErrors); n != 0 {
		t.Fatalf("write goroutine falhou %d vez(es)", n)
	}
	if _, err := VerifyBackup(backupDir); err != nil {
		t.Fatalf("VerifyBackup: %v", err)
	}
	if _, err := RestoreBackup(backupDir, filepath.Join(t.TempDir(), "restore")); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
}

func TestRestoreBackupDoesNotOverwriteExistingFiles(t *testing.T) {
	db := newBackupTestDB(t, filepath.Join(t.TempDir(), "db"))
	defer db.engine.Close()
	putAccount(t, db.engine, 1, "exists@example.com")

	backupDir := filepath.Join(t.TempDir(), "backup")
	if _, err := db.engine.BackupOnline(backupDir); err != nil {
		t.Fatalf("BackupOnline: %v", err)
	}

	targetDir := filepath.Join(t.TempDir(), "restore")
	if err := os.MkdirAll(targetDir, 0700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(targetDir, "accounts.heap"), []byte("already here"), 0600); err != nil {
		t.Fatalf("WriteFile existing: %v", err)
	}
	if _, err := RestoreBackup(backupDir, targetDir); err == nil {
		t.Fatal("RestoreBackup deveria recusar sobrescrever arquivo existente")
	}
}
