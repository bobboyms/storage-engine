//go:build faults

package faults_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

func TestFaultEnvDirRequiresEnvWhenStrict(t *testing.T) {
	if os.Getenv("STORAGE_ENGINE_STRICT_FAULT_HELPER") == "1" {
		faultEnvDir(t, "STORAGE_ENGINE_TEST_FAULT_DIR", "skip local fault test")
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestFaultEnvDirRequiresEnvWhenStrict", "-test.v")
	cmd.Env = append(os.Environ(),
		"STORAGE_ENGINE_STRICT_FAULT_HELPER=1",
		"STORAGE_ENGINE_REQUIRE_ENV_FAULTS=1",
	)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected missing strict fault environment to fail, output:\n%s", output)
	}
	if !strings.Contains(string(output), "STORAGE_ENGINE_TEST_FAULT_DIR must be set") {
		t.Fatalf("expected failure to name missing env var, output:\n%s", output)
	}
}

func faultEnvDir(t testing.TB, envName, skipMessage string) string {
	t.Helper()

	dir := os.Getenv(envName)
	if dir != "" {
		return dir
	}

	if os.Getenv("STORAGE_ENGINE_REQUIRE_ENV_FAULTS") == "1" {
		t.Fatalf("%s must be set for required environmental fault tests", envName)
	}

	t.Skip(skipMessage)
	return ""
}

func TestFaultENOSPCClassifierRejectsBufferPoolFull(t *testing.T) {
	err := fmt.Errorf("heap write failed: %w", pagestore.ErrBufferPoolFull)
	if isENOSPC(err) {
		t.Fatalf("buffer pool exhaustion must not be treated as ENOSPC: %v", err)
	}
}

func TestFaultENOSPCClassifierRejectsPathOnlyENOSPC(t *testing.T) {
	err := errors.New("write /tmp/storage-engine-enospc/pages.db: file too large")
	if isENOSPC(err) {
		t.Fatalf("path text containing enospc must not be treated as ENOSPC: %v", err)
	}
}

func isENOSPC(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ENOSPC) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no space left on device")
}

func isEIO(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EIO) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "input/output error") || strings.Contains(msg, "i/o error")
}

func faultWALEntry(lsn uint64, payload []byte) *wal.WALEntry {
	entry := wal.AcquireEntry()
	entry.Header.Magic = wal.WALMagic
	entry.Header.Version = wal.WALVersion
	entry.Header.EntryType = wal.EntryInsert
	entry.Header.LSN = lsn
	entry.Header.PayloadLen = uint32(len(payload)) //nolint:gosec // fault payload size is test-controlled
	entry.Header.CRC32 = wal.CalculateCRC32(payload)
	entry.Payload = append(entry.Payload[:0], payload...)
	return entry
}

type dbPaths struct {
	dir       string
	walPath   string
	heapPath  string
	btreePath string
}

func pathsFor(dir string) dbPaths {
	return dbPaths{
		dir:       dir,
		walPath:   filepath.Join(dir, "wal.log"),
		heapPath:  filepath.Join(dir, "table.heap.v2"),
		btreePath: filepath.Join(dir, "id.btree.v2"),
	}
}

func openEngine(t testing.TB, p dbPaths) *storage.StorageEngine {
	t.Helper()

	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, p.heapPath)
	if err != nil {
		t.Fatalf("open heap: %v", err)
	}
	idxTree, err := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, p.btreePath, nil)
	if err != nil {
		t.Fatalf("open btree: %v", err)
	}
	tm := storage.NewTableMenager()
	if err := tm.NewTable("t", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree},
	}, 0, hm); err != nil {
		t.Fatalf("create table metadata: %v", err)
	}
	ww, err := wal.NewWALWriter(p.walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	se, err := storage.NewProductionStorageEngine(tm, ww)
	if err != nil {
		_ = ww.Close()
		t.Fatalf("open production engine: %v", err)
	}
	return se
}

func seedAndClose(t testing.TB, p dbPaths, n int) {
	t.Helper()
	se := openEngine(t, p)
	for i := 1; i <= n; i++ {
		doc := fmt.Sprintf(`{"id":%d,"value":"v%d"}`, i, i)
		if err := se.Put(context.Background(), "t", "id", types.IntKey(int64(i)), doc); err != nil {
			_ = se.Close()
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := se.Close(); err != nil {
		t.Fatalf("close seed engine: %v", err)
	}
}

func flipByte(t testing.TB, path string, offset int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open %s for corruption: %v", path, err)
	}
	defer f.Close()

	buf := []byte{0}
	if _, err := f.ReadAt(buf, offset); err != nil {
		t.Fatalf("read byte at %s:%d: %v", path, offset, err)
	}
	buf[0] ^= 0x80
	if _, err := f.WriteAt(buf, offset); err != nil {
		t.Fatalf("write corrupt byte at %s:%d: %v", path, offset, err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync corrupt file %s: %v", path, err)
	}
}

func TestFaultWALPageCorruptionFailsRecovery(t *testing.T) {
	p := pathsFor(t.TempDir())
	seedAndClose(t, p, 25)

	flipByte(t, p.walPath, int64(pagestore.PageSize+pagestore.HeaderSize+16))

	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, p.heapPath)
	if err != nil {
		t.Fatalf("open heap: %v", err)
	}
	idxTree, err := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, p.btreePath, nil)
	if err != nil {
		t.Fatalf("open btree: %v", err)
	}
	tm := storage.NewTableMenager()
	if err := tm.NewTable("t", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree},
	}, 0, hm); err != nil {
		t.Fatalf("table metadata: %v", err)
	}
	ww, err := wal.NewWALWriter(p.walPath, wal.DefaultOptions())
	if err != nil {
		if errors.Is(err, pagestore.ErrChecksumMismatch) {
			return
		}
		t.Fatalf("open wal writer: %v", err)
	}
	se, err := storage.NewStorageEngine(tm, ww)
	if err == nil {
		err = se.Recover(context.Background(), p.walPath)
	}
	if se != nil {
		_ = se.Close()
	} else {
		_ = ww.Close()
	}
	if err == nil {
		t.Fatal("expected WAL corruption to fail startup/recovery")
	}
}

func TestFaultHeapPageCorruptionRecoveredFromWAL(t *testing.T) {
	p := pathsFor(t.TempDir())
	seedAndClose(t, p, 10)

	flipByte(t, p.heapPath, int64(pagestore.PageSize+pagestore.HeaderSize+32))

	se, err := tryOpenEngine(p)
	if err != nil {
		t.Fatalf("open engine after heap corruption: %v", err)
	}
	defer se.Close()

	raw, found, err := se.GetBytes(context.Background(), "t", "id", types.IntKey(1))
	if err != nil {
		t.Fatalf("expected WAL-based heap recovery, got read error: %v", err)
	}
	if !found {
		t.Fatal("expected heap page corruption to be repaired from WAL")
	}
	if len(raw) == 0 {
		t.Fatal("expected recovered document payload after heap page repair")
	}
}

func tryOpenEngine(p dbPaths) (*storage.StorageEngine, error) {
	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, p.heapPath)
	if err != nil {
		return nil, err
	}
	idxTree, err := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, p.btreePath, nil)
	if err != nil {
		return nil, err
	}
	tm := storage.NewTableMenager()
	if err := tm.NewTable("t", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree},
	}, 0, hm); err != nil {
		return nil, err
	}
	ww, err := wal.NewWALWriter(p.walPath, wal.DefaultOptions())
	if err != nil {
		return nil, err
	}
	se, err := storage.NewProductionStorageEngine(tm, ww)
	if err != nil {
		_ = ww.Close()
		return nil, err
	}
	return se, nil
}

func TestFaultBTreePageCorruptionDetectedOnOpenOrRead(t *testing.T) {
	p := pathsFor(t.TempDir())
	seedAndClose(t, p, 10)

	flipByte(t, p.btreePath, int64(pagestore.PageSize+pagestore.HeaderSize+32))

	hm, err := storage.NewHeapForTable(storage.HeapFormatV2, p.heapPath)
	if err != nil {
		t.Fatalf("open heap: %v", err)
	}
	idxTree, err := storage.NewBTreeForIndex(storage.BTreeFormatV2, true, storage.TypeInt, p.btreePath, nil)
	if err == nil {
		tm := storage.NewTableMenager()
		if tableErr := tm.NewTable("t", []storage.Index{
			{Name: "id", Primary: true, Type: storage.TypeInt, Tree: idxTree},
		}, 0, hm); tableErr != nil {
			t.Fatalf("table metadata: %v", tableErr)
		}
		ww, walErr := wal.NewWALWriter(p.walPath, wal.DefaultOptions())
		if walErr != nil {
			t.Fatalf("open wal: %v", walErr)
		}
		se, engineErr := storage.NewProductionStorageEngine(tm, ww)
		if engineErr == nil {
			_, _, engineErr = se.GetBytes(context.Background(), "t", "id", types.IntKey(1))
			_ = se.Close()
		} else {
			_ = ww.Close()
		}
		err = engineErr
	}
	if err == nil {
		t.Fatal("expected btree page corruption to fail open/recovery/read")
	}
}

func TestFaultENOSPCOnConstrainedFilesystem(t *testing.T) {
	dir := faultEnvDir(t,
		"STORAGE_ENGINE_ENOSPC_DIR",
		"set STORAGE_ENGINE_ENOSPC_DIR to a small mounted filesystem to run real ENOSPC test",
	)

	testDir := filepath.Join(dir, "storage-engine-enospc-"+strconvLikeTime())
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("create constrained db dir: %v", err)
	}
	defer os.RemoveAll(testDir)

	pf, err := pagestore.NewPageFile(filepath.Join(testDir, "pages.db"), nil)
	if err != nil {
		t.Fatalf("open page file on constrained filesystem: %v", err)
	}
	defer pf.Close()

	var page pagestore.Page
	for i := range page.Body() {
		page.Body()[i] = byte(i%251 + 1)
	}
	for i := 1; i <= 1_000_000; i++ {
		id, err := pf.AllocatePage()
		if err != nil {
			t.Fatalf("allocate page before ENOSPC: %v", err)
		}
		err = pf.WritePage(id, &page)
		if err == nil {
			continue
		}
		if isENOSPC(err) {
			t.Logf("observed expected ENOSPC after %d page writes: %v", i, err)
			return
		}
		t.Fatalf("expected ENOSPC from constrained filesystem, got unrelated error after %d page writes: %v", i, err)
	}
	if err := pf.Sync(); isENOSPC(err) {
		t.Logf("observed expected ENOSPC during sync: %v", err)
		return
	} else if err != nil {
		t.Fatalf("expected ENOSPC from constrained filesystem, got unrelated sync error: %v", err)
	}
	t.Fatal("expected ENOSPC on constrained filesystem, but page writes did not fail")
}

func TestFaultWALENOSPCOnConstrainedFilesystem(t *testing.T) {
	dir := faultEnvDir(t,
		"STORAGE_ENGINE_ENOSPC_DIR",
		"set STORAGE_ENGINE_ENOSPC_DIR to a small mounted filesystem to run real WAL ENOSPC test",
	)

	testDir := filepath.Join(dir, "storage-engine-wal-enospc-"+strconvLikeTime())
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("create constrained wal dir: %v", err)
	}
	defer os.RemoveAll(testDir)

	ww, err := wal.NewWALWriter(filepath.Join(testDir, "wal.log"), wal.DefaultOptions())
	if err != nil {
		t.Fatalf("open WAL on constrained filesystem: %v", err)
	}
	defer ww.Close()

	payload := make([]byte, 7000)
	for i := range payload {
		payload[i] = byte(i%251 + 1)
	}
	for i := 1; i <= 1_000_000; i++ {
		entry := faultWALEntry(uint64(i), payload)
		err := ww.WriteEntry(entry)
		wal.ReleaseEntry(entry)
		if err == nil {
			continue
		}
		if isENOSPC(err) {
			t.Logf("observed expected WAL ENOSPC after %d entries: %v", i, err)
			return
		}
		t.Fatalf("expected WAL ENOSPC, got unrelated error after %d entries: %v", i, err)
	}
	t.Fatal("expected WAL ENOSPC on constrained filesystem, but writes did not fail")
}

func TestFaultWALFsyncFailureOnFaultingFilesystem(t *testing.T) {
	dir := faultEnvDir(t,
		"STORAGE_ENGINE_FSYNC_FAIL_DIR",
		"set STORAGE_ENGINE_FSYNC_FAIL_DIR to enable WAL fsync fault injection",
	)
	markerPath := filepath.Join(dir, ".fail_fsync_now")
	_ = os.Remove(markerPath)

	testDir := filepath.Join(dir, "storage-engine-wal-fsync-"+strconvLikeTime())
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("create faulting wal dir: %v", err)
	}
	defer os.RemoveAll(testDir)

	ww, err := wal.NewWALWriter(filepath.Join(testDir, "wal.log"), wal.DefaultOptions())
	if err != nil {
		t.Fatalf("open WAL before fsync fault: %v", err)
	}
	if err := os.WriteFile(markerPath, []byte("1"), 0644); err != nil {
		_ = ww.Close()
		t.Fatalf("enable WAL fsync fault injection: %v", err)
	}
	entry := faultWALEntry(1, []byte("payload"))
	err = ww.WriteEntry(entry)
	wal.ReleaseEntry(entry)
	_ = os.Remove(markerPath)
	closeErr := ww.Close()
	if err == nil {
		t.Fatalf("expected WAL write to observe injected fsync failure, closeErr=%v", closeErr)
	}
	if !isEIO(err) {
		t.Fatalf("expected injected WAL fsync EIO, got: %v", err)
	}
}

func TestFaultEngineWALFsyncFailureDoesNotMutateVisibleState(t *testing.T) {
	dir := faultEnvDir(t,
		"STORAGE_ENGINE_FSYNC_FAIL_DIR",
		"set STORAGE_ENGINE_FSYNC_FAIL_DIR to enable engine WAL fsync fault injection",
	)
	markerPath := filepath.Join(dir, ".fail_fsync_now")
	_ = os.Remove(markerPath)

	p := pathsFor(filepath.Join(dir, "storage-engine-engine-fsync-"+strconvLikeTime()))
	if err := os.MkdirAll(p.dir, 0755); err != nil {
		t.Fatalf("create faulting engine dir: %v", err)
	}
	defer os.RemoveAll(p.dir)

	se := openEngine(t, p)
	if err := se.Put(context.Background(), "t", "id", types.IntKey(1), `{"id":1,"value":"stable"}`); err != nil {
		_ = se.Close()
		t.Fatalf("seed stable row before fsync fault: %v", err)
	}

	if err := os.WriteFile(markerPath, []byte("1"), 0644); err != nil {
		_ = se.Close()
		t.Fatalf("enable engine WAL fsync fault injection: %v", err)
	}
	err := se.Put(context.Background(), "t", "id", types.IntKey(2), `{"id":2,"value":"failed"}`)
	if err == nil {
		_ = os.Remove(markerPath)
		_ = se.Close()
		t.Fatal("expected engine Put to observe injected WAL fsync failure")
	}
	if !isEIO(err) {
		_ = os.Remove(markerPath)
		_ = se.Close()
		t.Fatalf("expected injected engine WAL fsync EIO, got: %v", err)
	}

	if _, found, getErr := se.GetBytes(context.Background(), "t", "id", types.IntKey(1)); getErr != nil || !found {
		_ = os.Remove(markerPath)
		_ = se.Close()
		t.Fatalf("stable row must remain visible after failed WAL fsync, found=%v err=%v", found, getErr)
	}
	if _, found, getErr := se.GetBytes(context.Background(), "t", "id", types.IntKey(2)); getErr != nil || found {
		_ = os.Remove(markerPath)
		_ = se.Close()
		t.Fatalf("failed WAL fsync row must not become visible, found=%v err=%v", found, getErr)
	}

	_ = os.Remove(markerPath)
	// Fail-stop: once an fsync fails the kernel may have dropped dirty
	// pages, so the WAL writer stays poisoned even after the fault clears.
	// Close must surface that instead of pretending the tail is durable.
	closeErr := se.Close()
	if closeErr == nil {
		t.Fatal("expected Close to surface poisoned WAL writer after fsync failure")
	}
	if !strings.Contains(closeErr.Error(), "poisoned by fsync failure") {
		t.Fatalf("expected poisoned-writer error from Close, got: %v", closeErr)
	}

	// Recovery path: a fresh engine over the same files must replay the WAL
	// and still see the row committed before the fault. The row whose fsync
	// failed has indeterminate outcome (commit bytes may have reached the
	// file), so it is intentionally not asserted here.
	se2 := openEngine(t, p)
	defer func() {
		if err := se2.Close(); err != nil {
			t.Errorf("close recovered engine: %v", err)
		}
	}()
	if err := se2.Recover(context.Background(), p.walPath); err != nil {
		t.Fatalf("recover after poisoned close: %v", err)
	}
	if _, found, getErr := se2.GetBytes(context.Background(), "t", "id", types.IntKey(1)); getErr != nil || !found {
		t.Fatalf("stable row must survive recovery after fsync fault, found=%v err=%v", found, getErr)
	}
}

func TestFaultFsyncFailureOnFaultingFilesystem(t *testing.T) {
	dir := faultEnvDir(t,
		"STORAGE_ENGINE_FSYNC_FAIL_DIR",
		"set STORAGE_ENGINE_FSYNC_FAIL_DIR to enable fsync fault injection",
	)
	markerPath := filepath.Join(dir, ".fail_fsync_now")
	_ = os.Remove(markerPath)

	p := pathsFor(filepath.Join(dir, "storage-engine-fsync-"+strconvLikeTime()))
	if err := os.MkdirAll(p.dir, 0755); err != nil {
		t.Fatalf("create faulting db dir: %v", err)
	}
	defer os.RemoveAll(p.dir)

	se := openEngine(t, p)
	if err := os.WriteFile(markerPath, []byte("1"), 0644); err != nil {
		_ = se.Close()
		t.Fatalf("enable fsync fault injection: %v", err)
	}
	err := se.Put(context.Background(), "t", "id", types.IntKey(1), `{"id":1}`)
	closeErr := se.Close()
	if err == nil && closeErr == nil {
		t.Fatal("expected put or close to observe injected fsync failure")
	}
}

func strconvLikeTime() string {
	return fmt.Sprintf("%d", os.Getpid())
}
