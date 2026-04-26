//go:build faults

package faults_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

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
		if err := se.Put("t", "id", types.IntKey(int64(i)), doc); err != nil {
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
		err = se.Recover(p.walPath)
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

	doc, found, err := se.Get("t", "id", types.IntKey(1))
	if err != nil {
		t.Fatalf("expected WAL-based heap recovery, got read error: %v", err)
	}
	if !found {
		t.Fatal("expected heap page corruption to be repaired from WAL")
	}
	if doc == "" {
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
			_, _, engineErr = se.Get("t", "id", types.IntKey(1))
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
	dir := os.Getenv("STORAGE_ENGINE_ENOSPC_DIR")
	if dir == "" {
		t.Skip("set STORAGE_ENGINE_ENOSPC_DIR to a small mounted filesystem to run real ENOSPC test")
	}

	p := pathsFor(filepath.Join(dir, "storage-engine-enospc-"+strconvLikeTime()))
	if err := os.MkdirAll(p.dir, 0755); err != nil {
		t.Fatalf("create constrained db dir: %v", err)
	}
	defer os.RemoveAll(p.dir)

	se := openEngine(t, p)
	defer se.Close()

	payload := make([]byte, 7000)
	for i := range payload {
		payload[i] = 'x'
	}
	for i := 1; i <= 1_000_000; i++ {
		doc := fmt.Sprintf(`{"id":%d,"payload":"%s"}`, i, payload)
		err := se.Put("t", "id", types.IntKey(int64(i)), doc)
		if err != nil {
			t.Logf("observed expected write failure after %d inserts: %v", i, err)
			return
		}
	}
	t.Fatal("expected ENOSPC/write failure on constrained filesystem, but writes did not fail")
}

func TestFaultFsyncFailureOnFaultingFilesystem(t *testing.T) {
	dir := os.Getenv("STORAGE_ENGINE_FSYNC_FAIL_DIR")
	if dir == "" {
		t.Skip("set STORAGE_ENGINE_FSYNC_FAIL_DIR to enable fsync fault injection")
	}
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
	err := se.Put("t", "id", types.IntKey(1), `{"id":1}`)
	closeErr := se.Close()
	if err == nil && closeErr == nil {
		t.Fatal("expected put or close to observe injected fsync failure")
	}
}

func strconvLikeTime() string {
	return fmt.Sprintf("%d", os.Getpid())
}
