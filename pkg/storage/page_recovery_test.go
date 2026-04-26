package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

type recoveryFixture struct {
	tableName string
	walPath   string
	heapPath  string
	indexPath string
}

func newRecoveryFixture(dir, tableName string) recoveryFixture {
	return recoveryFixture{
		tableName: tableName,
		walPath:   filepath.Join(dir, "wal.log"),
		heapPath:  filepath.Join(dir, tableName+".heap"),
		indexPath: filepath.Join(dir, tableName+".btree"),
	}
}

func openRecoveryEngine(t *testing.T, fx recoveryFixture, production bool) *StorageEngine {
	t.Helper()

	hm, err := NewHeapForTable(HeapFormatV2, fx.heapPath)
	if err != nil {
		t.Fatalf("NewHeapForTable: %v", err)
	}
	idxTree, err := NewBTreeForIndex(BTreeFormatV2, true, TypeInt, fx.indexPath, nil)
	if err != nil {
		t.Fatalf("NewBTreeForIndex: %v", err)
	}

	meta := NewTableMenager()
	if err := meta.NewTable(fx.tableName, []Index{
		{Name: "id", Primary: true, Type: TypeInt, Tree: idxTree},
	}, 0, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}

	ww, err := wal.NewWALWriter(fx.walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("NewWALWriter: %v", err)
	}

	if production {
		se, err := NewProductionStorageEngine(meta, ww)
		if err != nil {
			ww.Close()
			t.Fatalf("NewProductionStorageEngine: %v", err)
		}
		return se
	}

	se, err := NewStorageEngine(meta, ww)
	if err != nil {
		ww.Close()
		t.Fatalf("NewStorageEngine: %v", err)
	}
	return se
}

func corruptPageBody(t *testing.T, path string, pageID pagestore.PageID, bodyOffset int, bytes []byte) {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}

	pageStart := int(pageID) * pagestore.PageSize
	pos := pageStart + pagestore.HeaderSize + bodyOffset
	if pos+len(bytes) > len(raw) {
		t.Fatalf("corruption target out of range: path=%s pos=%d len=%d file=%d", path, pos, len(bytes), len(raw))
	}
	copy(raw[pos:pos+len(bytes)], bytes)
	if err := os.WriteFile(path, raw, 0644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func countVersionChain(t *testing.T, se *StorageEngine, tableName, indexName string, key types.Comparable) int {
	t.Helper()

	table, err := se.TableMetaData.GetTableByName(tableName)
	if err != nil {
		t.Fatalf("GetTableByName: %v", err)
	}
	index, err := table.GetIndex(indexName)
	if err != nil {
		t.Fatalf("GetIndex: %v", err)
	}

	offset, found, err := index.Tree.Get(key)
	if err != nil {
		t.Fatalf("Tree.Get: %v", err)
	}
	if !found {
		return 0
	}

	count := 0
	for offset != -1 {
		_, hdr, err := table.Heap.Read(offset)
		if err != nil {
			t.Fatalf("Heap.Read(offset=%d): %v", offset, err)
		}
		count++
		offset = hdr.PrevRecordID
	}
	return count
}

func requireDocumentVisible(t *testing.T, se *StorageEngine, tableName string, key int, want string) {
	t.Helper()

	got, found, err := se.Get(tableName, "id", types.IntKey(key))
	if err != nil {
		t.Fatalf("Get(%d): %v", key, err)
	}
	if !found {
		t.Fatalf("Get(%d): document missing", key)
	}
	if got != want {
		t.Fatalf("Get(%d): got %q want %q", key, got, want)
	}
}

func TestRecovery_RepeatedRecoveriesDoNotDuplicateVersions(t *testing.T) {
	dir := t.TempDir()
	fx := newRecoveryFixture(dir, "users")

	func() {
		se := openRecoveryEngine(t, fx, false)
		if err := se.Put("users", "id", types.IntKey(1), `{"id":1,"name":"alice"}`); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if err := se.WAL.Close(); err != nil {
			t.Fatalf("WAL.Close: %v", err)
		}
	}()

	recovered := openRecoveryEngine(t, fx, false)
	defer recovered.Close()

	if err := recovered.Recover(fx.walPath); err != nil {
		t.Fatalf("Recover #1: %v", err)
	}
	if err := recovered.Recover(fx.walPath); err != nil {
		t.Fatalf("Recover #2: %v", err)
	}
	if err := recovered.Recover(fx.walPath); err != nil {
		t.Fatalf("Recover #3: %v", err)
	}

	requireDocumentVisible(t, recovered, "users", 1, `{"id":1,"name":"alice"}`)
	if versions := countVersionChain(t, recovered, "users", "id", types.IntKey(1)); versions != 1 {
		t.Fatalf("expected exactly 1 version after repeated recovery, got %d", versions)
	}
}

func TestRecovery_RepairsTornHeapPageFromWAL(t *testing.T) {
	dir := t.TempDir()
	fx := newRecoveryFixture(dir, "orders")

	func() {
		se := openRecoveryEngine(t, fx, false)
		defer se.Close()

		for i := 1; i <= 6; i++ {
			doc := fmt.Sprintf(`{"id":%d,"status":"paid"}`, i)
			if err := se.Put("orders", "id", types.IntKey(i), doc); err != nil {
				t.Fatalf("Put %d: %v", i, err)
			}
		}
		if err := se.CreateCheckpoint(); err != nil {
			t.Fatalf("CreateCheckpoint: %v", err)
		}
	}()

	corruptPageBody(t, fx.heapPath, 1, 32, []byte("heap-page-torn-write"))

	recovered := openRecoveryEngine(t, fx, true)
	defer recovered.Close()

	for i := 1; i <= 6; i++ {
		requireDocumentVisible(t, recovered, "orders", i, fmt.Sprintf(`{"id":%d,"status":"paid"}`, i))
	}
}

func TestRecovery_RepairsTornIndexPageFromWAL(t *testing.T) {
	dir := t.TempDir()
	fx := newRecoveryFixture(dir, "accounts")

	func() {
		se := openRecoveryEngine(t, fx, false)
		defer se.Close()

		for i := 1; i <= 20; i++ {
			doc := fmt.Sprintf(`{"id":%d,"balance":%d}`, i, i*10)
			if err := se.Put("accounts", "id", types.IntKey(i), doc); err != nil {
				t.Fatalf("Put %d: %v", i, err)
			}
		}
		if err := se.CreateCheckpoint(); err != nil {
			t.Fatalf("CreateCheckpoint: %v", err)
		}
	}()

	corruptPageBody(t, fx.indexPath, 2, 48, []byte("index-page-torn-write"))

	recovered := openRecoveryEngine(t, fx, true)
	defer recovered.Close()

	for _, key := range []int{1, 7, 13, 20} {
		requireDocumentVisible(t, recovered, "accounts", key, fmt.Sprintf(`{"id":%d,"balance":%d}`, key, key*10))
	}
}

func TestRecovery_RepairsTornPageWhenCheckpointCrashesMidFlight(t *testing.T) {
	dir := t.TempDir()
	fx := newRecoveryFixture(dir, "events")

	func() {
		se := openRecoveryEngine(t, fx, false)

		for i := 1; i <= 8; i++ {
			doc := fmt.Sprintf(`{"id":%d,"kind":"checkpoint"}`, i)
			if err := se.Put("events", "id", types.IntKey(i), doc); err != nil {
				t.Fatalf("Put %d: %v", i, err)
			}
		}

		if err := se.WAL.Sync(); err != nil {
			t.Fatalf("WAL.Sync: %v", err)
		}
		if err := se.flushAllDirtyPages(); err != nil {
			t.Fatalf("flushAllDirtyPages: %v", err)
		}
		if err := se.WAL.Close(); err != nil {
			t.Fatalf("WAL.Close: %v", err)
		}
	}()

	corruptPageBody(t, fx.heapPath, 1, 96, []byte("checkpoint-mid-flight"))

	recovered := openRecoveryEngine(t, fx, true)
	defer recovered.Close()

	for i := 1; i <= 8; i++ {
		requireDocumentVisible(t, recovered, "events", i, fmt.Sprintf(`{"id":%d,"kind":"checkpoint"}`, i))
	}
}
