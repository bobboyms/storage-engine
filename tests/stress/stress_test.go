//go:build stress

package stress_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/query"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

type dbPaths struct {
	walPath   string
	heapPath  string
	btreePath string
}

func pathsFor(dir string) dbPaths {
	return dbPaths{
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

func stressDuration() time.Duration {
	raw := os.Getenv("STORAGE_ENGINE_STRESS_DURATION")
	if raw == "" {
		return 5 * time.Second
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 5 * time.Second
	}
	return d
}

func TestStressConcurrentWriteReadDeleteScanCheckpointVacuum(t *testing.T) {
	p := pathsFor(t.TempDir())
	se := openEngine(t, p)

	var nextKey atomic.Int64
	nextKey.Store(10_000)

	var oracleMu sync.Mutex
	inserted := make(map[int]string)
	deleted := make(map[int]string)

	for i := 1; i <= 500; i++ {
		doc := fmt.Sprintf(`{"id":%d,"seed":true}`, i)
		if err := se.Put("t", "id", types.IntKey(int64(i)), doc); err != nil {
			t.Fatalf("seed put %d: %v", i, err)
		}
	}

	deadline := time.Now().Add(stressDuration())
	stop := make(chan struct{})
	errs := make(chan error, 1000)
	var wg sync.WaitGroup

	for worker := 0; worker < 6; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				key := int(nextKey.Add(1))
				doc := fmt.Sprintf(`{"id":%d,"worker":%d}`, key, worker)
				if err := se.Put("t", "id", types.IntKey(int64(key)), doc); err != nil {
					errs <- fmt.Errorf("put %d: %w", key, err)
					continue
				}
				oracleMu.Lock()
				inserted[key] = doc
				oracleMu.Unlock()
			}
		}(worker)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; time.Now().Before(deadline); i++ {
			key := 1 + rand.Intn(500)
			ok, err := se.Del("t", "id", types.IntKey(int64(key)))
			if err != nil {
				errs <- fmt.Errorf("delete %d: %w", key, err)
				continue
			}
			if ok {
				oracleMu.Lock()
				deleted[key] = fmt.Sprintf("%d", key)
				oracleMu.Unlock()
			}
		}
	}()

	for reader := 0; reader < 4; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				key := int64(rand.Intn(int(nextKey.Load()) + 1))
				if _, _, err := se.Get("t", "id", types.IntKey(key)); err != nil {
					errs <- fmt.Errorf("get %d: %w", key, err)
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(deadline) {
			start := rand.Intn(500)
			end := start + rand.Intn(100)
			if _, err := se.Scan("t", "id", query.Between(types.IntKey(int64(start)), types.IntKey(int64(end)))); err != nil {
				errs <- fmt.Errorf("scan %d-%d: %w", start, end, err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if err := se.FuzzyCheckpoint(); err != nil {
					errs <- fmt.Errorf("fuzzy checkpoint: %w", err)
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(125 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if err := se.Vacuum("t"); err != nil {
					errs <- fmt.Errorf("vacuum: %w", err)
				}
			}
		}
	}()

	time.Sleep(stressDuration())
	close(stop)
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		_ = se.Close()
		return
	}
	if err := se.Close(); err != nil {
		t.Fatalf("close stressed engine: %v", err)
	}

	se = openEngine(t, p)
	defer se.Close()

	oracleMu.Lock()
	defer oracleMu.Unlock()
	for key, doc := range inserted {
		got, found, err := se.Get("t", "id", types.IntKey(int64(key)))
		if err != nil {
			t.Fatalf("post-recovery get inserted %d: %v", key, err)
		}
		if !found {
			t.Fatalf("post-recovery inserted key %d missing", key)
		}
		if got != doc {
			t.Fatalf("post-recovery inserted key %d corrupted: got %q want %q", key, got, doc)
		}
	}
	for key := range deleted {
		_, found, err := se.Get("t", "id", types.IntKey(int64(key)))
		if err != nil {
			t.Fatalf("post-recovery get deleted %d: %v", key, err)
		}
		if found {
			t.Fatalf("post-recovery deleted seed key %d is still visible", key)
		}
	}
}

func TestStressReopenLoop(t *testing.T) {
	p := pathsFor(t.TempDir())
	loops := 250
	if raw := os.Getenv("STORAGE_ENGINE_REOPEN_LOOPS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err == nil && n > 0 {
			loops = n
		}
	}

	want := make(map[int]string)
	for i := 1; i <= loops; i++ {
		se := openEngine(t, p)
		doc := fmt.Sprintf(`{"id":%d,"loop":%d}`, i, i)
		if err := se.Put("t", "id", types.IntKey(int64(i)), doc); err != nil {
			_ = se.Close()
			t.Fatalf("loop %d put: %v", i, err)
		}
		if i%25 == 0 {
			if err := se.FuzzyCheckpoint(); err != nil {
				_ = se.Close()
				t.Fatalf("loop %d checkpoint: %v", i, err)
			}
		}
		if err := se.Close(); err != nil {
			t.Fatalf("loop %d close: %v", i, err)
		}
		want[i] = doc
	}

	se := openEngine(t, p)
	defer se.Close()
	for key, doc := range want {
		got, found, err := se.Get("t", "id", types.IntKey(int64(key)))
		if err != nil {
			t.Fatalf("final get %d: %v", key, err)
		}
		if !found {
			t.Fatalf("final key %d missing", key)
		}
		if got != doc {
			t.Fatalf("final key %d corrupted: got %q want %q", key, got, doc)
		}
	}
}
