//go:build chaos

package chaos_test

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

type dbPaths struct {
	dir       string
	walPath   string
	heapPath  string
	btreePath string
	oracle    string
}

func pathsFor(dir string) dbPaths {
	return dbPaths{
		dir:       dir,
		walPath:   filepath.Join(dir, "wal.log"),
		heapPath:  filepath.Join(dir, "table.heap.v2"),
		btreePath: filepath.Join(dir, "id.btree.v2"),
		oracle:    filepath.Join(dir, "oracle.log"),
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

func appendOracle(t testing.TB, path string, key int, doc string) {
	t.Helper()

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("open oracle: %v", err)
	}
	if _, err := fmt.Fprintf(f, "%d\t%s\n", key, doc); err != nil {
		_ = f.Close()
		t.Fatalf("append oracle: %v", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		t.Fatalf("sync oracle: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close oracle: %v", err)
	}
}

func readOracle(t testing.TB, path string) map[int]string {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open oracle for read: %v", err)
	}
	defer f.Close()

	want := make(map[int]string)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			t.Fatalf("bad oracle line %q", line)
		}
		key, err := strconv.Atoi(parts[0])
		if err != nil {
			t.Fatalf("bad oracle key %q: %v", parts[0], err)
		}
		want[key] = parts[1]
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan oracle: %v", err)
	}
	return want
}

func waitForOracleRows(path string, minRows int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		f, err := os.Open(path)
		if err == nil {
			rows := 0
			sc := bufio.NewScanner(f)
			for sc.Scan() {
				rows++
			}
			_ = f.Close()
			if rows >= minRows {
				return nil
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("oracle did not reach %d rows within %s", minRows, timeout)
}

func TestChaosKill9CommittedWritesRecover(t *testing.T) {
	dir := t.TempDir()
	p := pathsFor(dir)

	cmd := exec.Command(os.Args[0], "-test.run", "^TestChaosChildProcess$", "-test.v")
	cmd.Env = append(os.Environ(),
		"STORAGE_ENGINE_CHAOS_CHILD=1",
		"STORAGE_ENGINE_CHAOS_DIR="+dir,
		"STORAGE_ENGINE_CHAOS_OPS=10000",
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}

	if err := waitForOracleRows(p.oracle, 50, 10*time.Second); err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.CombinedOutput()
		t.Fatal(err)
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill child: %v", err)
	}
	_ = cmd.Wait()

	want := readOracle(t, p.oracle)
	if len(want) == 0 {
		t.Fatal("oracle is empty; crash test did not commit any operation")
	}

	se := openEngine(t, p)
	defer se.Close()

	for key, doc := range want {
		got, found, err := se.Get("t", "id", types.IntKey(int64(key)))
		if err != nil {
			t.Fatalf("get key %d after crash recovery: %v", key, err)
		}
		if !found {
			t.Fatalf("committed key %d missing after kill -9 recovery", key)
		}
		if got != doc {
			t.Fatalf("committed key %d corrupted after recovery: got %q want %q", key, got, doc)
		}
	}
}

func TestChaosChildProcess(t *testing.T) {
	if os.Getenv("STORAGE_ENGINE_CHAOS_CHILD") != "1" {
		t.Skip("helper process only")
	}

	dir := os.Getenv("STORAGE_ENGINE_CHAOS_DIR")
	if dir == "" {
		t.Fatal("STORAGE_ENGINE_CHAOS_DIR is required")
	}
	ops := 10000
	if raw := os.Getenv("STORAGE_ENGINE_CHAOS_OPS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("bad STORAGE_ENGINE_CHAOS_OPS: %v", err)
		}
		ops = n
	}

	p := pathsFor(dir)
	se := openEngine(t, p)
	defer se.Close()

	for i := 1; i <= ops; i++ {
		doc := fmt.Sprintf(`{"id":%d,"value":"crash-%d"}`, i, i)
		if err := se.Put("t", "id", types.IntKey(int64(i)), doc); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		appendOracle(t, p.oracle, i, doc)
		time.Sleep(2 * time.Millisecond)
	}
}

func TestChaosRepeatedReopenRecovery(t *testing.T) {
	p := pathsFor(t.TempDir())
	want := make(map[int]string)

	for cycle := 1; cycle <= 100; cycle++ {
		se := openEngine(t, p)
		for i := 0; i < 5; i++ {
			key := cycle*1000 + i
			doc := fmt.Sprintf(`{"id":%d,"cycle":%d}`, key, cycle)
			if err := se.Put("t", "id", types.IntKey(int64(key)), doc); err != nil {
				_ = se.Close()
				t.Fatalf("cycle %d put %d: %v", cycle, key, err)
			}
			want[key] = doc
		}
		if cycle%10 == 0 {
			if err := se.FuzzyCheckpoint(); err != nil {
				_ = se.Close()
				t.Fatalf("cycle %d checkpoint: %v", cycle, err)
			}
		}
		if err := se.Close(); err != nil {
			t.Fatalf("cycle %d close: %v", cycle, err)
		}
	}

	se := openEngine(t, p)
	defer se.Close()
	for key, doc := range want {
		got, found, err := se.Get("t", "id", types.IntKey(int64(key)))
		if err != nil {
			t.Fatalf("final get %d: %v", key, err)
		}
		if !found {
			t.Fatalf("key %d missing after repeated reopen", key)
		}
		if got != doc {
			t.Fatalf("key %d corrupted after repeated reopen: got %q want %q", key, got, doc)
		}
	}
}
