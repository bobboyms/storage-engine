package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// setupEngineWithWAL cria um StorageEngine com tabela "users" e WAL em dir temporário.
// Retorna o engine e uma função cleanup para fechar tudo.
func setupEngineWithWAL(t *testing.T, dir, tableName string) *StorageEngine {
	t.Helper()
	heapPath := filepath.Join(dir, tableName+".heap")

	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("criar heap: %v", err)
	}

	meta := NewTableMenager()
	if err := meta.NewTable(tableName, []Index{
		{Name: "id", Primary: true, Type: TypeInt},
	}, 0, hm); err != nil {
		t.Fatalf("criar tabela: %v", err)
	}

	walPath := filepath.Join(dir, "wal.log")
	opts := wal.DefaultOptions()
	walWriter, err := wal.NewWALWriter(walPath, opts)
	if err != nil {
		t.Fatalf("criar WAL: %v", err)
	}

	se, err := NewStorageEngine(meta, walWriter)
	if err != nil {
		t.Fatalf("criar engine: %v", err)
	}
	t.Cleanup(func() { se.Close() })
	return se
}

func TestFuzzyCheckpoint_Basic(t *testing.T) {
	se := setupEngineWithWAL(t, t.TempDir(), "users")

	for i := 1; i <= 10; i++ {
		doc := fmt.Sprintf(`{"id":%d,"name":"user%d"}`, i, i)
		if err := se.Put("users", "id", types.IntKey(i), doc); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	if err := se.FuzzyCheckpoint(); err != nil {
		t.Fatalf("FuzzyCheckpoint: %v", err)
	}

	// Dados ainda acessíveis after checkpoint.
	for i := 1; i <= 10; i++ {
		_, found, err := se.Get("users", "id", types.IntKey(i))
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if !found {
			t.Fatalf("record %d not encontrado after checkpoint", i)
		}
	}
}

func TestFuzzyCheckpoint_WALContainsCheckpointEntry(t *testing.T) {
	dir := t.TempDir()
	se := setupEngineWithWAL(t, dir, "items")

	if err := se.FuzzyCheckpoint(); err != nil {
		t.Fatalf("FuzzyCheckpoint: %v", err)
	}

	walPath := filepath.Join(dir, "wal.log")
	// Verifica que o arquivo WAL exists (o engine o criou).
	if _, err := os.Stat(walPath); err != nil {
		t.Fatalf("WAL not encontrado: %v", err)
	}

	ckLSN, found, err := findLastCheckpointLSN(walPath)
	if err != nil {
		t.Fatalf("findLastCheckpointLSN: %v", err)
	}
	if !found {
		t.Fatal("record de checkpoint not encontrado no WAL")
	}
	_ = ckLSN
}

func TestFuzzyCheckpoint_NoWAL_IsNoop(t *testing.T) {
	meta := NewTableMenager()
	se, err := NewStorageEngine(meta, nil)
	if err != nil {
		t.Fatalf("criar engine sem WAL: %v", err)
	}
	defer se.Close()

	// FuzzyCheckpoint sem WAL must retornar nil (no-op).
	if err := se.FuzzyCheckpoint(); err != nil {
		t.Fatalf("FuzzyCheckpoint sem WAL should be no-op: %v", err)
	}
}

func TestFuzzyCheckpoint_RecoverySkipsBeforeCheckpointLSN(t *testing.T) {
	dir := t.TempDir()
	tableName := "employees"
	heapPath := filepath.Join(dir, tableName+".heap")
	walPath := filepath.Join(dir, "wal.log")

	// --- Fase 1: inserts + fuzzy checkpoint ---
	func() {
		hm, err := NewHeapForTable(HeapFormatV2, heapPath)
		if err != nil {
			t.Fatalf("criar heap: %v", err)
		}
		meta := NewTableMenager()
		if err := meta.NewTable(tableName, []Index{
			{Name: "id", Primary: true, Type: TypeInt},
		}, 0, hm); err != nil {
			t.Fatalf("criar tabela: %v", err)
		}

		opts := wal.DefaultOptions()
		walWriter, err := wal.NewWALWriter(walPath, opts)
		if err != nil {
			t.Fatalf("criar WAL: %v", err)
		}
		se, err := NewStorageEngine(meta, walWriter)
		if err != nil {
			t.Fatalf("criar engine: %v", err)
		}
		defer se.Close()

		for i := 1; i <= 5; i++ {
			doc := fmt.Sprintf(`{"id":%d,"name":"emp%d"}`, i, i)
			if err := se.Put(tableName, "id", types.IntKey(i), doc); err != nil {
				t.Fatalf("Put %d: %v", i, err)
			}
		}

		// Checkpoint fuzzy — grava record no WAL com beginLSN.
		if err := se.FuzzyCheckpoint(); err != nil {
			t.Fatalf("FuzzyCheckpoint: %v", err)
		}

		// Mais inserts depois do checkpoint.
		for i := 6; i <= 8; i++ {
			doc := fmt.Sprintf(`{"id":%d,"name":"emp%d"}`, i, i)
			if err := se.Put(tableName, "id", types.IntKey(i), doc); err != nil {
				t.Fatalf("Put %d: %v", i, err)
			}
		}
	}()

	// --- Fase 2: verificar que findLastCheckpointLSN encontra o record ---
	ckLSN, found, err := findLastCheckpointLSN(walPath)
	if err != nil {
		t.Fatalf("findLastCheckpointLSN: %v", err)
	}
	if !found {
		t.Fatal("expected encontrar record de checkpoint no WAL")
	}
	if ckLSN == 0 {
		t.Fatal("checkpoint LSN should not ser 0")
	}

	// --- Fase 3: recovery usa checkpoint LSN ---
	hm2, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("criar heap (recovery): %v", err)
	}
	meta2 := NewTableMenager()
	if err := meta2.NewTable(tableName, []Index{
		{Name: "id", Primary: true, Type: TypeInt},
	}, 0, hm2); err != nil {
		t.Fatalf("criar tabela (recovery): %v", err)
	}

	opts2 := wal.DefaultOptions()
	walWriter2, err := wal.NewWALWriter(walPath, opts2)
	if err != nil {
		t.Fatalf("criar WAL (recovery): %v", err)
	}
	se2, err := NewProductionStorageEngine(meta2, walWriter2)
	if err != nil {
		t.Fatalf("ProductionStorageEngine (recovery): %v", err)
	}
	defer se2.Close()

	// Todas as inserts (1-8) mustm estar visible after recovery.
	for i := 1; i <= 8; i++ {
		_, found, err := se2.Get(tableName, "id", types.IntKey(i))
		if err != nil {
			t.Fatalf("Get %d after recovery: %v", i, err)
		}
		if !found {
			t.Fatalf("record %d not encontrado after recovery com checkpoint", i)
		}
	}
}

func TestFuzzyCheckpoint_MultipleCheckpoints(t *testing.T) {
	dir := t.TempDir()
	se := setupEngineWithWAL(t, dir, "items")

	// Três rounds de inserts + checkpoints.
	for round := 1; round <= 3; round++ {
		for i := 1; i <= 3; i++ {
			key := (round-1)*3 + i
			doc := fmt.Sprintf(`{"id":%d,"val":%d}`, key, round)
			if err := se.Put("items", "id", types.IntKey(key), doc); err != nil {
				t.Fatalf("Put round=%d i=%d: %v", round, i, err)
			}
		}
		if err := se.FuzzyCheckpoint(); err != nil {
			t.Fatalf("FuzzyCheckpoint round=%d: %v", round, err)
		}
	}

	walPath := filepath.Join(dir, "wal.log")
	ckLSN, found, err := findLastCheckpointLSN(walPath)
	if err != nil {
		t.Fatalf("findLastCheckpointLSN: %v", err)
	}
	if !found {
		t.Fatal("expected record de checkpoint")
	}
	// 3 rounds * 3 inserts = 9 inserts → último beginLSN must ser >= 9.
	if ckLSN < 9 {
		t.Fatalf("último checkpoint LSN expected >= 9, got %d", ckLSN)
	}
}

func TestFuzzyCheckpoint_RotatesAndTruncatesWALSafely(t *testing.T) {
	dir := t.TempDir()
	tableName := "accounts"
	heapPath := filepath.Join(dir, tableName+".heap")
	walPath := filepath.Join(dir, "wal.log")

	open := func(t *testing.T) *StorageEngine {
		t.Helper()
		hm, err := NewHeapForTable(HeapFormatV2, heapPath)
		if err != nil {
			t.Fatalf("criar heap: %v", err)
		}
		meta := NewTableMenager()
		if err := meta.NewTable(tableName, []Index{
			{Name: "id", Primary: true, Type: TypeInt},
		}, 0, hm); err != nil {
			t.Fatalf("criar tabela: %v", err)
		}
		opts := wal.DefaultOptions()
		opts.MaxSegmentBytes = 1
		opts.RetentionSegments = 0
		walWriter, err := wal.NewWALWriter(walPath, opts)
		if err != nil {
			t.Fatalf("criar WAL: %v", err)
		}
		se, err := NewProductionStorageEngine(meta, walWriter)
		if err != nil {
			walWriter.Close()
			t.Fatalf("criar engine: %v", err)
		}
		return se
	}

	se := open(t)
	for i := 1; i <= 3; i++ {
		if err := se.Put(tableName, "id", types.IntKey(i), fmt.Sprintf(`{"id":%d}`, i)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if err := se.FuzzyCheckpoint(); err != nil {
		t.Fatalf("checkpoint 1: %v", err)
	}
	for i := 4; i <= 6; i++ {
		if err := se.Put(tableName, "id", types.IntKey(i), fmt.Sprintf(`{"id":%d}`, i)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if err := se.FuzzyCheckpoint(); err != nil {
		t.Fatalf("checkpoint 2: %v", err)
	}
	if err := se.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	paths, err := wal.SegmentPaths(walPath)
	if err != nil {
		t.Fatalf("SegmentPaths: %v", err)
	}
	if len(paths) >= 8 {
		t.Fatalf("WAL was not truncado; segmentos=%v", paths)
	}

	recovered := open(t)
	defer recovered.Close()
	for i := 1; i <= 6; i++ {
		_, found, err := recovered.Get(tableName, "id", types.IntKey(i))
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if !found {
			t.Fatalf("record %d not recuperado after truncate de WAL", i)
		}
	}
}
