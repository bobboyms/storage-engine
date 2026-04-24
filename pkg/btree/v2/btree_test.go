package v2

import (
	"crypto/rand"
	"io"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/crypto"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// k é atalho pra envolver int64 em types.IntKey.
func k(v int64) types.Comparable { return types.IntKey(v) }

func mustKey(t testing.TB) []byte {
	t.Helper()
	key := make([]byte, crypto.KeySize)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatal(err)
	}
	return key
}

func newCipher(t testing.TB) crypto.Cipher {
	t.Helper()
	c, err := crypto.NewAESGCM(mustKey(t))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func newTree(t testing.TB, cipher crypto.Cipher) *BTreeV2 {
	t.Helper()
	path := filepath.Join(t.TempDir(), "btree.v2")
	tr, err := NewBTreeV2(path, 16, cipher)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.Close() })
	return tr
}

func TestBTreeV2_NewAndClose(t *testing.T) {
	tr := newTree(t, nil)
	if tr == nil {
		t.Fatal("árvore nil")
	}
}

func TestBTreeV2_InsertAndGet_Single(t *testing.T) {
	tr := newTree(t, nil)

	if err := tr.Insert(k(42), 1000); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	v, found, err := tr.Get(k(42))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("chave 42 deveria existir")
	}
	if v != 1000 {
		t.Fatalf("esperava valor 1000, recebi %d", v)
	}
}

func TestBTreeV2_InsertAndGet_Multiple(t *testing.T) {
	tr := newTree(t, nil)

	keys := []int64{30, 10, 50, 20, 40}
	values := []int64{3000, 1000, 5000, 2000, 4000}

	for i, key := range keys {
		if err := tr.Insert(k(key), values[i]); err != nil {
			t.Fatalf("Insert(%d): %v", key, err)
		}
	}

	expected := map[int64]int64{
		10: 1000, 20: 2000, 30: 3000, 40: 4000, 50: 5000,
	}
	for key, want := range expected {
		got, found, _ := tr.Get(k(key))
		if !found {
			t.Fatalf("chave %d sumiu", key)
		}
		if got != want {
			t.Fatalf("chave %d: esperado valor %d, recebi %d", key, want, got)
		}
	}

	if _, found, _ := tr.Get(k(999)); found {
		t.Fatal("chave inexistente não deveria ser achada")
	}
}

func TestBTreeV2_UpdateExistingKey(t *testing.T) {
	tr := newTree(t, nil)

	tr.Insert(k(1), 100)
	tr.Insert(k(1), 200)

	v, _, _ := tr.Get(k(1))
	if v != 200 {
		t.Fatalf("esperava valor atualizado 200, recebi %d", v)
	}
}

func TestBTreeV2_ReopenPreservesKeys(t *testing.T) {
	cipher := newCipher(t)
	path := filepath.Join(t.TempDir(), "btree.v2")

	tr, err := NewBTreeV2(path, 16, cipher)
	if err != nil {
		t.Fatal(err)
	}

	inserted := map[int64]int64{
		100: 1, 200: 2, 300: 3, 50: 50, 25: 25,
	}
	for key, v := range inserted {
		if err := tr.Insert(k(key), v); err != nil {
			t.Fatal(err)
		}
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	tr2, err := NewBTreeV2(path, 16, cipher)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer tr2.Close()

	for key, want := range inserted {
		v, found, _ := tr2.Get(k(key))
		if !found {
			t.Fatalf("chave %d sumiu após reopen", key)
		}
		if v != want {
			t.Fatalf("chave %d: esperado %d, recebi %d (após reopen)", key, want, v)
		}
	}
}

func TestBTreeV2_InsertForcesInternalSplit_3LevelTree(t *testing.T) {
	if testing.Short() {
		t.Skip("pesado; roda em modo completo")
	}

	path := filepath.Join(t.TempDir(), "deep.btree.v2")
	tr, err := NewBTreeV2(path, 64, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	const N = 130_000
	for i := int64(0); i < N; i++ {
		if err := tr.Insert(k(i), i*7); err != nil {
			t.Fatalf("Insert(%d) falhou: %v", i, err)
		}
	}

	checks := []int64{0, 1, 509, 100_000, 128_780, 129_286, N - 1}
	for _, key := range checks {
		if key >= N {
			continue
		}
		v, found, err := tr.Get(k(key))
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if !found {
			t.Fatalf("chave %d sumiu — split recursivo quebrado", key)
		}
		if v != key*7 {
			t.Fatalf("chave %d corrompida: esperado %d, recebi %d", key, key*7, v)
		}
	}

	for i := int64(0); i < N; i += 1000 {
		v, found, _ := tr.Get(k(i))
		if !found || v != i*7 {
			t.Fatalf("sweep i=%d: found=%v v=%d", i, found, v)
		}
	}
}

func TestBTreeV2_InsertBeyondSingleLeaf_2LevelTree(t *testing.T) {
	tr := newTree(t, nil)

	const N = 1000
	for i := int64(0); i < N; i++ {
		if err := tr.Insert(k(i), i*10); err != nil {
			t.Fatalf("Insert(%d) falhou: %v", i, err)
		}
	}

	for i := int64(0); i < N; i++ {
		v, found, err := tr.Get(k(i))
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if !found {
			t.Fatalf("chave %d sumiu após inserts+splits", i)
		}
		if v != i*10 {
			t.Fatalf("chave %d corrompida: esperado %d, recebi %d", i, i*10, v)
		}
	}
}

func TestBTreeV2_Upsert_NewKey(t *testing.T) {
	tr := newTree(t, nil)
	called := false
	err := tr.Upsert(k(42), func(old int64, exists bool) (int64, error) {
		called = true
		if exists {
			t.Fatal("exists deveria ser false pra key nova")
		}
		if old != 0 {
			t.Fatalf("old deveria ser 0 pra key nova, recebi %d", old)
		}
		return 100, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("fn não foi chamada")
	}
	v, found, _ := tr.Get(k(42))
	if !found || v != 100 {
		t.Fatalf("Upsert não persistiu: found=%v v=%d", found, v)
	}
}

func TestBTreeV2_Upsert_ExistingKey(t *testing.T) {
	tr := newTree(t, nil)
	tr.Insert(k(7), 700)

	err := tr.Upsert(k(7), func(old int64, exists bool) (int64, error) {
		if !exists {
			t.Fatal("exists deveria ser true")
		}
		if old != 700 {
			t.Fatalf("old esperado 700, recebi %d", old)
		}
		return old + 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	v, _, _ := tr.Get(k(7))
	if v != 701 {
		t.Fatalf("Upsert não atualizou: %d", v)
	}
}

func TestBTreeV2_Remove_MultiLeaf(t *testing.T) {
	tr := newTree(t, nil)

	const total = 1000
	for i := int64(0); i < total; i++ {
		if err := tr.Insert(k(i), i*10); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	deleted := map[int64]struct{}{
		0: {}, 1: {}, 255: {}, 256: {}, 511: {}, 777: {}, 999: {},
	}
	for key := range deleted {
		removed, err := tr.Remove(k(key))
		if err != nil {
			t.Fatalf("Remove(%d): %v", key, err)
		}
		if !removed {
			t.Fatalf("Remove(%d) deveria retornar true", key)
		}
	}

	if removed, err := tr.Remove(k(424242)); err != nil || removed {
		t.Fatalf("Remove missing: removed=%v err=%v", removed, err)
	}

	for i := int64(0); i < total; i++ {
		v, found, err := tr.Get(k(i))
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}

		_, wasDeleted := deleted[i]
		if wasDeleted {
			if found {
				t.Fatalf("key %d deveria ter sido removida", i)
			}
			continue
		}

		if !found || v != i*10 {
			t.Fatalf("key %d: found=%v v=%d", i, found, v)
		}
	}

	count := 0
	err := tr.ScanAll(func(key types.Comparable, value int64) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("ScanAll após deletes: %v", err)
	}
	if count != total-len(deleted) {
		t.Fatalf("scan count esperado %d, recebi %d", total-len(deleted), count)
	}
}

func TestBTreeV2_Remove_CollapsesRootToLeaf(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixed-delete-collapse.btree.v2")
	tr, err := NewBTreeV2(path, 16, nil)
	if err != nil {
		t.Fatal(err)
	}

	const total = 2000
	for i := int64(0); i < total; i++ {
		if err := tr.Insert(k(i), i*11); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	for i := int64(0); i < total-5; i++ {
		removed, err := tr.Remove(k(i))
		if err != nil {
			t.Fatalf("Remove(%d): %v", i, err)
		}
		if !removed {
			t.Fatalf("Remove(%d) deveria retornar true", i)
		}
	}

	rootH, err := tr.bp.Fetch(tr.rootPage())
	if err != nil {
		t.Fatal(err)
	}
	rootNP, err := OpenNodePage(rootH.Page(), tr.maxBodySize, tr.codec.Compare)
	if err != nil {
		rootH.Release()
		t.Fatal(err)
	}
	if !rootNP.IsLeaf() {
		rootH.Release()
		t.Fatal("root deveria colapsar para leaf após deletes")
	}
	rootH.Release()

	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}

	tr2, err := NewBTreeV2(path, 16, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tr2.Close()

	for i := int64(0); i < total-5; i++ {
		if _, found, err := tr2.Get(k(i)); err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		} else if found {
			t.Fatalf("key %d deveria ter sido removida", i)
		}
	}
	for i := int64(total - 5); i < total; i++ {
		v, found, err := tr2.Get(k(i))
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if !found || v != i*11 {
			t.Fatalf("key %d: found=%v v=%d", i, found, v)
		}
	}
}
