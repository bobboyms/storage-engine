package v2

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func newVarcharTree(t *testing.T) *BTreeV2 {
	t.Helper()
	path := filepath.Join(t.TempDir(), "varchar.btree.v2")
	tr, err := NewBTreeV2Varchar(path, 16, nil, VarcharKeyCodec{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.Close() })
	return tr
}

// s converte string pra types.VarcharKey (atalho pra testes).
func s(v string) types.Comparable { return types.VarcharKey(v) }

func TestBTreeV2_Varchar_InsertGet_SingleLeaf(t *testing.T) {
	tr := newVarcharTree(t)

	// Insere fora de ordem lexicográfica
	pairs := map[string]int64{
		"charlie": 3,
		"alice":   1,
		"bob":     2,
		"dave":    4,
	}
	for k, v := range pairs {
		if err := tr.Insert(s(k), v); err != nil {
			t.Fatalf("Insert %q: %v", k, err)
		}
	}

	// Lookup
	for k, want := range pairs {
		v, found, err := tr.Get(s(k))
		if err != nil {
			t.Fatalf("Get %q: %v", k, err)
		}
		if !found {
			t.Fatalf("key %q sumiu", k)
		}
		if v != want {
			t.Fatalf("key %q: esperado %d, recebi %d", k, want, v)
		}
	}

	// Key inexistente
	if _, found, _ := tr.Get(s("zulu")); found {
		t.Fatal("zulu não deveria existir")
	}
}

func TestBTreeV2_Varchar_Update(t *testing.T) {
	tr := newVarcharTree(t)
	tr.Insert(s("foo"), 100)
	tr.Insert(s("foo"), 200)

	v, _, _ := tr.Get(s("foo"))
	if v != 200 {
		t.Fatalf("update falhou: %d", v)
	}
}

func TestBTreeV2_Varchar_Upsert(t *testing.T) {
	tr := newVarcharTree(t)

	// Nova chave: oldValue=0, exists=false
	called := false
	tr.Upsert(s("new"), func(old int64, exists bool) (int64, error) {
		called = true
		if exists {
			t.Fatal("exists deveria ser false pra key nova")
		}
		return 42, nil
	})
	if !called {
		t.Fatal("fn não chamada")
	}
	v, _, _ := tr.Get(s("new"))
	if v != 42 {
		t.Fatalf("Upsert não persistiu")
	}

	// Chave existente: recebe oldValue=42
	tr.Upsert(s("new"), func(old int64, exists bool) (int64, error) {
		if !exists || old != 42 {
			t.Fatalf("Upsert: esperava exists=true old=42, recebi exists=%v old=%d", exists, old)
		}
		return old + 1, nil
	})
	v, _, _ = tr.Get(s("new"))
	if v != 43 {
		t.Fatalf("Upsert update falhou: %d", v)
	}
}

func TestBTreeV2_Varchar_ScanAll(t *testing.T) {
	tr := newVarcharTree(t)

	input := []string{"zebra", "apple", "mango", "banana", "cherry"}
	for i, k := range input {
		tr.Insert(s(k), int64(i))
	}

	// ScanAll deve emitir em ordem lexicográfica
	var seen []string
	err := tr.ScanAll(func(key types.Comparable, v int64) error {
		seen = append(seen, string(key.(types.VarcharKey)))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"apple", "banana", "cherry", "mango", "zebra"}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("pos %d: esperado %q, recebi %q", i, want[i], seen[i])
		}
	}
}

func TestBTreeV2_Varchar_Scan_Range(t *testing.T) {
	tr := newVarcharTree(t)

	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		tr.Insert(s(k), 1)
	}

	var seen []string
	err := tr.Scan(s("b"), s("e"), func(key types.Comparable, v int64) error {
		seen = append(seen, string(key.(types.VarcharKey)))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"b", "c", "d", "e"}
	if len(seen) != len(want) {
		t.Fatalf("esperado %v, recebi %v", want, seen)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("pos %d: esperado %q, recebi %q", i, want[i], seen[i])
		}
	}
}

func TestBTreeV2_Varchar_ReopenPreservesKeys(t *testing.T) {
	cipher := newCipher(t) // TDE ativo (fecha buraco do .chk pra varchar também)
	path := filepath.Join(t.TempDir(), "varchar.btree.v2")

	tr, err := NewBTreeV2Varchar(path, 16, cipher, VarcharKeyCodec{})
	if err != nil {
		t.Fatal(err)
	}

	inserted := map[string]int64{
		"alpha":   1,
		"beta":    2,
		"gamma":   3,
		"omega":   99,
		"unicode": 0xCAFE,
	}
	for k, v := range inserted {
		if err := tr.Insert(s(k), v); err != nil {
			t.Fatal(err)
		}
	}
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}

	// Reabre com mesma chave
	tr2, err := NewBTreeV2Varchar(path, 16, cipher, VarcharKeyCodec{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer tr2.Close()

	for k, want := range inserted {
		v, found, _ := tr2.Get(s(k))
		if !found || v != want {
			t.Fatalf("key %q: found=%v v=%d (esperado %d)", k, found, v, want)
		}
	}
}

func TestBTreeV2_Varchar_InsertBeyondLeaf_Splits(t *testing.T) {
	// Força múltiplos splits com chaves variáveis. Keys de 20 bytes:
	// ~8KB body / (12 slot + 20 key) = ~255 slots por leaf.
	// 1000 chaves → pelo menos 3-4 leaves.
	tr := newVarcharTree(t)

	const N = 1000
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("key-%010d-padding", i) // 21 bytes
		if err := tr.Insert(s(key), int64(i)); err != nil {
			t.Fatalf("Insert %q: %v", key, err)
		}
	}

	// Lookup de todas
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("key-%010d-padding", i)
		v, found, err := tr.Get(s(key))
		if err != nil || !found || v != int64(i) {
			t.Fatalf("key %q: found=%v v=%d err=%v", key, found, v, err)
		}
	}

	// ScanAll em ordem
	prev := ""
	count := 0
	tr.ScanAll(func(key types.Comparable, v int64) error {
		k := string(key.(types.VarcharKey))
		if prev != "" && k <= prev {
			t.Fatalf("scan fora de ordem: %q <= %q", k, prev)
		}
		prev = k
		count++
		return nil
	})
	if count != N {
		t.Fatalf("scan esperado %d, recebi %d", N, count)
	}
}

func TestBTreeV2_Varchar_Remove_MultiLeafAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "varchar-delete.btree.v2")
	tr, err := NewBTreeV2Varchar(path, 16, nil, VarcharKeyCodec{})
	if err != nil {
		t.Fatal(err)
	}

	const total = 600
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%04d-payload", i)
		if err := tr.Insert(s(key), int64(i)); err != nil {
			t.Fatalf("Insert %q: %v", key, err)
		}
	}

	deleted := map[string]struct{}{
		"key-0000-payload": {},
		"key-0001-payload": {},
		"key-0200-payload": {},
		"key-0201-payload": {},
		"key-0599-payload": {},
	}
	for key := range deleted {
		removed, err := tr.Delete(s(key))
		if err != nil {
			t.Fatalf("Delete %q: %v", key, err)
		}
		if !removed {
			t.Fatalf("Delete %q deveria retornar true", key)
		}
	}

	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}

	tr2, err := NewBTreeV2Varchar(path, 16, nil, VarcharKeyCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer tr2.Close()

	count := 0
	err = tr2.ScanAll(func(key types.Comparable, value int64) error {
		count++
		if _, gone := deleted[string(key.(types.VarcharKey))]; gone {
			t.Fatalf("scan retornou chave removida %q", key)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ScanAll pós-reopen: %v", err)
	}
	if count != total-len(deleted) {
		t.Fatalf("scan count esperado %d, recebi %d", total-len(deleted), count)
	}

	for key := range deleted {
		if _, found, err := tr2.Get(s(key)); err != nil {
			t.Fatalf("Get %q: %v", key, err)
		} else if found {
			t.Fatalf("key %q deveria ter sido removida", key)
		}
	}

	for _, key := range []string{"key-0002-payload", "key-0202-payload", "key-0598-payload"} {
		if _, found, err := tr2.Get(s(key)); err != nil {
			t.Fatalf("Get %q: %v", key, err)
		} else if !found {
			t.Fatalf("key %q sumiu após delete/reopen", key)
		}
	}
}

func TestBTreeV2_Varchar_Remove_CollapsesRootToLeaf(t *testing.T) {
	path := filepath.Join(t.TempDir(), "varchar-delete-collapse.btree.v2")
	tr, err := NewBTreeV2Varchar(path, 16, nil, VarcharKeyCodec{})
	if err != nil {
		t.Fatal(err)
	}

	const total = 1200
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("payload-key-%04d-extra-data", i)
		if err := tr.Insert(s(key), int64(i*3)); err != nil {
			t.Fatalf("Insert %q: %v", key, err)
		}
	}

	for i := 0; i < total-4; i++ {
		key := fmt.Sprintf("payload-key-%04d-extra-data", i)
		removed, err := tr.Delete(s(key))
		if err != nil {
			t.Fatalf("Delete %q: %v", key, err)
		}
		if !removed {
			t.Fatalf("Delete %q deveria retornar true", key)
		}
	}

	rootH, err := tr.bp.Fetch(tr.rootPage())
	if err != nil {
		t.Fatal(err)
	}
	rootVP, err := OpenVariableNodePage(rootH.Page(), tr.maxBodySize, tr.varCodec.Compare)
	if err != nil {
		rootH.Release()
		t.Fatal(err)
	}
	if !rootVP.IsLeaf() {
		rootH.Release()
		t.Fatal("root deveria colapsar para leaf após deletes em varchar")
	}
	rootH.Release()

	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}

	tr2, err := NewBTreeV2Varchar(path, 16, nil, VarcharKeyCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer tr2.Close()

	for i := 0; i < total-4; i++ {
		key := fmt.Sprintf("payload-key-%04d-extra-data", i)
		if _, found, err := tr2.Get(s(key)); err != nil {
			t.Fatalf("Get %q: %v", key, err)
		} else if found {
			t.Fatalf("key %q deveria ter sido removida", key)
		}
	}
	for i := total - 4; i < total; i++ {
		key := fmt.Sprintf("payload-key-%04d-extra-data", i)
		v, found, err := tr2.Get(s(key))
		if err != nil {
			t.Fatalf("Get %q: %v", key, err)
		}
		if !found || v != int64(i*3) {
			t.Fatalf("key %q: found=%v v=%d", key, found, v)
		}
	}
}
