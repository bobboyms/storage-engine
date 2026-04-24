package v2

import (
	"errors"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// kvDec extrai (int64, int64) de um par (types.Comparable, int64).
// Assumimos IntKey (é o codec default do newTree).
func kvDec(key types.Comparable, v int64) (int64, int64) {
	return int64(key.(types.IntKey)), v
}

func TestBTreeV2_ScanAll_SingleLeaf(t *testing.T) {
	tr := newTree(t, nil)

	for _, key := range []int64{30, 10, 50, 20, 40} {
		if err := tr.Insert(k(key), key*10); err != nil {
			t.Fatal(err)
		}
	}

	var got [][2]int64
	err := tr.ScanAll(func(key types.Comparable, v int64) error {
		kk, vv := kvDec(key, v)
		got = append(got, [2]int64{kk, vv})
		return nil
	})
	if err != nil {
		t.Fatalf("ScanAll: %v", err)
	}

	want := [][2]int64{{10, 100}, {20, 200}, {30, 300}, {40, 400}, {50, 500}}
	if len(got) != len(want) {
		t.Fatalf("esperado %d pares, recebi %d", len(want), len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pos %d: esperado %v, recebi %v", i, want[i], got[i])
		}
	}
}

func TestBTreeV2_ScanAll_EmptyTree(t *testing.T) {
	tr := newTree(t, nil)

	count := 0
	err := tr.ScanAll(func(key types.Comparable, v int64) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("árvore vazia deveria não iterar, iterou %d", count)
	}
}

func TestBTreeV2_ScanAll_SpansMultipleLeaves(t *testing.T) {
	tr := newTree(t, nil)

	const N = 1000
	for i := int64(0); i < N; i++ {
		if err := tr.Insert(k(i), i*3); err != nil {
			t.Fatal(err)
		}
	}

	expectedKey := int64(0)
	count := 0
	err := tr.ScanAll(func(key types.Comparable, v int64) error {
		kk, _ := kvDec(key, v)
		if kk != expectedKey {
			t.Fatalf("pos %d: esperado key %d, recebi %d", count, expectedKey, kk)
		}
		if v != kk*3 {
			t.Fatalf("key %d: esperado value %d, recebi %d", kk, kk*3, v)
		}
		expectedKey++
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != N {
		t.Fatalf("esperado %d iterações, recebi %d", N, count)
	}
}

func TestBTreeV2_Scan_Range_SingleLeaf(t *testing.T) {
	tr := newTree(t, nil)
	for i := int64(0); i < 20; i++ {
		tr.Insert(k(i), i)
	}

	var got []int64
	err := tr.Scan(k(5), k(10), func(key types.Comparable, v int64) error {
		kk, _ := kvDec(key, v)
		got = append(got, kk)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []int64{5, 6, 7, 8, 9, 10}
	if len(got) != len(want) {
		t.Fatalf("esperado %d chaves, recebi %d: %v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pos %d: esperado %d, recebi %d", i, want[i], got[i])
		}
	}
}

func TestBTreeV2_Scan_Range_SpansLeaves(t *testing.T) {
	tr := newTree(t, nil)
	for i := int64(0); i < 2000; i++ {
		tr.Insert(k(i), i)
	}

	count := 0
	err := tr.Scan(k(100), k(999), func(key types.Comparable, v int64) error {
		kk, _ := kvDec(key, v)
		if kk != int64(100+count) {
			t.Fatalf("esperado key %d, recebi %d", 100+count, kk)
		}
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 900 {
		t.Fatalf("esperado 900 chaves, recebi %d", count)
	}
}

func TestBTreeV2_Scan_EmptyRange(t *testing.T) {
	tr := newTree(t, nil)
	for i := int64(0); i < 10; i++ {
		tr.Insert(k(i), i)
	}

	count := 0
	err := tr.Scan(k(100), k(200), func(key types.Comparable, v int64) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("range fora do domínio deveria iterar 0, iterou %d", count)
	}
}

func TestBTreeV2_Scan_StartBeforeFirstKey(t *testing.T) {
	tr := newTree(t, nil)
	tr.Insert(k(50), 500)
	tr.Insert(k(100), 1000)
	tr.Insert(k(150), 1500)

	var got []int64
	tr.Scan(k(-999), k(120), func(key types.Comparable, v int64) error {
		kk, _ := kvDec(key, v)
		got = append(got, kk)
		return nil
	})
	want := []int64{50, 100}
	if len(got) != len(want) {
		t.Fatalf("esperado %v, recebi %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pos %d: esperado %d, recebi %d", i, want[i], got[i])
		}
	}
}

func TestBTreeV2_Scan_EarlyStop(t *testing.T) {
	tr := newTree(t, nil)
	for i := int64(0); i < 100; i++ {
		tr.Insert(k(i), i)
	}

	stop := errors.New("parar")
	count := 0
	err := tr.Scan(k(0), k(99), func(key types.Comparable, v int64) error {
		count++
		if count == 5 {
			return stop
		}
		return nil
	})
	if err != stop {
		t.Fatalf("esperava erro 'parar', recebi %v", err)
	}
	if count != 5 {
		t.Fatalf("esperado parar em 5, parou em %d", count)
	}
}
