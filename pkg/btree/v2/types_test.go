package v2

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// Testes end-to-end pra cada KeyCodec. Provam que os 4 tipos suportados
// (Int, Float, Bool, Date) funcionam através da BTreeV2 completa —
// incluindo splits, scan ordenado, reopen.

func newTreeTyped(t *testing.T, codec KeyCodec) *BTreeV2 {
	t.Helper()
	path := filepath.Join(t.TempDir(), "typed.btree.v2")
	tr, err := NewBTreeV2Typed(path, 16, nil, codec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.Close() })
	return tr
}

func TestBTreeV2_Typed_Float(t *testing.T) {
	tr := newTreeTyped(t, FloatKeyCodec{})

	// Inclui negativos — seriam ordenados incorretamente com comparação
	// uint64 direta de bits IEEE754. Prova que o codec.Compare é usado.
	keys := []float64{-3.14, -1.5, 0.0, 1.5, 3.14, 100.5}
	for i, k := range keys {
		if err := tr.Insert(types.FloatKey(k), int64(i)); err != nil {
			t.Fatal(err)
		}
	}

	// Lookup de cada um
	for i, k := range keys {
		v, found, _ := tr.Get(types.FloatKey(k))
		if !found || v != int64(i) {
			t.Fatalf("FloatKey(%v): found=%v v=%d, esperado i=%d", k, found, v, i)
		}
	}

	// Scan ordenado
	var seen []float64
	tr.ScanAll(func(key types.Comparable, v int64) error {
		seen = append(seen, float64(key.(types.FloatKey)))
		return nil
	})
	want := []float64{-3.14, -1.5, 0.0, 1.5, 3.14, 100.5}
	if len(seen) != len(want) {
		t.Fatalf("ScanAll esperado %d, recebi %d", len(want), len(seen))
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("ScanAll pos %d: esperado %v, recebi %v (ordenação via FloatKeyCodec quebrou)",
				i, want[i], seen[i])
		}
	}
}

func TestBTreeV2_Typed_Bool(t *testing.T) {
	tr := newTreeTyped(t, BoolKeyCodec{})

	tr.Insert(types.BoolKey(true), 1)
	tr.Insert(types.BoolKey(false), 0)

	if v, ok, _ := tr.Get(types.BoolKey(true)); !ok || v != 1 {
		t.Fatalf("Get(true): ok=%v v=%d", ok, v)
	}
	if v, ok, _ := tr.Get(types.BoolKey(false)); !ok || v != 0 {
		t.Fatalf("Get(false): ok=%v v=%d", ok, v)
	}

	// Ordem: false < true
	var seen []bool
	tr.ScanAll(func(key types.Comparable, v int64) error {
		seen = append(seen, bool(key.(types.BoolKey)))
		return nil
	})
	want := []bool{false, true}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("Bool pos %d: esperado %v, recebi %v", i, want[i], seen[i])
		}
	}
}

func TestBTreeV2_Typed_Date(t *testing.T) {
	tr := newTreeTyped(t, DateKeyCodec{})

	// Datas intencionalmente fora de ordem
	dates := []time.Time{
		time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2020, 6, 15, 0, 0, 0, 0, time.UTC),
		time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 3, 10, 0, 0, 0, 0, time.UTC),
	}
	for i, d := range dates {
		if err := tr.Insert(types.DateKey(d), int64(i)); err != nil {
			t.Fatal(err)
		}
	}

	// Scan ordenado: crescente cronológico
	var seen []time.Time
	tr.ScanAll(func(key types.Comparable, v int64) error {
		seen = append(seen, time.Time(key.(types.DateKey)))
		return nil
	})
	if len(seen) != 4 {
		t.Fatalf("esperado 4, recebi %d", len(seen))
	}
	for i := 1; i < len(seen); i++ {
		if seen[i].Before(seen[i-1]) {
			t.Fatalf("ScanAll fora de ordem: %v vem antes de %v", seen[i], seen[i-1])
		}
	}

	// Lookup por data específica
	v, found, _ := tr.Get(types.DateKey(dates[1]))
	if !found || v != 1 {
		t.Fatalf("Get(dates[1]): found=%v v=%d", found, v)
	}
}

func TestBTreeV2_Typed_Int_NegativesOrderedCorrectly(t *testing.T) {
	// Testa IntKey negativo — caso que um raw uint64 compare estragaria.
	tr := newTreeTyped(t, IntKeyCodec{})

	for _, v := range []int64{-100, -10, 0, 10, 100} {
		tr.Insert(types.IntKey(v), v*10)
	}

	var seen []int64
	tr.ScanAll(func(key types.Comparable, v int64) error {
		seen = append(seen, int64(key.(types.IntKey)))
		return nil
	})
	want := []int64{-100, -10, 0, 10, 100}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("pos %d: esperado %d, recebi %d (IntKey negativo ordenado errado)", i, want[i], seen[i])
		}
	}
}
