package v2

import (
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestCursor_FullScanReturnsAllKeysInOrder(t *testing.T) {
	tr := newTree(t, nil)
	for _, key := range []int64{30, 10, 50, 20, 40} {
		if err := tr.Insert(k(key), key*10); err != nil {
			t.Fatal(err)
		}
	}

	cur, err := tr.NewCursor(nil, nil)
	if err != nil {
		t.Fatalf("NewCursor: %v", err)
	}
	defer cur.Close()

	var got [][2]int64
	for cur.Next() {
		kk, vv := kvDec(cur.Key(), cur.Value())
		got = append(got, [2]int64{kk, vv})
	}
	if err := cur.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}

	want := [][2]int64{{10, 100}, {20, 200}, {30, 300}, {40, 400}, {50, 500}}
	if len(got) != len(want) {
		t.Fatalf("expected %d pairs, got %d", len(want), len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pos %d: expected %v, got %v", i, want[i], got[i])
		}
	}
}

func TestCursor_RangeRespectsLowerUpperInclusive(t *testing.T) {
	tr := newTree(t, nil)
	for i := int64(1); i <= 20; i++ {
		if err := tr.Insert(k(i), i*10); err != nil {
			t.Fatal(err)
		}
	}

	cur, err := tr.NewCursor(k(5), k(10))
	if err != nil {
		t.Fatalf("NewCursor: %v", err)
	}
	defer cur.Close()

	var got []int64
	for cur.Next() {
		got = append(got, int64(cur.Key().(types.IntKey)))
	}
	if err := cur.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}

	want := []int64{5, 6, 7, 8, 9, 10}
	if len(got) != len(want) {
		t.Fatalf("range: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pos %d: got %d want %d", i, got[i], want[i])
		}
	}
}

func TestCursor_EmptyTreeNextReturnsFalseImmediately(t *testing.T) {
	tr := newTree(t, nil)
	cur, err := tr.NewCursor(nil, nil)
	if err != nil {
		t.Fatalf("NewCursor: %v", err)
	}
	defer cur.Close()

	if cur.Next() {
		t.Fatal("Next() must return false on empty tree")
	}
	if err := cur.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
}

func TestCursor_CloseIsIdempotent(t *testing.T) {
	tr := newTree(t, nil)
	if err := tr.Insert(k(1), 10); err != nil {
		t.Fatal(err)
	}
	cur, err := tr.NewCursor(nil, nil)
	if err != nil {
		t.Fatalf("NewCursor: %v", err)
	}
	if err := cur.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	if err := cur.Close(); err != nil {
		t.Fatalf("Close 2: %v", err)
	}
}

func TestCursor_EarlyCloseReleasesLeafSoWritesCanProceed(t *testing.T) {
	tr := newTree(t, nil)
	for i := int64(1); i <= 5; i++ {
		if err := tr.Insert(k(i), i); err != nil {
			t.Fatal(err)
		}
	}

	cur, err := tr.NewCursor(nil, nil)
	if err != nil {
		t.Fatalf("NewCursor: %v", err)
	}
	if !cur.Next() {
		t.Fatalf("Next: expected true, err=%v", cur.Err())
	}
	// Consumer cancels mid-scan and a writer immediately needs the leaf.
	if err := cur.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := tr.Insert(k(100), 1000); err != nil {
		t.Fatalf("write after cursor close: %v", err)
	}
}
