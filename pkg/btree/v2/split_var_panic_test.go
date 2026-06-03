package v2

import (
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// TestSplitLeafIntoVar_ReturnsErrorOnBadPrecondition verifies the leaf
// split surfaces an error instead of panicking when its target page is
// not an empty leaf.
func TestSplitLeafIntoVar_ReturnsErrorOnBadPrecondition(t *testing.T) {
	_, vp := newVarLeaf(t)
	if err := vp.LeafInsertVar([]byte("a"), 1); err != nil {
		t.Fatalf("seed a: %v", err)
	}
	if err := vp.LeafInsertVar([]byte("b"), 2); err != nil {
		t.Fatalf("seed b: %v", err)
	}

	_, other := newVarLeaf(t)
	if err := other.LeafInsertVar([]byte("x"), 9); err != nil {
		t.Fatalf("seed other: %v", err)
	}

	if _, err := vp.splitLeafIntoVar(other); err == nil {
		t.Fatal("expected error splitting into a non-empty leaf, got nil")
	}
}

// TestSplitInternalIntoVar_ReturnsErrorOnBadPrecondition is the internal
// node counterpart.
func TestSplitInternalIntoVar_ReturnsErrorOnBadPrecondition(t *testing.T) {
	_, vp := newVarInternal(t, pagestore.PageID(1))
	if err := vp.InsertSeparatorVar([]byte("m"), pagestore.PageID(2)); err != nil {
		t.Fatalf("seed sep: %v", err)
	}

	_, other := newVarInternal(t, pagestore.PageID(3))
	if err := other.InsertSeparatorVar([]byte("z"), pagestore.PageID(4)); err != nil {
		t.Fatalf("seed other sep: %v", err)
	}

	if _, err := vp.splitInternalIntoVar(other); err == nil {
		t.Fatal("expected error splitting into a non-empty internal node, got nil")
	}
}
