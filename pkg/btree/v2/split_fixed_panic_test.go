package v2

import (
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// TestSplitLeafInto_ReturnsErrorOnBadPrecondition verifies the fixed-key
// leaf split returns an error instead of panicking when its target page
// is not an empty leaf.
func TestSplitLeafInto_ReturnsErrorOnBadPrecondition(t *testing.T) {
	_, np := newLeafPage(t)
	if err := np.LeafInsert(1, 10); err != nil {
		t.Fatalf("seed 1: %v", err)
	}
	if err := np.LeafInsert(2, 20); err != nil {
		t.Fatalf("seed 2: %v", err)
	}

	_, other := newLeafPage(t)
	if err := other.LeafInsert(9, 90); err != nil {
		t.Fatalf("seed other: %v", err)
	}

	if _, err := np.splitLeafInto(other); err == nil {
		t.Fatal("expected error splitting into a non-empty leaf, got nil")
	}
}

// TestSplitInternalInto_ReturnsErrorOnBadPrecondition is the internal
// node counterpart.
func TestSplitInternalInto_ReturnsErrorOnBadPrecondition(t *testing.T) {
	var p pagestore.Page
	np := InitInternalPage(&p, pagestore.BodySize, pagestore.PageID(1), nil)
	if err := np.InsertSeparator(5, pagestore.PageID(2)); err != nil {
		t.Fatalf("seed sep: %v", err)
	}

	var op pagestore.Page
	other := InitInternalPage(&op, pagestore.BodySize, pagestore.PageID(3), nil)
	if err := other.InsertSeparator(9, pagestore.PageID(4)); err != nil {
		t.Fatalf("seed other sep: %v", err)
	}

	if _, err := np.splitInternalInto(other); err == nil {
		t.Fatal("expected error splitting into a non-empty internal node, got nil")
	}
}
