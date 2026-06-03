package v2

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// TestRebuildLeafVar_ReturnsErrorWhenEntriesOverflow verifies that
// rebuilding a leaf with more data than the page can hold returns an
// error instead of panicking. The rebalance/merge paths guard against
// this with canFit checks, but the primitive itself must degrade
// gracefully rather than crash the process.
func TestRebuildLeafVar_ReturnsErrorWhenEntriesOverflow(t *testing.T) {
	_, vp := newVarLeaf(t)

	bigKey := bytes.Repeat([]byte("k"), 1024)
	entries := make([]varLeafEntry, 0, 64)
	for i := range 64 {
		key := append(fmt.Appendf(nil, "%04d", i), bigKey...)
		entries = append(entries, varLeafEntry{key: key, value: int64(i)})
	}

	if err := rebuildLeafVar(vp, entries, pagestore.InvalidPageID); err == nil {
		t.Fatal("expected error rebuilding leaf with overflowing entries, got nil")
	}
}

// TestRebuildInternalVar_ReturnsErrorWhenEntriesOverflow is the internal
// node counterpart.
func TestRebuildInternalVar_ReturnsErrorWhenEntriesOverflow(t *testing.T) {
	_, vp := newVarInternal(t, pagestore.PageID(1))

	bigKey := bytes.Repeat([]byte("k"), 1024)
	entries := make([]varInternalEntry, 0, 64)
	for i := range 64 {
		key := append(fmt.Appendf(nil, "%04d", i), bigKey...)
		entries = append(entries, varInternalEntry{key: key, child: pagestore.PageID(i + 2)})
	}

	if err := rebuildInternalVar(vp, pagestore.PageID(1), entries); err == nil {
		t.Fatal("expected error rebuilding internal node with overflowing entries, got nil")
	}
}
