package storage

import (
	"sync/atomic"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// isPoisonedLSN reports whether lsn is the MaxUint64 sentinel that leaked
// into WAL entry headers and page headers from pre-clamp vacuum runs
// (TransactionRegistry.GetMinActiveLSN returns MaxUint64 when no transaction
// is active). It is never a real LSN — the tracker fail-stops before reaching
// it — so recovery treats it as corrupt metadata rather than a high-water
// mark. The sentinel rule itself lives in pagestore, next to the page-header
// LSN handling that shares it.
func isPoisonedLSN(lsn uint64) bool {
	return pagestore.IsPoisonedPageLSN(lsn)
}

// advanceMaxLSN folds a WAL entry header LSN into the running maximum used
// to seed the LSN counter after a scan/replay, ignoring the poisoned
// sentinel so a corrupt entry cannot become the next allocation point.
func advanceMaxLSN(maxLSN *uint64, lsn uint64) {
	if lsn > *maxLSN && !isPoisonedLSN(lsn) {
		*maxLSN = lsn
	}
}

// LSNTracker manages the Log Sequence Number in a thread-safe way
type LSNTracker struct {
	current uint64
	// We could use sync.Mutex for operations that are not purely atomic if needed,
	// but for a simple counter, atomic is sufficient and faster.
	// We keep the struct ready for more complex logic if required.
}

func NewLSNTracker(start uint64) *LSNTracker {
	return &LSNTracker{
		current: start,
	}
}

// Next increments and returns the next LSN.
//
// Wrapping to 0 is never a valid allocation: visibility is "createLSN <=
// snapshotLSN", so a wrapped counter makes every committed row invisible to
// new snapshots while writes keep succeeding. Fail-stop instead — reaching
// MaxUint64 means the counter was seeded from corrupt state (e.g. a poisoned
// WAL entry) and continuing would silently lose data.
func (lt *LSNTracker) Next() uint64 {
	next := atomic.AddUint64(&lt.current, 1)
	if next == 0 {
		panic("storage: LSN counter overflow — refusing to wrap to 0 (corrupt LSN source)")
	}
	return next
}

// Current returns the current LSN
func (lt *LSNTracker) Current() uint64 {
	return atomic.LoadUint64(&lt.current)
}

// Set define o LSN atual (usado no recovery)
func (lt *LSNTracker) Set(val uint64) {
	atomic.StoreUint64(&lt.current, val)
}
