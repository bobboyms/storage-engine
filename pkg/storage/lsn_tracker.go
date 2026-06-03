package storage

import (
	"sync/atomic"
)

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

// Next increments and returns the next LSN
func (lt *LSNTracker) Next() uint64 {
	return atomic.AddUint64(&lt.current, 1)
}

// Current returns the current LSN
func (lt *LSNTracker) Current() uint64 {
	return atomic.LoadUint64(&lt.current)
}

// Set define o LSN atual (usado no recovery)
func (lt *LSNTracker) Set(val uint64) {
	atomic.StoreUint64(&lt.current, val)
}
