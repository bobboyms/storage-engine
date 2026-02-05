package storage

import (
	"math"
	"sync"
)

// TransactionRegistry tracks active transactions to determine the oldest visible snapshot.
// This is crucial for Garbage Collection (Vacuum): we can only delete Tombstones
// (deleted records) if NO active transaction can see them anymore.
//
// A Tombstone with DeleteLSN < MinSnapshotLSN is safe to remove, because
// any future transaction will have SnapshotLSN >= CurrentLSN > DeleteLSN,
// seeing it as deleted. Any active transaction has SnapshotLSN >= MinSnapshotLSN > DeleteLSN,
// also seeing it as deleted.
type TransactionRegistry struct {
	mu           sync.Mutex
	activeTxns   map[*Transaction]struct{}
	minActiveLSN uint64
}

func NewTransactionRegistry() *TransactionRegistry {
	return &TransactionRegistry{
		activeTxns:   make(map[*Transaction]struct{}),
		minActiveLSN: math.MaxUint64,
	}
}

// Register adds a transaction to the registry.
func (tr *TransactionRegistry) Register(tx *Transaction) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	tr.activeTxns[tx] = struct{}{}
	if tx.SnapshotLSN < tr.minActiveLSN {
		tr.minActiveLSN = tx.SnapshotLSN
	}
}

// Unregister removes a transaction from the registry.
func (tr *TransactionRegistry) Unregister(tx *Transaction) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	delete(tr.activeTxns, tx)

	// Re-calculate MinActiveLSN
	// Optimization: If the removed tx was NOT the min, we don't need to recalculate.
	// But it's safer/simpler to just recalculate if map is small.
	// If map is empty, minActiveLSN = MaxUint64 (infinity).

	if len(tr.activeTxns) == 0 {
		tr.minActiveLSN = math.MaxUint64
		return
	}

	min := uint64(math.MaxUint64)
	for t := range tr.activeTxns {
		if t.SnapshotLSN < min {
			min = t.SnapshotLSN
		}
	}
	tr.minActiveLSN = min
}

// GetMinActiveLSN returns the smallest SnapshotLSN among all active transactions.
// Returns MaxUint64 if no transactions are active.
func (tr *TransactionRegistry) GetMinActiveLSN() uint64 {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.minActiveLSN
}
