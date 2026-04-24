package storage

import "sync"

type AppliedLSNTracker struct {
	mu      sync.RWMutex
	byIndex map[string]uint64
}

func NewAppliedLSNTracker() *AppliedLSNTracker {
	return &AppliedLSNTracker{
		byIndex: make(map[string]uint64),
	}
}

func appliedLSNKey(tableName, indexName string) string {
	return tableName + "." + indexName
}

func (t *AppliedLSNTracker) Get(tableName, indexName string) uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.byIndex[appliedLSNKey(tableName, indexName)]
}

func (t *AppliedLSNTracker) MarkApplied(tableName, indexName string, lsn uint64) {
	key := appliedLSNKey(tableName, indexName)

	t.mu.Lock()
	defer t.mu.Unlock()

	if current := t.byIndex[key]; lsn > current {
		t.byIndex[key] = lsn
	}
}

func (t *AppliedLSNTracker) Set(tableName, indexName string, lsn uint64) {
	key := appliedLSNKey(tableName, indexName)

	t.mu.Lock()
	defer t.mu.Unlock()
	t.byIndex[key] = lsn
}
