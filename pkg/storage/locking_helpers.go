package storage

import "context"

func (se *StorageEngine) withAutoCommitLocks(ctx context.Context, resources []string, fn func() error) error {
	if se.LockManager == nil || len(resources) == 0 {
		return fn()
	}

	txID := se.nextTxID()
	for _, resource := range resources {
		if err := se.LockManager.Acquire(ctx, txID, resource); err != nil {
			se.LockManager.ReleaseAll(txID)
			return err
		}
	}
	defer se.LockManager.ReleaseAll(txID)

	return fn()
}
