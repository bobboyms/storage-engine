package storage

import (
	"errors"
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/types"
)

var ErrEngineDegraded = errors.New("storage: engine requires recovery after failed post-commit apply")

type postCommitApplyStage string

const (
	postCommitStageBeforeOp          postCommitApplyStage = "before_op"
	postCommitStageAfterHeapMutation postCommitApplyStage = "after_heap_mutation"
	postCommitStageAfterIndexInstall postCommitApplyStage = "after_index_install"
)

type postCommitApplyInfo struct {
	TxID      uint64
	Step      int
	Total     int
	Stage     postCommitApplyStage
	OpType    uint8
	TableName string
	IndexName string
	Key       types.Comparable
}

type storageEngineTestHooks struct {
	onPostCommitApplyStage func(info postCommitApplyInfo) error
}

func (se *StorageEngine) runtimeReadyError() error {
	if se == nil {
		return nil
	}

	se.runtimeMu.RLock()
	defer se.runtimeMu.RUnlock()

	if se.degradedErr == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrEngineDegraded, se.degradedErr)
}

func (se *StorageEngine) markDegraded(err error) {
	if se == nil || err == nil {
		return
	}

	se.runtimeMu.Lock()
	if se.degradedErr == nil {
		se.degradedErr = err
	}
	se.runtimeMu.Unlock()
}

func (se *StorageEngine) clearDegraded() {
	if se == nil {
		return
	}

	se.runtimeMu.Lock()
	se.degradedErr = nil
	se.runtimeMu.Unlock()
}

func (se *StorageEngine) runPostCommitApplyHook(info postCommitApplyInfo) error {
	if se == nil || se.testHooks.onPostCommitApplyStage == nil {
		return nil
	}
	return se.testHooks.onPostCommitApplyStage(info)
}
