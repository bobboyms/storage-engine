package storage

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bobboyms/storage-engine/pkg/types"
)

var ErrLockWaitTimeout = errors.New("storage: lock wait timeout")
var ErrDeadlockVictim = errors.New("storage: transaction aborted as deadlock victim")

type DeadlockError struct {
	VictimTxID uint64
	Cycle      []uint64
}

func (e *DeadlockError) Error() string {
	return fmt.Sprintf("storage: deadlock detected, aborting tx %d (cycle=%v)", e.VictimTxID, e.Cycle)
}

func (e *DeadlockError) Unwrap() error {
	return ErrDeadlockVictim
}

type LockManagerConfig struct {
	WaitTimeout time.Duration
}

type LockManager struct {
	mu          sync.Mutex
	waitTimeout time.Duration
	resources   map[string]*lockState
	heldByTx    map[uint64]map[string]struct{}
	waitingByTx map[uint64]*lockWaiter
	abortedTxs  map[uint64]error
}

type lockState struct {
	holder  uint64
	waiters []*lockWaiter
}

type lockWaiter struct {
	txID     uint64
	resource string
	result   chan error
	done     bool
}

func NewLockManager(cfg LockManagerConfig) *LockManager {
	waitTimeout := cfg.WaitTimeout
	if waitTimeout <= 0 {
		waitTimeout = 5 * time.Second
	}

	return &LockManager{
		waitTimeout: waitTimeout,
		resources:   make(map[string]*lockState),
		heldByTx:    make(map[uint64]map[string]struct{}),
		waitingByTx: make(map[uint64]*lockWaiter),
		abortedTxs:  make(map[uint64]error),
	}
}

func (lm *LockManager) Acquire(txID uint64, resource string) error {
	for {
		lm.mu.Lock()

		if err := lm.abortedTxs[txID]; err != nil {
			lm.mu.Unlock()
			return err
		}

		state := lm.ensureResourceLocked(resource)
		if state.holder == 0 || state.holder == txID {
			state.holder = txID
			lm.recordHeldResourceLocked(txID, resource)
			lm.mu.Unlock()
			return nil
		}

		waiter := &lockWaiter{
			txID:     txID,
			resource: resource,
			result:   make(chan error, 1),
		}
		state.waiters = append(state.waiters, waiter)
		lm.waitingByTx[txID] = waiter

		if cycle := lm.findDeadlockCycleLocked(txID); len(cycle) > 0 {
			victim := chooseDeadlockVictim(cycle)
			lm.abortTransactionLocked(victim, &DeadlockError{
				VictimTxID: victim,
				Cycle:      slices.Clone(cycle),
			})
		}

		if waiter.done {
			lm.mu.Unlock()
			return <-waiter.result
		}

		lm.mu.Unlock()

		timer := time.NewTimer(lm.waitTimeout)
		select {
		case err := <-waiter.result:
			if !timer.Stop() {
				<-timer.C
			}
			return err
		case <-timer.C:
		}

		lm.mu.Lock()
		if waiter.done {
			lm.mu.Unlock()
			return <-waiter.result
		}
		lm.removeWaiterLocked(waiter)
		delete(lm.waitingByTx, txID)
		waiter.done = true
		lm.mu.Unlock()
		return ErrLockWaitTimeout
	}
}

func (lm *LockManager) Release(txID uint64, resource string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.releaseResourceLocked(txID, resource)
}

func (lm *LockManager) ReleaseAll(txID uint64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.releaseAllResourcesLocked(txID)
	delete(lm.abortedTxs, txID)
}

func (lm *LockManager) IsAborted(txID uint64) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.abortedTxs[txID]
}

func lockResourceID(tableName, indexName, key string) string {
	return strings.Join([]string{tableName, indexName, key}, "\x00")
}

func lockResourceForKey(tableName, indexName string, key types.Comparable) (string, error) {
	if key == nil {
		return "", fmt.Errorf("storage: nil lock key for %s.%s", tableName, indexName)
	}
	return lockResourceID(tableName, indexName, fmt.Sprintf("%T:%v", key, key)), nil
}

func lockResourcesForKeys(tableName string, keys map[string]types.Comparable) ([]string, error) {
	resources := make([]string, 0, len(keys))
	for indexName, key := range keys {
		resource, err := lockResourceForKey(tableName, indexName, key)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}
	sort.Strings(resources)
	return resources, nil
}

func (lm *LockManager) ensureResourceLocked(resource string) *lockState {
	state, ok := lm.resources[resource]
	if !ok {
		state = &lockState{}
		lm.resources[resource] = state
	}
	return state
}

func (lm *LockManager) recordHeldResourceLocked(txID uint64, resource string) {
	held, ok := lm.heldByTx[txID]
	if !ok {
		held = make(map[string]struct{})
		lm.heldByTx[txID] = held
	}
	held[resource] = struct{}{}
}

func (lm *LockManager) findDeadlockCycleLocked(startTxID uint64) []uint64 {
	seen := make(map[uint64]int)
	path := make([]uint64, 0, 4)
	current := startTxID

	for {
		if idx, ok := seen[current]; ok {
			return slices.Clone(path[idx:])
		}

		seen[current] = len(path)
		path = append(path, current)

		waiter, ok := lm.waitingByTx[current]
		if !ok {
			return nil
		}

		state := lm.resources[waiter.resource]
		if state == nil || state.holder == 0 {
			return nil
		}

		current = state.holder
	}
}

func chooseDeadlockVictim(cycle []uint64) uint64 {
	victim := cycle[0]
	for _, txID := range cycle[1:] {
		if txID > victim {
			victim = txID
		}
	}
	return victim
}

func (lm *LockManager) abortTransactionLocked(txID uint64, err error) {
	if _, already := lm.abortedTxs[txID]; already {
		return
	}
	lm.abortedTxs[txID] = err

	if waiter, ok := lm.waitingByTx[txID]; ok {
		lm.removeWaiterLocked(waiter)
		delete(lm.waitingByTx, txID)
		lm.finishWaiterLocked(waiter, err)
	}

	lm.releaseAllResourcesLocked(txID)
}

func (lm *LockManager) releaseAllResourcesLocked(txID uint64) {
	held, ok := lm.heldByTx[txID]
	if !ok {
		return
	}

	resources := make([]string, 0, len(held))
	for resource := range held {
		resources = append(resources, resource)
	}

	for _, resource := range resources {
		lm.releaseResourceLocked(txID, resource)
	}

	delete(lm.heldByTx, txID)
}

func (lm *LockManager) releaseResourceLocked(txID uint64, resource string) {
	state := lm.resources[resource]
	if state == nil || state.holder != txID {
		return
	}

	state.holder = 0
	if held, ok := lm.heldByTx[txID]; ok {
		delete(held, resource)
		if len(held) == 0 {
			delete(lm.heldByTx, txID)
		}
	}

	lm.grantNextWaiterLocked(resource, state)
}

func (lm *LockManager) grantNextWaiterLocked(resource string, state *lockState) {
	for len(state.waiters) > 0 {
		waiter := state.waiters[0]
		state.waiters = state.waiters[1:]

		if waiter.done {
			continue
		}
		if err := lm.abortedTxs[waiter.txID]; err != nil {
			delete(lm.waitingByTx, waiter.txID)
			lm.finishWaiterLocked(waiter, err)
			continue
		}

		state.holder = waiter.txID
		lm.recordHeldResourceLocked(waiter.txID, resource)
		delete(lm.waitingByTx, waiter.txID)
		lm.finishWaiterLocked(waiter, nil)
		return
	}

	if state.holder == 0 {
		delete(lm.resources, resource)
	}
}

func (lm *LockManager) removeWaiterLocked(waiter *lockWaiter) {
	state := lm.resources[waiter.resource]
	if state == nil {
		return
	}
	for i, candidate := range state.waiters {
		if candidate == waiter {
			state.waiters = append(state.waiters[:i], state.waiters[i+1:]...)
			break
		}
	}
	if state.holder == 0 && len(state.waiters) == 0 {
		delete(lm.resources, waiter.resource)
	}
}

func (lm *LockManager) finishWaiterLocked(waiter *lockWaiter, err error) {
	if waiter.done {
		return
	}
	waiter.done = true
	waiter.result <- err
}
