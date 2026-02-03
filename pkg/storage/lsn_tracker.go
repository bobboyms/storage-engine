package storage

import (
	"sync/atomic"
)

// LSNTracker gerencia o Log Sequence Number de forma thread-safe
type LSNTracker struct {
	current uint64
	// Utilizando sync.Mutex para operações que não são puramente atômicas se necessário,
	// mas para um contador simples, atomic é suficiente e mais rápido.
	// Mantemos a struct preparada para lógica mais complexa se precisar.
}

func NewLSNTracker(start uint64) *LSNTracker {
	return &LSNTracker{
		current: start,
	}
}

// Next incrementa e retorna o próximo LSN
func (lt *LSNTracker) Next() uint64 {
	return atomic.AddUint64(&lt.current, 1)
}

// Current retorna o LSN atual
func (lt *LSNTracker) Current() uint64 {
	return atomic.LoadUint64(&lt.current)
}

// Set define o LSN atual (usado no recovery)
func (lt *LSNTracker) Set(val uint64) {
	atomic.StoreUint64(&lt.current, val)
}
