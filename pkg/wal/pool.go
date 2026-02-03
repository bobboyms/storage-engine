package wal

import "sync"

// pool.go: Gerenciamento de memória para evitar alocações excessivas no GC

var (
	// Pool de entradas WAL (reutiliza struct WALEntry)
	entryPool = sync.Pool{
		New: func() interface{} {
			return &WALEntry{
				Payload: make([]byte, 0, 4096), // Pre-aloca 4KB
			}
		},
	}

	// Pool de buffers de bytes (para serialização/header)
	bufferPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 0, 8192) // 8KB buffer
			return &buf
		},
	}
)

// AcquireEntry obtém uma entrada do pool
func AcquireEntry() *WALEntry {
	return entryPool.Get().(*WALEntry)
}

// ReleaseEntry devolve a entrada ao pool
func ReleaseEntry(e *WALEntry) {
	e.Header = WALHeader{}    // Zero header
	e.Payload = e.Payload[:0] // Reset payload slice (mantém cap)
	entryPool.Put(e)
}

// AcquireBuffer obtém um buffer de bytes do pool
func AcquireBuffer() *[]byte {
	return bufferPool.Get().(*[]byte)
}

// ReleaseBuffer devolve o buffer ao pool
func ReleaseBuffer(buf *[]byte) {
	*buf = (*buf)[:0]
	bufferPool.Put(buf)
}
