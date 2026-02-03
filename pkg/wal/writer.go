package wal

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"
)

// WALWriter gerencia a escrita no log
type WALWriter struct {
	mu      sync.Mutex
	file    *os.File
	writer  *bufio.Writer
	options Options

	// Estado para Batching
	batchBytes int64 // Bytes escritos desde o último sync

	// Controle de Threads
	done   chan struct{}
	ticker *time.Ticker
	closed bool
}

// NewWALWriter cria um novo Writer
func NewWALWriter(path string, opts Options) (*WALWriter, error) {
	// Garante que o diretório existe
	// Nota: Em uma implementação completa de segmented WAL, gerenciariamos arquivos rotacionados.
	// Por enquanto, faremos um único arquivo append-only.

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("falha ao abrir arquivo WAL: %w", err)
	}

	w := &WALWriter{
		file:    f,
		writer:  bufio.NewWriterSize(f, opts.BufferSize),
		options: opts,
		done:    make(chan struct{}),
	}

	// Inicia rotina de background sync se necessário
	if opts.SyncPolicy == SyncInterval {
		w.ticker = time.NewTicker(opts.SyncIntervalDuration)
		go w.backgroundSync()
	}

	return w, nil
}

// WriteEntry escreve uma entrada no WAL
func (w *WALWriter) WriteEntry(entry *WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Escreve no buffer (memória)
	n, err := entry.WriteTo(w.writer)
	if err != nil {
		return err
	}

	w.batchBytes += n

	// Aplica política de Sync
	switch w.options.SyncPolicy {
	case SyncEveryWrite:
		return w.syncLocked()

	case SyncBatch:
		if w.batchBytes >= w.options.SyncBatchBytes {
			return w.syncLocked()
		}
	}

	return nil
}

// Sync força a persistência em disco
func (w *WALWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncLocked()
}

func (w *WALWriter) syncLocked() error {
	// Flush do buffer para o descritor de arquivo
	if err := w.writer.Flush(); err != nil {
		return err
	}

	// fsync do arquivo físico
	if err := w.file.Sync(); err != nil {
		return err
	}

	w.batchBytes = 0
	return nil
}

// Close fecha o arquivo e encerra rotinas
func (w *WALWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	if w.ticker != nil {
		w.ticker.Stop()
		close(w.done)
	}

	// Último flush
	if err := w.syncLocked(); err != nil {
		w.file.Close() // Try to close anyway
		return err
	}

	return w.file.Close()
}

func (w *WALWriter) backgroundSync() {
	for {
		select {
		case <-w.ticker.C:
			w.Sync() // Thread-safe
		case <-w.done:
			return
		}
	}
}
