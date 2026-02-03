package wal

import "time"

// SyncPolicy define a estratégia de durabilidade
type SyncPolicy int

const (
	// SyncEveryWrite chama fsync() após cada escrita.
	// Mais seguro, menor performance.
	SyncEveryWrite SyncPolicy = iota

	// SyncInterval chama fsync() periodicamente (background).
	// Balanceado.
	SyncInterval

	// SyncBatch chama fsync() quando o buffer atinge um tamanho ou contagem.
	// Alta performance.
	SyncBatch
)

// Options configura o WAL Writer
type Options struct {
	// Caminho do diretório onde os logs serão salvos
	DirPath string

	// Tamanho do buffer em memória antes de flush para o SO (bufio)
	BufferSize int

	// Política de Sync
	SyncPolicy SyncPolicy

	// Intervalo para SyncInterval
	SyncIntervalDuration time.Duration

	// Tamanho acumulado em bytes para disparar Sync (apenas SyncBatch)
	SyncBatchBytes int64
}

// DefaultOptions retorna uma configuração segura
func DefaultOptions() Options {
	return Options{
		DirPath:              "./wal_data",
		BufferSize:           64 * 1024, // 64KB bufio buffer
		SyncPolicy:           SyncInterval,
		SyncIntervalDuration: 200 * time.Millisecond,
		SyncBatchBytes:       1 * 1024 * 1024, // 1MB
	}
}
