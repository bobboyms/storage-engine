package wal

import (
	"time"

	"github.com/bobboyms/storage-engine/pkg/crypto"
)

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

	// Cipher opcional para TDE (Transparent Data Encryption).
	// Se nil, o WAL é escrito em claro (comportamento padrão).
	// Quando configurado, o body das páginas do WAL é cifrado via
	// pagestore.PageFile; headers físicos de página ficam em claro para
	// validação de integridade, mas entries lógicas do WAL não ficam.
	Cipher crypto.Cipher

	// MaxSegmentBytes habilita rotação automática do WAL quando o arquivo
	// ativo atinge este tamanho. Zero ou negativo desliga rotação.
	MaxSegmentBytes int64

	// RetentionSegments define quantos segmentos antigos, já cobertos por
	// checkpoint, devem permanecer no diretório ativo após truncate.
	// Segmentos necessários para recovery desde o último checkpoint nunca
	// são removidos por essa política.
	RetentionSegments int

	// ArchiveDir, quando configurado, recebe uma cópia dos segmentos antes
	// de eles serem removidos do diretório ativo.
	ArchiveDir string
}

// DefaultOptions retorna uma configuração segura por padrão:
// SyncEveryWrite (durabilidade estrita — cada WriteEntry fsyncado).
//
// Uso em produção: DefaultOptions é o caminho correto. Cada commit é
// persistido antes de retornar — zero janela de perda.
//
// Se o workload pode tolerar perda de alguns ms (analytics, caches),
// troque explicitamente pra SyncInterval/SyncBatch — é opt-in consciente,
// não o padrão.
func DefaultOptions() Options {
	return Options{
		DirPath:              "./wal_data",
		BufferSize:           64 * 1024, // 64KB bufio buffer
		SyncPolicy:           SyncEveryWrite,
		SyncIntervalDuration: 200 * time.Millisecond, // só aplicável a SyncInterval
		SyncBatchBytes:       1 * 1024 * 1024,        // só aplicável a SyncBatch
		MaxSegmentBytes:      64 * 1024 * 1024,
		RetentionSegments:    1,
	}
}

// PerformanceOptions retorna a configuração antiga (SyncInterval 200ms).
// Use SOMENTE quando você aceita janela de perda em troca de throughput,
// e documentou isso pro seu cliente no SLA.
func PerformanceOptions() Options {
	return Options{
		DirPath:              "./wal_data",
		BufferSize:           64 * 1024,
		SyncPolicy:           SyncInterval,
		SyncIntervalDuration: 200 * time.Millisecond,
		SyncBatchBytes:       1 * 1024 * 1024,
		MaxSegmentBytes:      64 * 1024 * 1024,
		RetentionSegments:    1,
	}
}
