package btree

import "github.com/bobboyms/storage-engine/pkg/types"

// Tree é a abstração da B+ tree page-based usada pelo engine.
type Tree interface {
	// Insert adiciona (key, dataPtr). Comportamento em duplicatas
	// depende da implementação; o path atual sobrescreve a key existsnte.
	Insert(key types.Comparable, dataPtr int64) error

	// Get retorna (value, encontrada, erro).
	Get(key types.Comparable) (int64, bool, error)

	// Upsert busca a key. Se exists, chama fn(oldValue, true); otherwise
	// fn(0, false). Grava o value retornado. Essencial pra MVCC (engine
	// usa isso pra encadear versões via PrevRecordID).
	Upsert(key types.Comparable, fn func(oldValue int64, exists bool) (newValue int64, err error)) error

	// Replace sobrescreve unconditionally.
	Replace(key types.Comparable, dataPtr int64) error

	// Remove apaga a key fisicamente do index. Retorna false quando a
	// key does not exist. O storage engine does not use isso no path MVCC
	// (ele mantém tombstones no heap), mas a operação é útil para uso
	// direto da tree e manutenção.
	Remove(key types.Comparable) (bool, error)

	// Close libera recursos.
	Close() error
}
