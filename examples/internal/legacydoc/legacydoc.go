// Package legacydoc bridges the engine's bytes/Iterator API back to the
// string-based shapes the examples use for printable output. It exists
// only to keep the examples readable; new consumers should use
// storage.GetBytes / storage.NewIterator directly and decode payloads
// with their codec of choice.
package legacydoc

import (
	"context"

	"github.com/bobboyms/storage-engine/pkg/codec/bsoncodec"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

var codec = bsoncodec.New()

// Decode returns the textual (JSON-ish) form of the raw heap payload. If
// the bytes are not BSON it falls back to a plain string conversion.
func Decode(raw []byte) string {
	if raw == nil {
		return ""
	}
	text, err := codec.DecodeToText(raw)
	if err != nil {
		return string(raw)
	}
	return text
}

// Fetch wraps StorageEngine.GetBytes + Decode.
func Fetch(engine *storage.StorageEngine, table, idx string, key types.Comparable) (string, bool, error) {
	raw, found, err := engine.GetBytes(context.Background(), table, idx, key)
	if err != nil || !found {
		return "", found, err
	}
	return Decode(raw), true, nil
}

// FetchTx is the transaction-scoped variant.
func FetchTx(tx *storage.Transaction, table, idx string, key types.Comparable) (string, bool, error) {
	raw, found, err := tx.GetBytes(context.Background(), table, idx, key)
	if err != nil || !found {
		return "", found, err
	}
	return Decode(raw), true, nil
}

// Range drains the iterator scoped to [lo, hi] (inclusive). nil bounds
// are unbounded.
func Range(engine *storage.StorageEngine, table, idx string, lo, hi types.Comparable) ([]string, error) {
	it, err := engine.NewIterator(context.Background(), table, idx, storage.IterOptions{Lower: lo, Upper: hi})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := []string{}
	for it.Next() {
		out = append(out, Decode(it.Value()))
	}
	return out, it.Err()
}

// All is shorthand for Range(engine, table, idx, nil, nil).
func All(engine *storage.StorageEngine, table, idx string) ([]string, error) {
	return Range(engine, table, idx, nil, nil)
}
