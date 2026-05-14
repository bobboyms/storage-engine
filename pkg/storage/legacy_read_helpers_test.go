package storage

import (
	"context"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/codec/bsoncodec"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// Test-only helpers that bridge the old (Get/Scan returning strings) call
// shape onto the new bytes/iterator API. They live in `package storage`
// so internal _test.go files can use them without rewriting every
// content assertion.

var legacyTestCodec = bsoncodec.New()

func legacyDecode(t testing.TB, raw []byte) string {
	t.Helper()
	if raw == nil {
		return ""
	}
	text, err := legacyTestCodec.DecodeToText(raw)
	if err != nil {
		return string(raw)
	}
	return text
}

func getDocString(t testing.TB, se *StorageEngine, table, idx string, key types.Comparable) (string, bool, error) {
	t.Helper()
	raw, found, err := se.GetBytes(context.Background(), table, idx, key)
	if err != nil || !found {
		return "", found, err
	}
	return legacyDecode(t, raw), true, nil
}

func getDocStringTx(t testing.TB, tx *Transaction, table, idx string, key types.Comparable) (string, bool, error) {
	t.Helper()
	raw, found, err := tx.GetBytes(context.Background(), table, idx, key)
	if err != nil || !found {
		return "", found, err
	}
	return legacyDecode(t, raw), true, nil
}

func getDocStringWTx(t testing.TB, tx *WriteTransaction, table, idx string, key types.Comparable) (string, bool, error) {
	t.Helper()
	raw, found, err := tx.GetBytes(context.Background(), table, idx, key)
	if err != nil || !found {
		return "", found, err
	}
	return legacyDecode(t, raw), true, nil
}

func scanRangeDocs(t testing.TB, se *StorageEngine, table, idx string, lo, hi types.Comparable) ([]string, error) {
	t.Helper()
	it, err := se.NewIterator(context.Background(), table, idx, IterOptions{Lower: lo, Upper: hi})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	results := []string{}
	for it.Next() {
		results = append(results, legacyDecode(t, it.Value()))
	}
	return results, it.Err()
}

func scanRangeDocsTx(t testing.TB, tx *Transaction, table, idx string, lo, hi types.Comparable) ([]string, error) {
	t.Helper()
	it, err := tx.NewIterator(context.Background(), table, idx, IterOptions{Lower: lo, Upper: hi})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	results := []string{}
	for it.Next() {
		results = append(results, legacyDecode(t, it.Value()))
	}
	return results, it.Err()
}
