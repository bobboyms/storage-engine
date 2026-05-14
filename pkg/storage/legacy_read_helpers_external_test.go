package storage_test

import (
	"testing"

	"github.com/bobboyms/storage-engine/pkg/codec/bsoncodec"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// External-test mirrors of the internal helpers. Same shape but typed for
// the public package so files in `package storage_test` can use them.

var externalLegacyCodec = bsoncodec.New()

func externalLegacyDecode(t testing.TB, raw []byte) string {
	t.Helper()
	if raw == nil {
		return ""
	}
	text, err := externalLegacyCodec.DecodeToText(raw)
	if err != nil {
		return string(raw)
	}
	return text
}

func getDocStringExt(t testing.TB, se *storage.StorageEngine, table, idx string, key types.Comparable) (string, bool, error) {
	t.Helper()
	raw, found, err := se.GetBytes(table, idx, key)
	if err != nil || !found {
		return "", found, err
	}
	return externalLegacyDecode(t, raw), true, nil
}

func getDocStringExtTx(t testing.TB, tx *storage.Transaction, table, idx string, key types.Comparable) (string, bool, error) {
	t.Helper()
	raw, found, err := tx.GetBytes(table, idx, key)
	if err != nil || !found {
		return "", found, err
	}
	return externalLegacyDecode(t, raw), true, nil
}

func scanAllDocsExt(t testing.TB, se *storage.StorageEngine, table, idx string) ([]string, error) {
	t.Helper()
	return scanRangeDocsExt(t, se, table, idx, nil, nil)
}

func scanRangeDocsExt(t testing.TB, se *storage.StorageEngine, table, idx string, lo, hi types.Comparable) ([]string, error) {
	t.Helper()
	it, err := se.NewIterator(table, idx, storage.IterOptions{Lower: lo, Upper: hi})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	results := []string{}
	for it.Next() {
		results = append(results, externalLegacyDecode(t, it.Value()))
	}
	return results, it.Err()
}

func scanRangeDocsExtTx(t testing.TB, tx *storage.Transaction, table, idx string, lo, hi types.Comparable) ([]string, error) {
	t.Helper()
	it, err := tx.NewIterator(table, idx, storage.IterOptions{Lower: lo, Upper: hi})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	results := []string{}
	for it.Next() {
		results = append(results, externalLegacyDecode(t, it.Value()))
	}
	return results, it.Err()
}
