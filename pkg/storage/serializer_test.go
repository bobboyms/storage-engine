package storage

import (
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestSerializeDocumentEntry_Protobuf(t *testing.T) {
	testCases := []struct {
		name      string
		tableName string
		indexName string
		key       types.Comparable
		document  []byte
	}{
		{
			name:      "IntKey",
			tableName: "users",
			indexName: "id",
			key:       types.IntKey(123),
			document:  []byte(`{"name": "Alice"}`),
		},
		{
			name:      "VarcharKey",
			tableName: "products",
			indexName: "sku",
			key:       types.VarcharKey("ABC-123"),
			document:  []byte(`{"price": 99.99}`),
		},
		{
			name:      "BoolKey",
			tableName: "settings",
			indexName: "active",
			key:       types.BoolKey(true),
			document:  []byte(`{"updated": true}`),
		},
		{
			name:      "FloatKey",
			tableName: "metrics",
			indexName: "value",
			key:       types.FloatKey(12.34),
			document:  []byte(`{"unit": "ms"}`),
		},
		{
			name:      "DateKey",
			tableName: "events",
			indexName: "timestamp",
			key:       types.DateKey(time.Now().Truncate(time.Millisecond)),
			document:  []byte(`{"type": "click"}`),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := SerializeDocumentEntry(tc.tableName, tc.indexName, tc.key, tc.document)
			if err != nil {
				t.Fatalf("SerializeDocumentEntry failed: %v", err)
			}

			tName, iName, gotKey, gotDoc, err := DeserializeDocumentEntry(data)
			if err != nil {
				t.Fatalf("DeserializeDocumentEntry failed: %v", err)
			}

			if tName != tc.tableName {
				t.Errorf("Expected table name %q, got %q", tc.tableName, tName)
			}
			if iName != tc.indexName {
				t.Errorf("Expected index name %q, got %q", tc.indexName, iName)
			}
			if string(gotDoc) != string(tc.document) {
				t.Errorf("Expected document %q, got %q", string(tc.document), string(gotDoc))
			}

			// Key comparison
			if tc.key.Compare(gotKey) != 0 {
				t.Errorf("Key mismatch. Expected %v, got %v", tc.key, gotKey)
			}
		})
	}
}

func TestSerializeDocumentEntry_EdgeCases(t *testing.T) {
	t.Run("EmptyDocument", func(t *testing.T) {
		data, err := SerializeDocumentEntry("t", "i", types.IntKey(1), []byte{})
		if err != nil {
			t.Fatal(err)
		}
		_, _, _, doc, err := DeserializeDocumentEntry(data)
		if err != nil {
			t.Fatal(err)
		}
		if len(doc) != 0 {
			t.Errorf("Expected empty doc, got %d bytes", len(doc))
		}
	})

	t.Run("NilDocument", func(t *testing.T) {
		data, err := SerializeDocumentEntry("t", "i", types.IntKey(1), nil)
		if err != nil {
			t.Fatal(err)
		}
		_, _, _, doc, err := DeserializeDocumentEntry(data)
		if err != nil {
			t.Fatal(err)
		}
		if len(doc) != 0 {
			t.Errorf("Expected nil/empty doc, got %d bytes", len(doc))
		}
	})

	t.Run("UnsupportedKeyType", func(t *testing.T) {
		type UnsupportedKey struct{ types.Comparable }
		_, err := SerializeDocumentEntry("t", "i", UnsupportedKey{}, nil)
		if err == nil {
			t.Error("Expected error for unsupported key type, got nil")
		}
	})
}
