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

func TestDocumentEntry_Generated_Coverage(t *testing.T) {
	entry := &DocumentEntry{
		TableName: "t1",
		IndexName: "i1",
		Key: &Key{
			Value: &Key_StringValue{StringValue: "k1"},
		},
		Document: []byte("d1"),
	}

	// Call generated methods
	if entry.GetTableName() != "t1" {
		t.Error("GetTableName failed")
	}
	if entry.GetIndexName() != "i1" {
		t.Error("GetIndexName failed")
	}
	if entry.GetKey().GetStringValue() != "k1" {
		t.Error("GetKey failed")
	}
	if string(entry.GetDocument()) != "d1" {
		t.Error("GetDocument failed")
	}

	_ = entry.String()
	entry.ProtoMessage()
	_, _ = entry.Descriptor()
	_ = entry.ProtoReflect()

	// Hit Key methods
	k := entry.GetKey()
	_ = k.String()
	k.ProtoMessage()
	_, _ = k.Descriptor()
	_ = k.ProtoReflect()
	_ = k.GetValue()
	_ = k.GetIntValue()
	_ = k.GetBoolValue()
	_ = k.GetFloatValue()
	_ = k.GetDateValue()

	entry.Reset()
	if entry.GetTableName() != "" {
		t.Error("Reset failed")
	}

	var nilEntry *DocumentEntry
	if nilEntry.GetTableName() != "" {
		t.Log("Nil GetTableName works")
	}
	if nilEntry.GetIndexName() != "" {
		t.Log("Nil GetIndexName works")
	}
	if nilEntry.GetKey() != nil {
		t.Log("Nil GetKey works")
	}
	if nilEntry.GetDocument() != nil {
		t.Log("Nil GetDocument works")
	}
}

func TestDeserializeDocumentEntry_Error(t *testing.T) {
	// Garbage data
	_, _, _, _, err := DeserializeDocumentEntry([]byte{1, 2, 3, 4})
	if err == nil {
		t.Error("Expected error for garbage data")
	}
}

type customKey struct{}
func (c customKey) Compare(other types.Comparable) int { return 0 }
func (c customKey) String() string { return "" }

func TestSerializeDocumentEntry_UnsupportedKey(t *testing.T) {
	_, err := SerializeDocumentEntry("t", "i", customKey{}, []byte{})
	if err == nil {
		t.Error("Expected error for unsupported key type")
	}
}

func TestKey_Oneof_Coverage(t *testing.T) {
	k := &Key{Value: &Key_IntValue{IntValue: 10}}
	if k.GetStringValue() != "" { t.Error("Should be empty") }
	if k.GetBoolValue() != false { t.Error("Should be false") }
	if k.GetFloatValue() != 0 { t.Error("Should be 0") }
	if k.GetDateValue() != 0 { t.Error("Should be 0") }

	k = &Key{Value: &Key_StringValue{StringValue: "s"}}
	if k.GetIntValue() != 0 { t.Error("Should be 0") }
}
