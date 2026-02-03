package storage_test

import (
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestBson_Unmarshal(t *testing.T) {
	// Valid BSON
	doc := bson.D{{Key: "foo", Value: "bar"}}
	data, _ := storage.MarshalBson(doc)

	res, err := storage.UnmarshalBson(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if len(res) != 1 || res[0].Key != "foo" || res[0].Value != "bar" {
		t.Error("Unmarshal content mismatch")
	}

	// Invalid BSON
	_, err = storage.UnmarshalBson([]byte("garbage"))
	if err == nil {
		t.Error("Expected error for invalid bson")
	}
}

func TestBson_DoesTheKeyExist_Types(t *testing.T) {
	doc := bson.D{
		{Key: "int", Value: int64(1)},
		{Key: "int32", Value: int32(1)},
		{Key: "int_sys", Value: int(1)},
		{Key: "str", Value: "s"},
		{Key: "bool", Value: true},
		{Key: "float", Value: 1.5},
		{Key: "date", Value: time.Now()},
		{Key: "unknown", Value: []byte("bytes")},
	}

	tests := []struct {
		key      string
		expected storage.DataType
	}{
		{"int", storage.TypeInt},
		{"int32", storage.TypeInt},
		{"int_sys", storage.TypeInt},
		{"str", storage.TypeVarchar},
		{"bool", storage.TypeBoolean},
		{"float", storage.TypeFloat},
		{"date", storage.TypeDate},
		{"unknown", storage.TypeVarchar}, // Fallback
	}

	for _, tc := range tests {
		exists, dt := storage.DoesTheKeyExist(doc, tc.key)
		if !exists {
			t.Errorf("Key %s should exist", tc.key)
		}
		if dt != tc.expected {
			t.Errorf("Key %s expected type %s got %s", tc.key, tc.expected, dt)
		}
	}

	exists, _ := storage.DoesTheKeyExist(doc, "missing")
	if exists {
		t.Error("Missing key should not exist")
	}
}
