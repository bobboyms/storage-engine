package storage_test

import (
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/storage"
)

func TestProto_DocumentEntry_Getters(t *testing.T) {
	entry := &storage.DocumentEntry{
		TableName: "users",
		IndexName: "id",
		Document:  []byte("doc"),
		// Key is nil
	}

	if entry.GetTableName() != "users" {
		t.Error("GetTableName failed")
	}
	if entry.GetIndexName() != "id" {
		t.Error("GetIndexName failed")
	}
	if string(entry.GetDocument()) != "doc" {
		t.Error("GetDocument failed")
	}
	if entry.GetKey() != nil {
		t.Error("GetKey should be nil")
	}
}

func TestProto_Key_Getters(t *testing.T) {
	// Int Key
	kInt := &storage.Key{
		Value: &storage.Key_IntValue{IntValue: 123},
	}
	if kInt.GetIntValue() != 123 {
		t.Error("GetIntValue failed")
	}
	// Fallback for defaults
	if kInt.GetStringValue() != "" {
		t.Error("GetStringValue should be empty")
	}

	// String Key
	kStr := &storage.Key{
		Value: &storage.Key_StringValue{StringValue: "s"},
	}
	if kStr.GetStringValue() != "s" {
		t.Error("GetStringValue failed")
	}

	// Bool Key
	kBool := &storage.Key{
		Value: &storage.Key_BoolValue{BoolValue: true},
	}
	if !kBool.GetBoolValue() {
		t.Error("GetBoolValue failed")
	}

	// Float Key
	kFloat := &storage.Key{
		Value: &storage.Key_FloatValue{FloatValue: 1.5},
	}
	if kFloat.GetFloatValue() != 1.5 {
		t.Error("GetFloatValue failed")
	}

	// Date Key
	now := time.Now().UnixNano()
	kDate := &storage.Key{
		Value: &storage.Key_DateValue{DateValue: now},
	}
	if kDate.GetDateValue() != now {
		t.Error("GetDateValue failed")
	}

	// Nil safety
	var nilKey *storage.Key
	if nilKey.GetIntValue() != 0 {
		t.Error("Nil GetIntValue should be 0")
	}
	if nilKey.GetValue() != nil {
		t.Error("Nil GetValue should be nil")
	}
}
