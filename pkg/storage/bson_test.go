package storage

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestDoesTheKeyExist(t *testing.T) {
	tests := []struct {
		name          string
		doc           bson.D
		key           string
		expectedFound bool
		expectedType  DataType
	}{
		{
			name:          "Int Key",
			doc:           bson.D{{Key: "age", Value: 30}},
			key:           "age",
			expectedFound: true,
			expectedType:  TypeInt,
		},
		{
			name:          "Int32 Key",
			doc:           bson.D{{Key: "count", Value: int32(100)}},
			key:           "count",
			expectedFound: true,
			expectedType:  TypeInt,
		},
		{
			name:          "Int64 Key",
			doc:           bson.D{{Key: "id", Value: int64(123456789)}},
			key:           "id",
			expectedFound: true,
			expectedType:  TypeInt,
		},
		{
			name:          "String Key",
			doc:           bson.D{{Key: "name", Value: "Thiago"}},
			key:           "name",
			expectedFound: true,
			expectedType:  TypeVarchar,
		},
		{
			name:          "Boolean Key",
			doc:           bson.D{{Key: "isActive", Value: true}},
			key:           "isActive",
			expectedFound: true,
			expectedType:  TypeBoolean,
		},
		{
			name:          "Float64 Key",
			doc:           bson.D{{Key: "price", Value: 19.99}},
			key:           "price",
			expectedFound: true,
			expectedType:  TypeFloat,
		},
		{
			name:          "Date Key (time.Time)",
			doc:           bson.D{{Key: "createdAt", Value: time.Now()}},
			key:           "createdAt",
			expectedFound: true,
			expectedType:  TypeDate,
		},
		{
			name:          "Key Not Found",
			doc:           bson.D{{Key: "name", Value: "Thiago"}},
			key:           "email",
			expectedFound: false,
			expectedType:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			found, dataType := DoesTheKeyExist(tt.doc, tt.key)
			if found != tt.expectedFound {
				t.Errorf("DoesTheKeyExist() found = %v, want %v", found, tt.expectedFound)
			}
			if found && dataType != tt.expectedType {
				t.Errorf("DoesTheKeyExist() dataType = %v, want %v", dataType, tt.expectedType)
			}
		})
	}
}
