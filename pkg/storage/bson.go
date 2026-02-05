package storage

import (
	"fmt"
	"time"

	"github.com/bobboyms/storage-engine/pkg/types"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func MarshalBson(bsonDoc bson.D) ([]byte, error) {
	return bson.Marshal(bsonDoc)
}

func UnmarshalBson(bsonData []byte) (bson.D, error) {
	var doc bson.D
	err := bson.Unmarshal(bsonData, &doc)
	if err != nil {
		return nil, fmt.Errorf("erro no parser nativo: %w", err)
	}
	return doc, nil
}

func JsonToBson(jsonStr string) (bson.D, error) {
	var doc bson.D
	// true = Canonical (estrito), false = Relaxed
	// Tenta converter diretamente de JSON bytes para estrutura BSON interna
	err := bson.UnmarshalExtJSON([]byte(jsonStr), true, &doc)
	if err != nil {
		return nil, fmt.Errorf("erro no parser nativo: %w", err)
	}

	return doc, nil
}

func BsonToJson(bsonData []byte) (string, error) {
	var doc bson.D
	err := bson.Unmarshal(bsonData, &doc)
	if err != nil {
		return "", err
	}

	// Converte estrutura BSON para JSON string
	jsonBytes, err := bson.MarshalExtJSON(doc, false, false)
	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}

func DoesTheKeyExist(doc bson.D, key string) (bool, DataType) {
	for _, v := range doc {
		if v.Key == key {
			switch v.Value.(type) {
			case int, int32, int64:
				return true, TypeInt
			case string:
				return true, TypeVarchar
			case bool:
				return true, TypeBoolean
			case float32, float64:
				return true, TypeFloat
			case time.Time:
				return true, TypeDate
			default:
				// Verificamos via string para tipos que não importamos diretamente (ex: primitive.DateTime)
				if fmt.Sprintf("%T", v.Value) == "primitive.DateTime" {
					return true, TypeDate
				}
				return true, TypeVarchar
			}
		}
	}
	return false, 0
}

func GetValueFromBson(doc bson.D, key string) (types.Comparable, error) {
	for _, v := range doc {
		if v.Key == key {
			switch val := v.Value.(type) {
			case int:
				return types.IntKey(val), nil
			case int32:
				return types.IntKey(val), nil
			case int64:
				return types.IntKey(val), nil
			case string:
				return types.VarcharKey(val), nil
			case bool:
				return types.BoolKey(val), nil
			case float32:
				return types.FloatKey(val), nil
			case float64:
				return types.FloatKey(val), nil
			case time.Time:
				return types.DateKey(val), nil
			default:
				// Helper for primitive.DateTime without import
				if fmt.Sprintf("%T", val) == "primitive.DateTime" {
					// We can't access .Time() without casting.
					// Fallback to string representation logic or just return Varchar
					return types.VarcharKey(fmt.Sprintf("%v", val)), nil
				}
				return types.VarcharKey(fmt.Sprintf("%v", val)), nil
			}
		}
	}
	return nil, fmt.Errorf("key %s not found in document", key)
}
