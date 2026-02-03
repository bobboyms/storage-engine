package storage

import (
	"fmt"
	"time"

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
