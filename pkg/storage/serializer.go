package storage

import (
	"fmt"
	"time"

	"github.com/bobboyms/storage-engine/pkg/types"
	"google.golang.org/protobuf/proto"
)

// SerializeDocumentEntry serializa uma entrada com documento para WAL usando Protobuf
func SerializeDocumentEntry(tableName, indexName string, key types.Comparable, document []byte) ([]byte, error) {
	entry := &DocumentEntry{
		TableName: tableName,
		IndexName: indexName,
		Document:  document,
		Key:       &Key{},
	}

	// Key serialization
	switch k := key.(type) {
	case types.IntKey:
		entry.Key.Value = &Key_IntValue{IntValue: int64(k)}
	case types.VarcharKey:
		entry.Key.Value = &Key_StringValue{StringValue: string(k)}
	case types.BoolKey:
		entry.Key.Value = &Key_BoolValue{BoolValue: bool(k)}
	case types.FloatKey:
		entry.Key.Value = &Key_FloatValue{FloatValue: float64(k)}
	case types.DateKey:
		entry.Key.Value = &Key_DateValue{DateValue: time.Time(k).UnixNano()}
	default:
		return nil, fmt.Errorf("unsupported key type: %T", k)
	}

	return proto.Marshal(entry)
}

// DeserializeDocumentEntry desserializa uma entrada com documento do WAL usando Protobuf
func DeserializeDocumentEntry(data []byte) (tableName, indexName string, key types.Comparable, document []byte, err error) {
	entry := &DocumentEntry{}
	if err = proto.Unmarshal(data, entry); err != nil {
		return
	}

	tableName = entry.TableName
	indexName = entry.IndexName
	document = entry.Document

	// Key deserialization
	if entry.Key != nil && entry.Key.Value != nil {
		switch v := entry.Key.Value.(type) {
		case *Key_IntValue:
			key = types.IntKey(v.IntValue)
		case *Key_StringValue:
			key = types.VarcharKey(v.StringValue)
		case *Key_BoolValue:
			key = types.BoolKey(v.BoolValue)
		case *Key_FloatValue:
			key = types.FloatKey(v.FloatValue)
		case *Key_DateValue:
			key = types.DateKey(time.Unix(0, v.DateValue))
		default:
			err = fmt.Errorf("unsupported key type in protobuf")
		}
	} else {
		err = fmt.Errorf("missing key in document entry")
	}

	return
}
