package storage

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/bobboyms/storage-engine/pkg/types"
	"google.golang.org/protobuf/proto"
)

const compensationEntryHeaderSize = 21

// SerializeDocumentEntry serializa uma entrada com documento para WAL usando Protobuf
func SerializeDocumentEntry(tableName, indexName string, key types.Comparable, document []byte) ([]byte, error) {
	entryKey, err := serializeKeyToProto(key)
	if err != nil {
		return nil, err
	}

	entry := &DocumentEntry{
		TableName: tableName,
		IndexName: indexName,
		Document:  document,
		Key:       entryKey,
	}

	return proto.Marshal(entry)
}

// SerializeMultiIndexEntry serializa uma entrada com múltiplos índices para WAL
func SerializeMultiIndexEntry(tableName string, keys map[string]types.Comparable, document []byte) ([]byte, error) {
	protoKeys := make(map[string]*Key)
	for name, k := range keys {
		pk, err := serializeKeyToProto(k)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize key for index %s: %w", name, err)
		}
		protoKeys[name] = pk
	}

	entry := &MultiIndexEntry{
		TableName: tableName,
		Keys:      protoKeys,
		Document:  document,
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

	key, err = deserializeKeyFromProto(entry.Key)
	return
}

// DeserializeMultiIndexEntry desserializa uma entrada com múltiplos índices do WAL
func DeserializeMultiIndexEntry(data []byte) (tableName string, keys map[string]types.Comparable, document []byte, err error) {
	entry := &MultiIndexEntry{}
	if err = proto.Unmarshal(data, entry); err != nil {
		return
	}

	tableName = entry.TableName
	document = entry.Document
	keys = make(map[string]types.Comparable)

	for name, pk := range entry.Keys {
		k, kErr := deserializeKeyFromProto(pk)
		if kErr != nil {
			err = fmt.Errorf("failed to deserialize key for index %s: %w", name, kErr)
			return
		}
		keys[name] = k
	}

	return
}

func SerializeCompensationEntry(originalLSN uint64, originalEntryType uint8, originalPayload []byte, undoNextLSN uint64) []byte {
	buf := make([]byte, compensationEntryHeaderSize+len(originalPayload))
	binary.LittleEndian.PutUint64(buf[0:8], originalLSN)
	buf[8] = originalEntryType
	binary.LittleEndian.PutUint64(buf[9:17], undoNextLSN)
	binary.LittleEndian.PutUint32(buf[17:21], uint32(len(originalPayload)))
	copy(buf[21:], originalPayload)
	return buf
}

func DeserializeCompensationEntry(data []byte) (originalLSN uint64, originalEntryType uint8, originalPayload []byte, undoNextLSN uint64, err error) {
	if len(data) < compensationEntryHeaderSize {
		err = fmt.Errorf("compensation entry too short: %d", len(data))
		return
	}

	originalLSN = binary.LittleEndian.Uint64(data[0:8])
	originalEntryType = data[8]
	undoNextLSN = binary.LittleEndian.Uint64(data[9:17])
	payloadLen := int(binary.LittleEndian.Uint32(data[17:21]))
	if len(data) < compensationEntryHeaderSize+payloadLen {
		err = fmt.Errorf("compensation payload truncated: header=%d payload=%d data=%d", compensationEntryHeaderSize, payloadLen, len(data))
		return
	}
	originalPayload = data[21 : 21+payloadLen]
	return
}

func serializeKeyToProto(key types.Comparable) (*Key, error) {
	if key == nil {
		return &Key{}, nil
	}
	pk := &Key{}
	switch k := key.(type) {
	case types.IntKey:
		pk.Value = &Key_IntValue{IntValue: int64(k)}
	case types.VarcharKey:
		pk.Value = &Key_StringValue{StringValue: string(k)}
	case types.BoolKey:
		pk.Value = &Key_BoolValue{BoolValue: bool(k)}
	case types.FloatKey:
		pk.Value = &Key_FloatValue{FloatValue: float64(k)}
	case types.DateKey:
		pk.Value = &Key_DateValue{DateValue: time.Time(k).UnixNano()}
	default:
		return nil, fmt.Errorf("unsupported key type: %T", k)
	}
	return pk, nil
}

func deserializeKeyFromProto(pk *Key) (types.Comparable, error) {
	if pk == nil || pk.Value == nil {
		return nil, fmt.Errorf("missing key value")
	}

	switch v := pk.Value.(type) {
	case *Key_IntValue:
		return types.IntKey(v.IntValue), nil
	case *Key_StringValue:
		return types.VarcharKey(v.StringValue), nil
	case *Key_BoolValue:
		return types.BoolKey(v.BoolValue), nil
	case *Key_FloatValue:
		return types.FloatKey(v.FloatValue), nil
	case *Key_DateValue:
		return types.DateKey(time.Unix(0, v.DateValue)), nil
	default:
		return nil, fmt.Errorf("unsupported key type in protobuf")
	}
}
