// Package bsoncodec is the default codec implementation used by the storage
// engine. It interprets documents as JSON on the wire and stores them as BSON
// on the heap, leveraging the MongoDB BSON library for marshalling and key
// extraction.
package bsoncodec

import (
	"fmt"
	"time"

	"github.com/bobboyms/storage-engine/pkg/codec"
	"github.com/bobboyms/storage-engine/pkg/types"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Codec implements codec.Codec using BSON as the on-disk format and JSON as
// the textual representation.
type Codec struct{}

// New returns a Codec ready for use. It is safe for concurrent use.
func New() *Codec { return &Codec{} }

// Parse implements codec.Codec.
func (Codec) Parse(doc string) (codec.Document, error) {
	parsed, err := parseJSON(doc)
	if err != nil {
		return nil, err
	}
	return &document{doc: parsed}, nil
}

// Open implements codec.Codec.
func (Codec) Open(raw []byte) (codec.Document, error) {
	parsed, err := unmarshalBSON(raw)
	if err != nil {
		return nil, err
	}
	d := &document{doc: parsed}
	// Preserve the original bytes so callers can avoid a re-marshal.
	d.encoded = append(d.encoded[:0], raw...)
	d.encodedOK = true
	return d, nil
}

// DecodeToText implements codec.Codec.
func (Codec) DecodeToText(raw []byte) (string, error) {
	parsed, err := unmarshalBSON(raw)
	if err != nil {
		return "", err
	}
	jsonBytes, err := bson.MarshalExtJSON(parsed, false, false)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

// document is the BSON-backed implementation of codec.Document.
type document struct {
	doc       bson.D
	encoded   []byte
	encodedOK bool
}

func (d *document) Bytes() ([]byte, error) {
	if d.encodedOK {
		return d.encoded, nil
	}
	raw, err := bson.Marshal(d.doc)
	if err != nil {
		return nil, err
	}
	d.encoded = raw
	d.encodedOK = true
	return raw, nil
}

func (d *document) Key(field string) (types.Comparable, bool, error) {
	for _, v := range d.doc {
		if v.Key != field {
			continue
		}
		return comparableFromValue(v.Value), true, nil
	}
	return nil, false, nil
}

func parseJSON(jsonStr string) (bson.D, error) {
	var doc bson.D
	if err := bson.UnmarshalExtJSON([]byte(jsonStr), true, &doc); err != nil {
		return nil, fmt.Errorf("bsoncodec: parse json: %w", err)
	}
	return doc, nil
}

func unmarshalBSON(raw []byte) (bson.D, error) {
	var doc bson.D
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("bsoncodec: unmarshal bson: %w", err)
	}
	return doc, nil
}

func comparableFromValue(v any) types.Comparable {
	switch val := v.(type) {
	case int:
		return types.IntKey(val)
	case int32:
		return types.IntKey(val)
	case int64:
		return types.IntKey(val)
	case string:
		return types.VarcharKey(val)
	case bool:
		return types.BoolKey(val)
	case float32:
		return types.FloatKey(val)
	case float64:
		return types.FloatKey(val)
	case time.Time:
		return types.DateKey(val)
	case bson.DateTime:
		return types.DateKey(val.Time())
	default:
		return types.VarcharKey(fmt.Sprintf("%v", val))
	}
}
