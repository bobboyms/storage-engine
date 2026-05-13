package bsoncodec_test

import (
	"strings"
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/codec"
	"github.com/bobboyms/storage-engine/pkg/codec/bsoncodec"
	"github.com/bobboyms/storage-engine/pkg/types"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNewReturnsCodec(t *testing.T) {
	// Compile-time assertion that *bsoncodec.Codec satisfies codec.Codec.
	var _ codec.Codec = bsoncodec.New()
}

func TestParseAndDecodeRoundTrip(t *testing.T) {
	c := bsoncodec.New()

	doc, err := c.Parse(`{"id": 42, "name": "alice"}`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	raw, err := doc.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("expected non-empty encoded bytes")
	}

	text, err := c.DecodeToText(raw)
	if err != nil {
		t.Fatalf("DecodeToText: %v", err)
	}
	if !strings.Contains(text, `"id"`) || !strings.Contains(text, `"name"`) {
		t.Fatalf("decoded text missing fields: %q", text)
	}
}

func TestKeyExtractionTypes(t *testing.T) {
	c := bsoncodec.New()

	cases := []struct {
		name   string
		doc    string
		field  string
		want   types.Comparable
		exists bool
	}{
		{"int", `{"id": 42}`, "id", types.IntKey(42), true},
		{"string", `{"name": "alice"}`, "name", types.VarcharKey("alice"), true},
		{"bool", `{"flag": true}`, "flag", types.BoolKey(true), true},
		{"float", `{"score": 3.5}`, "score", types.FloatKey(3.5), true},
		{"missing", `{"id": 1}`, "absent", nil, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := c.Parse(tc.doc)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			got, ok, err := doc.Key(tc.field)
			if err != nil {
				t.Fatalf("Key: %v", err)
			}
			if ok != tc.exists {
				t.Fatalf("exists: got %v want %v", ok, tc.exists)
			}
			if !tc.exists {
				return
			}
			if got.Compare(tc.want) != 0 {
				t.Fatalf("key value: got %v want %v", got, tc.want)
			}
		})
	}
}

func TestOpenExtractsKeyFromEncodedBytes(t *testing.T) {
	c := bsoncodec.New()

	parsed, err := c.Parse(`{"id": 7, "tag": "x"}`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	raw, err := parsed.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}

	opened, err := c.Open(raw)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	k, ok, err := opened.Key("id")
	if err != nil || !ok {
		t.Fatalf("Key id: ok=%v err=%v", ok, err)
	}
	if k.Compare(types.IntKey(7)) != 0 {
		t.Fatalf("key id: got %v want 7", k)
	}
}

func TestParseInvalidJSONReturnsError(t *testing.T) {
	c := bsoncodec.New()
	if _, err := c.Parse("not-json"); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestOpenInvalidBytesReturnsError(t *testing.T) {
	c := bsoncodec.New()
	if _, err := c.Open([]byte("garbage")); err == nil {
		t.Fatal("expected error for invalid encoded bytes")
	}
}

func TestKeyExtractionHandlesNativeBSONTypes(t *testing.T) {
	c := bsoncodec.New()

	// Build a BSON document containing types JSON parsing won't naturally
	// produce (int64, time.Time, []byte). This exercises the comparable
	// conversion branches reached when decoding stored bytes during read /
	// undo paths.
	raw, err := bson.Marshal(bson.D{
		{Key: "big", Value: int64(1 << 40)},
		{Key: "ts", Value: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)},
		{Key: "blob", Value: []byte("hi")},
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	doc, err := c.Open(raw)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	k, ok, err := doc.Key("big")
	if err != nil || !ok {
		t.Fatalf("Key big: ok=%v err=%v", ok, err)
	}
	if k.Compare(types.IntKey(1<<40)) != 0 {
		t.Fatalf("int64 key mismatch: got %v", k)
	}

	k, ok, err = doc.Key("ts")
	if err != nil || !ok {
		t.Fatalf("Key ts: ok=%v err=%v", ok, err)
	}
	if _, isDate := k.(types.DateKey); !isDate {
		t.Fatalf("ts: expected DateKey, got %T", k)
	}

	k, ok, _ = doc.Key("blob")
	if !ok || k == nil {
		t.Fatal("blob key should fall back to a non-nil Comparable")
	}
}
