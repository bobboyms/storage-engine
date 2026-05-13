// Package codec defines the document encoding contract used by the storage
// engine. The engine treats stored payloads as opaque bytes and relies on a
// Codec implementation to translate between a textual form (typically JSON),
// the canonical byte form persisted on the heap, and the typed values needed
// to populate secondary indexes.
package codec

import "github.com/bobboyms/storage-engine/pkg/types"

// Document is a parsed view over a document. It can be materialised back into
// its canonical encoded form (Bytes) and queried for indexed field values
// (Key). Implementations are expected to be cheap to use once obtained — the
// codec is the one responsible for parsing work.
type Document interface {
	// Bytes returns the canonical encoded form persisted by the engine.
	Bytes() ([]byte, error)

	// Key returns the typed value of `field` from the document. The second
	// return value reports whether the field is present.
	Key(field string) (types.Comparable, bool, error)
}

// Codec encodes and decodes documents handled by the storage engine.
type Codec interface {
	// Parse converts a textual representation (e.g. JSON) into a Document.
	Parse(doc string) (Document, error)

	// Open wraps already encoded bytes (as previously returned by
	// Document.Bytes) into a Document handle.
	Open(raw []byte) (Document, error)

	// DecodeToText converts encoded bytes back into the textual
	// representation the caller originally supplied to Parse.
	DecodeToText(raw []byte) (string, error)
}
