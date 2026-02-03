package storage_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/btree"
	"github.com/bobboyms/storage-engine/pkg/storage"
)

type failWriter struct {
	err error
}

func (f *failWriter) Write(p []byte) (n int, err error) {
	return 0, f.err
}

func TestSerializeBPlusTree_NilRoot(t *testing.T) {
	tree := btree.NewTree(3)
	tree.Root = nil // Force nil root behavior
	_, err := storage.SerializeBPlusTree(tree, 0)
	if err == nil {
		t.Error("Expected error for nil root")
	}
}

func TestSerializeNode_WriteError(t *testing.T) {
	node := btree.NewNode(3, true)
	fw := &failWriter{err: errors.New("write failed")}

	err := storage.SerializeNode(fw, node)
	if err == nil {
		t.Error("Expected write error")
	}
}

func TestDeserializeBPlusTree_InvalidMagic(t *testing.T) {
	// 50 bytes of zeros (header is ~30)
	buf := make([]byte, 50)
	// Magic is at offset 0. 0x00000000 is invalid.

	_, _, err := storage.DeserializeBPlusTree(buf)
	if err == nil {
		t.Error("Expected error for invalid magic")
	} else if err.Error() != "invalid checkpoint magic" {
		t.Logf("Got error: %v", err)
	}
}

func TestDeserializeNode_EOF(t *testing.T) {
	// Provide empty buffer
	buf := bytes.NewReader([]byte{})
	_, err := storage.DeserializeNode(buf, 3)
	if err == nil {
		t.Error("Expected error for empty buffer")
	}
}
