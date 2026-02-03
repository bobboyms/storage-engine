package wal

import (
	"bytes"
	"testing"
)

func TestWALHeaderEncoding(t *testing.T) {
	original := WALHeader{
		Magic:      WALMagic,
		Version:    WALVersion,
		EntryType:  EntryInsert,
		LSN:        1024,
		PayloadLen: 50,
		CRC32:      0x12345678,
	}

	var buf [HeaderSize]byte
	original.Encode(buf[:])

	var decoded WALHeader
	decoded.Decode(buf[:])

	if decoded != original {
		t.Errorf("Header decoding mismatch.\nExpected: %+v\nGot: %+v", original, decoded)
	}
}

func TestCRC32(t *testing.T) {
	data := []byte("hello WAL world")
	crc := CalculateCRC32(data)

	if !ValidateCRC32(data, crc) {
		t.Error("CRC32 validation failed for valid data")
	}

	if ValidateCRC32([]byte("corrupted"), crc) {
		t.Error("CRC32 validation passed for corrupted data")
	}
}

func TestPool(t *testing.T) {
	// Test Entry Pool
	entry := AcquireEntry()
	if entry == nil {
		t.Fatal("Failed to acquire entry")
	}
	if cap(entry.Payload) < 4096 {
		t.Errorf("Expected payload cap >= 4096, got %d", cap(entry.Payload))
	}

	entry.Header.LSN = 999
	entry.Payload = append(entry.Payload, []byte("test")...)

	ReleaseEntry(entry)

	// Reuse
	entry2 := AcquireEntry()
	if len(entry2.Payload) != 0 {
		t.Error("Released entry payload length should be 0")
	}
	if entry2.Header.LSN != 0 {
		t.Error("Released entry header should be zeroed")
	}
}

func TestEntryWriteTo(t *testing.T) {
	entry := AcquireEntry()
	defer ReleaseEntry(entry)

	payload := []byte("logging data")
	entry.Header = WALHeader{
		Magic:      WALMagic,
		Version:    1,
		EntryType:  EntryInsert,
		LSN:        1,
		PayloadLen: uint32(len(payload)),
		CRC32:      CalculateCRC32(payload),
	}
	entry.Payload = append(entry.Payload, payload...)

	var buf bytes.Buffer
	n, err := entry.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	expectedSize := int64(HeaderSize + len(payload))
	if n != expectedSize {
		t.Errorf("Expected to write %d bytes, wrote %d", expectedSize, n)
	}

	if buf.Len() != int(expectedSize) {
		t.Errorf("Buffer length mismatch. Got %d, want %d", buf.Len(), expectedSize)
	}
}

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()
	if opts.BufferSize <= 0 {
		t.Error("Expected positive BufferSize")
	}
	if opts.SyncPolicy != SyncInterval {
		t.Error("Expected SyncInterval as default")
	}
	if opts.SyncIntervalDuration <= 0 {
		t.Error("Expected positive SyncIntervalDuration")
	}
}

func TestBufferPool(t *testing.T) {
	bufPtr := AcquireBuffer()
	if bufPtr == nil {
		t.Fatal("AcquireBuffer returned nil")
	}
	if cap(*bufPtr) < 8192 {
		t.Errorf("Expected buffer capacity >= 8192, got %d", cap(*bufPtr))
	}

	// Use buffer
	*bufPtr = append(*bufPtr, []byte("test")...)

	ReleaseBuffer(bufPtr)

	// Verify reset (pointer still valid, but content length 0)
	bufPtr2 := AcquireBuffer()
	// NOTE: Pools don't guarantee same object, but if reused:
	if len(*bufPtr2) != 0 {
		t.Errorf("Acquired buffer should have length 0, got %d", len(*bufPtr2))
	}
	ReleaseBuffer(bufPtr2)
}
