package wal

import (
	"encoding/binary"
	"io"
	"os"
	"testing"
)

func TestWALReader_ReadSeconds(t *testing.T) {
	tmpFile := "test_wal_read_seconds.log"
	defer os.Remove(tmpFile)

	// 1. Criar dados válidos
	opts := Options{SyncPolicy: SyncEveryWrite, BufferSize: 1024}
	w, _ := NewWALWriter(tmpFile, opts)

	payload1 := []byte("first entry")
	payload2 := []byte("second entry")

	e1 := AcquireEntry()
	e1.Header.Magic = WALMagic
	e1.Header.Version = 1
	e1.Header.EntryType = EntryInsert
	e1.Header.LSN = 100
	e1.Header.PayloadLen = uint32(len(payload1))
	e1.Header.CRC32 = CalculateCRC32(payload1)
	e1.Payload = append(e1.Payload, payload1...)
	w.WriteEntry(e1)

	e2 := AcquireEntry()
	e2.Header.Magic = WALMagic
	e2.Header.Version = 1
	e2.Header.EntryType = EntryUpdate
	e2.Header.LSN = 101
	e2.Header.PayloadLen = uint32(len(payload2))
	e2.Header.CRC32 = CalculateCRC32(payload2)
	e2.Payload = append(e2.Payload, payload2...)
	w.WriteEntry(e2)
	w.Close()

	// 2. Ler de volta
	r, err := NewWALReader(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	defer r.Close()

	// Li e1
	read1, err := r.ReadEntry()
	if err != nil {
		t.Fatalf("ReadEntry 1 failed: %v", err)
	}
	if string(read1.Payload) != string(payload1) {
		t.Errorf("Payload mismatch. Got %s, want %s", read1.Payload, payload1)
	}
	ReleaseEntry(read1)

	// Li e2
	read2, err := r.ReadEntry()
	if err != nil {
		t.Fatalf("ReadEntry 2 failed: %v", err)
	}
	if read2.Header.LSN != 101 {
		t.Errorf("LSN mismatch. Got %d, want 101", read2.Header.LSN)
	}
	ReleaseEntry(read2)

	// EOF
	_, err = r.ReadEntry()
	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}
}

func TestWALReader_Corruption(t *testing.T) {
	tmpFile := "test_wal_corruption.log"
	defer os.Remove(tmpFile)

	// 1. Escrever
	opts := Options{SyncPolicy: SyncEveryWrite, BufferSize: 1024}
	w, _ := NewWALWriter(tmpFile, opts)
	payload := []byte("critical data")
	e := AcquireEntry()
	e.Header.Magic = WALMagic
	e.Header.Version = 1
	e.Header.PayloadLen = uint32(len(payload))
	e.Header.CRC32 = CalculateCRC32(payload)
	e.Payload = append(e.Payload, payload...)
	w.WriteEntry(e)
	w.Close()

	// 2. Corromper 1 byte do arquivo (no payload)
	f, _ := os.OpenFile(tmpFile, os.O_RDWR, 0644)
	f.Seek(int64(HeaderSize+2), 0) // Pula header + 2 bytes
	f.Write([]byte{0xFF})          // Inverte bits
	f.Close()

	// 3. Tentar ler
	r, _ := NewWALReader(tmpFile)
	defer r.Close()

	_, err := r.ReadEntry()
	if err != ErrChecksumMismatch {
		t.Errorf("Expected ErrChecksumMismatch, got %v", err)
	}
}

func TestWALReader_TruncatedPayload(t *testing.T) {
	tmpFile := "test_wal_truncated.log"
	defer os.Remove(tmpFile)

	opts := Options{SyncPolicy: SyncEveryWrite}
	w, _ := NewWALWriter(tmpFile, opts)
	payload := []byte("loooooong data")
	e := AcquireEntry()
	e.Header.Magic = WALMagic
	e.Header.Version = 1
	e.Header.PayloadLen = uint32(len(payload))
	e.Header.CRC32 = CalculateCRC32(payload)
	e.Payload = append(e.Payload, payload...)
	w.WriteEntry(e)
	w.Close()

	// Truncar arquivo removendo últimos bytes
	os.Truncate(tmpFile, int64(HeaderSize+5)) // Deixa só 5 bytes do payload

	r, _ := NewWALReader(tmpFile)
	defer r.Close()

	_, err := r.ReadEntry()
	if err != io.ErrUnexpectedEOF {
		t.Errorf("Expected ErrUnexpectedEOF, got %v", err)
	}
}

func TestWALReader_InvalidMagic(t *testing.T) {
	tmpFile := "test_wal_magic.log"
	defer os.Remove(tmpFile)

	f, _ := os.Create(tmpFile)
	// Escreve header com Magic invalido
	invalidHeader := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint32(invalidHeader[0:4], 0xCAFEBABE)
	f.Write(invalidHeader)
	f.Close()

	r, _ := NewWALReader(tmpFile)
	defer r.Close()

	_, err := r.ReadEntry()
	if err != ErrInvalidMagic {
		t.Errorf("Expected ErrInvalidMagic, got %v", err)
	}
}
