package wal

import (
	"io"
	"os"
	"testing"
)

// Testes de leitura do WAL no novo backend page-based. Tests que
// manipulavam bytes raw (TestWALReader_InvalidMagic, _TruncatedPayload,
// etc) foram removidos — magic/truncation agora são checados na camada
// do pagestore (checksum de página + magic de página), não no WAL
// entry level. Tests equivalentes existem em pkg/pagestore.

func TestWALReader_ReadsMultipleEntriesInOrder(t *testing.T) {
	tmpFile := t.TempDir() + "/wal_read.log"

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

	// Lê de volta
	r, err := NewWALReader(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	defer r.Close()

	read1, err := r.ReadEntry()
	if err != nil {
		t.Fatalf("ReadEntry 1 failed: %v", err)
	}
	if string(read1.Payload) != string(payload1) {
		t.Errorf("Payload mismatch. Got %s, want %s", read1.Payload, payload1)
	}
	ReleaseEntry(read1)

	read2, err := r.ReadEntry()
	if err != nil {
		t.Fatalf("ReadEntry 2 failed: %v", err)
	}
	if read2.Header.LSN != 101 {
		t.Errorf("LSN mismatch. Got %d, want 101", read2.Header.LSN)
	}
	ReleaseEntry(read2)

	_, err = r.ReadEntry()
	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}
}

// TestWALReader_PageCorruption_PropagatesError: corrupção no body da
// página (bit flip) é detectada pelo checksum CRC32 do pagestore e
// propagada como ErrChecksumMismatch. Importante pra alerting em
// produção — NUNCA silenciar corrupção como EOF.
func TestWALReader_PageCorruption_PropagatesError(t *testing.T) {
	tmpFile := t.TempDir() + "/wal_corrupt.log"

	w, _ := NewWALWriter(tmpFile, Options{SyncPolicy: SyncEveryWrite})
	payload := []byte("critical data")
	e := AcquireEntry()
	e.Header.Magic = WALMagic
	e.Header.Version = 1
	e.Header.LSN = 1
	e.Header.PayloadLen = uint32(len(payload))
	e.Header.CRC32 = CalculateCRC32(payload)
	e.Payload = append(e.Payload, payload...)
	w.WriteEntry(e)
	w.Close()

	// Corrompe byte dentro do body da página 1 (pageID 0 é reservado)
	f, _ := os.OpenFile(tmpFile, os.O_RDWR, 0644)
	f.WriteAt([]byte{0xFF}, 8192+200)
	f.Close()

	r, _ := NewWALReader(tmpFile)
	defer r.Close()

	_, err := r.ReadEntry()
	if err != ErrChecksumMismatch {
		t.Errorf("Esperava ErrChecksumMismatch, recebi: %v", err)
	}
}

func TestNewWALReader_NonExistent(t *testing.T) {
	_, err := NewWALReader("/path/that/does/not/exist/wal.log")
	if err == nil {
		t.Error("Esperava erro para arquivo inexistente")
	}
}

// TestWALReader_EmptyWAL: WAL recém-criado sem entries → ReadEntry devolve EOF.
func TestWALReader_EmptyWAL(t *testing.T) {
	tmpFile := t.TempDir() + "/wal_empty.log"

	w, _ := NewWALWriter(tmpFile, Options{SyncPolicy: SyncEveryWrite})
	w.Close()

	r, err := NewWALReader(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	defer r.Close()

	_, err = r.ReadEntry()
	if err != io.EOF {
		t.Errorf("Esperava EOF em WAL vazio, recebi %v", err)
	}
}
