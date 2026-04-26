package wal

import (
	"bytes"
	"crypto/rand"
	"io"
	"path/filepath"
	"testing"
)

// TestWAL_EntrySpansMultiplePages: o ganho de flexibilidade do formato
// page-based é que entries podem ser arbitrariamente grandes. Uma entry
// com payload de 20KB cruza 2-3 pages; reader must recompô-la sem
// problemas.
func TestWAL_EntrySpansMultiplePages(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "wal_span.log")

	w, err := NewWALWriter(tmpFile, Options{SyncPolicy: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}

	// Payload grande (~20KB): cruza múltiplas pages de 8KB
	bigPayload := make([]byte, 20000)
	if _, err := rand.Read(bigPayload); err != nil {
		t.Fatal(err)
	}

	entry := &WALEntry{
		Header: WALHeader{
			Magic:      WALMagic,
			Version:    WALVersion,
			EntryType:  EntryInsert,
			LSN:        1,
			PayloadLen: uint32(len(bigPayload)),
			CRC32:      CalculateCRC32(bigPayload),
		},
		Payload: bigPayload,
	}
	if err := w.WriteEntry(entry); err != nil {
		t.Fatalf("WriteEntry: %v", err)
	}

	// Também escreve uma entry pequena depois, pra verificar que o
	// posicionamento pós-cross-page é correto
	smallPayload := []byte("depois do gigante")
	small := &WALEntry{
		Header: WALHeader{
			Magic:      WALMagic,
			Version:    WALVersion,
			EntryType:  EntryUpdate,
			LSN:        2,
			PayloadLen: uint32(len(smallPayload)),
			CRC32:      CalculateCRC32(smallPayload),
		},
		Payload: smallPayload,
	}
	if err := w.WriteEntry(small); err != nil {
		t.Fatalf("WriteEntry 2: %v", err)
	}
	w.Close()

	// Lê de volta
	r, err := NewWALReader(tmpFile)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got1, err := r.ReadEntry()
	if err != nil {
		t.Fatalf("ReadEntry 1 (20KB): %v", err)
	}
	if !bytes.Equal(got1.Payload, bigPayload) {
		t.Fatal("payload 20KB divergente after cross-page roundtrip")
	}
	if got1.Header.LSN != 1 {
		t.Fatalf("LSN 1 expected, got %d", got1.Header.LSN)
	}

	got2, err := r.ReadEntry()
	if err != nil {
		t.Fatalf("ReadEntry 2: %v", err)
	}
	if string(got2.Payload) != string(smallPayload) {
		t.Fatalf("payload pequeno divergente: %q", got2.Payload)
	}

	// EOF
	if _, err := r.ReadEntry(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

// TestWAL_ManyEntriesFillManyPages: escreve 1000 entries pequenas,
// lê todas de volta em ordem. Valida que o page rotation funciona
// continuamente.
func TestWAL_ManyEntriesFillManyPages(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "wal_many.log")

	w, _ := NewWALWriter(tmpFile, Options{SyncPolicy: SyncEveryWrite})

	const N = 1000
	for i := 0; i < N; i++ {
		payload := []byte("entry-" + string(rune('a'+i%26)))
		entry := &WALEntry{
			Header: WALHeader{
				Magic:      WALMagic,
				Version:    WALVersion,
				EntryType:  EntryInsert,
				LSN:        uint64(i + 1),
				PayloadLen: uint32(len(payload)),
				CRC32:      CalculateCRC32(payload),
			},
			Payload: payload,
		}
		if err := w.WriteEntry(entry); err != nil {
			t.Fatalf("WriteEntry %d: %v", i, err)
		}
	}
	w.Close()

	r, _ := NewWALReader(tmpFile)
	defer r.Close()

	for i := 0; i < N; i++ {
		entry, err := r.ReadEntry()
		if err != nil {
			t.Fatalf("ReadEntry %d: %v", i, err)
		}
		if entry.Header.LSN != uint64(i+1) {
			t.Fatalf("entry %d: LSN expected %d, got %d", i, i+1, entry.Header.LSN)
		}
	}
	if _, err := r.ReadEntry(); err != io.EOF {
		t.Fatalf("expected EOF after %d entries, got %v", N, err)
	}
}

// TestWAL_ReopenAndAppend: escreve, fecha, reabre, continua appending.
// Prova que o writer adota corretamente a última page existsnte.
func TestWAL_ReopenAndAppend(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "wal_reopen.log")

	// Sessão 1
	w1, _ := NewWALWriter(tmpFile, Options{SyncPolicy: SyncEveryWrite})
	for i := 1; i <= 3; i++ {
		e := &WALEntry{
			Header: WALHeader{
				Magic: WALMagic, Version: WALVersion, EntryType: EntryInsert,
				LSN: uint64(i), PayloadLen: 5, CRC32: CalculateCRC32([]byte("aaaaa")),
			},
			Payload: []byte("aaaaa"),
		}
		w1.WriteEntry(e)
	}
	w1.Close()

	// Sessão 2: reabre e escreve mais
	w2, err := NewWALWriter(tmpFile, Options{SyncPolicy: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	for i := 4; i <= 6; i++ {
		e := &WALEntry{
			Header: WALHeader{
				Magic: WALMagic, Version: WALVersion, EntryType: EntryInsert,
				LSN: uint64(i), PayloadLen: 5, CRC32: CalculateCRC32([]byte("bbbbb")),
			},
			Payload: []byte("bbbbb"),
		}
		if err := w2.WriteEntry(e); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	w2.Close()

	// Lê tudo — must ter 6 entries em ordem 1..6
	r, _ := NewWALReader(tmpFile)
	defer r.Close()
	for i := 1; i <= 6; i++ {
		entry, err := r.ReadEntry()
		if err != nil {
			t.Fatalf("ReadEntry %d: %v", i, err)
		}
		if entry.Header.LSN != uint64(i) {
			t.Fatalf("entry %d: LSN expected %d, got %d", i, i, entry.Header.LSN)
		}
	}
}
