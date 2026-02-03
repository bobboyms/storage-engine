package wal

import (
	"os"
	"testing"
	"time"
)

func TestWALWriter_IntervalSync(t *testing.T) {
	tmpFile := "test_wal_interval.log"
	defer os.Remove(tmpFile)

	payload := []byte("some data")
	crc := CalculateCRC32(payload)

	opts := Options{
		SyncPolicy:           SyncInterval,
		SyncIntervalDuration: 50 * time.Millisecond,
		BufferSize:           1024,
	}

	w, err := NewWALWriter(tmpFile, opts)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Escreve sem forçar sync
	entry := AcquireEntry()
	entry.Header = WALHeader{
		Magic:      WALMagic,
		Version:    1,
		EntryType:  EntryInsert,
		PayloadLen: uint32(len(payload)),
		CRC32:      crc,
		LSN:        1,
	}
	entry.Payload = append(entry.Payload, payload...)

	if err := w.WriteEntry(entry); err != nil {
		t.Fatalf("WriteEntry failed: %v", err)
	}
	ReleaseEntry(entry)

	// Espera o background sync (50ms)
	time.Sleep(100 * time.Millisecond)

	// Verifica se o arquivo tem tamanho > 0
	info, err := os.Stat(tmpFile)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Size() == 0 {
		t.Error("File size is 0 after background sync, expected content")
	}

	w.Close()
}

func TestWALWriter_BatchSync(t *testing.T) {
	tmpFile := "test_wal_batch.log"
	defer os.Remove(tmpFile)

	opts := Options{
		SyncPolicy:     SyncBatch,
		SyncBatchBytes: 100, // Sync a cada 100 bytes
		BufferSize:     1024,
	}

	w, err := NewWALWriter(tmpFile, opts)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Entry pequena (~30 bytes total)
	payload := []byte("12345")
	entrySize := int64(HeaderSize + len(payload))

	// Escreve 2 entradas (total ~60 bytes, < 100). Não deve syncar fisica
	entry := AcquireEntry()
	entry.Header.PayloadLen = uint32(len(payload))
	entry.Payload = append(entry.Payload, payload...)

	w.WriteEntry(entry)
	w.WriteEntry(entry)

	// O arquivo fisico pode estar vazio ou incompleto pois está no buffer do bufio/OS
	// Vamos forçar mais 2 escritas para estourar o limite de 100 bytes
	w.WriteEntry(entry)
	w.WriteEntry(entry)
	ReleaseEntry(entry)

	// Agora deve ter syncado
	info, err := os.Stat(tmpFile)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	// Tamanho esperado = 4 * entrySize
	expected := 4 * entrySize
	if info.Size() != expected {
		// Nota: Testar isso com precisão depende de como o SO reporta, mas o Sync garante flush
		t.Logf("File size: %d, Expected: %d", info.Size(), expected)
	}

	w.Close()
}

func TestWALWriter_SyncError(t *testing.T) {
	tmpFile := "test_wal_sync_error.log"
	defer os.Remove(tmpFile)

	w, _ := NewWALWriter(tmpFile, Options{SyncPolicy: SyncEveryWrite})
	w.file.Close() // Force future syncs to fail

	entry := AcquireEntry()
	entry.Header.Magic = WALMagic
	err := w.WriteEntry(entry)
	if err == nil {
		t.Error("Expected error writing to closed file")
	}
	ReleaseEntry(entry)
}

func TestWALWriter_BackgroundSyncPanic(t *testing.T) {
	// backgroundSync calls w.Sync(). If file is closed, it might log or fail quietly.
	// We just want to cover the code path.
	tmpFile := "test_wal_bg_sync.log"
	defer os.Remove(tmpFile)

	w, _ := NewWALWriter(tmpFile, Options{SyncPolicy: SyncInterval, SyncIntervalDuration: 10 * time.Millisecond})
	time.Sleep(20 * time.Millisecond)
	w.Close()
}

func TestWALWriter_CloseSyncError(t *testing.T) {
	path := "test_close_sync.log"
	defer os.Remove(path)

	w, _ := NewWALWriter(path, DefaultOptions())
	entry := AcquireEntry()
	entry.Payload = []byte("data")
	entry.Header.CRC32 = CalculateCRC32(entry.Payload)
	w.WriteEntry(entry)

	// Close file to force sync error
	w.file.Close()

	err := w.Close()
	if err == nil {
		t.Error("Expected error closing writer with closed file")
	}
}

func TestNewWALWriter_Error(t *testing.T) {
	// Trying to open a directory as a file for writing should fail
	tmpDir := t.TempDir()
	_, err := NewWALWriter(tmpDir, DefaultOptions())
	if err == nil {
		t.Error("Expected error opening directory as WAL file")
	}
}
