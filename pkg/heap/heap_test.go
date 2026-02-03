package heap

import (
	"os"
	"testing"
)

func TestNewHeapManager_NewFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "heap_test_*.bin")
	if err != nil {
		t.Fatal(err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close() // Close immediately, let Manager open it
	defer os.Remove(tmpPath)

	hm, err := NewHeapManager(tmpPath)
	if err != nil {
		t.Fatalf("Failed to create heap manager: %v", err)
	}
	defer hm.Close()

	if hm.filename != tmpPath {
		t.Errorf("Expected filename %s, got %s", tmpPath, hm.filename)
	}
	if hm.nextOffset != int64(HeaderSize) {
		t.Errorf("Expected nextOffset %d, got %d", HeaderSize, hm.nextOffset)
	}
}

func TestNewHeapManager_ExistingFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "heap_test_*.bin")
	if err != nil {
		t.Fatal(err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	// Open and initialize
	hm1, err := NewHeapManager(tmpPath)
	if err != nil {
		t.Fatalf("Failed to create heap manager 1: %v", err)
	}

	// Write some data to advance offset
	data := []byte("test data")
	_, err = hm1.Write(data, 100, -1)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	expectedNextOffset := hm1.nextOffset
	hm1.Close()

	// Reopen
	hm2, err := NewHeapManager(tmpPath)
	if err != nil {
		t.Fatalf("Failed to create heap manager 2: %v", err)
	}
	defer hm2.Close()

	if hm2.nextOffset != expectedNextOffset {
		t.Errorf("Expected restored nextOffset %d, got %d", expectedNextOffset, hm2.nextOffset)
	}
}

func TestHeapManager_WriteRead(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "heap_test_*.bin")
	if err != nil {
		t.Fatal(err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	hm, err := NewHeapManager(tmpPath)
	if err != nil {
		t.Fatal(err)
	}
	defer hm.Close()

	docs := []struct {
		content    string
		createLSN  uint64
		prevOffset int64
	}{
		{"doc1", 10, -1},
		{"doc2", 11, 123},
		{"longer document content", 12, 456},
	}

	offsets := make([]int64, len(docs))

	for i, d := range docs {
		offset, err := hm.Write([]byte(d.content), d.createLSN, d.prevOffset)
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
		offsets[i] = offset
	}

	for i, d := range docs {
		data, header, err := hm.Read(offsets[i])
		if err != nil {
			t.Fatalf("Read %d failed: %v", i, err)
		}

		if string(data) != d.content {
			t.Errorf("Doc %d content mismatch: expected %s, got %s", i, d.content, string(data))
		}
		if header.CreateLSN != d.createLSN {
			t.Errorf("Doc %d CreateLSN mismatch: expected %d, got %d", i, d.createLSN, header.CreateLSN)
		}
		if header.PrevOffset != d.prevOffset {
			t.Errorf("Doc %d PrevOffset mismatch: expected %d, got %d", i, d.prevOffset, header.PrevOffset)
		}
		if !header.Valid {
			t.Errorf("Doc %d expected Valid=true", i)
		}
	}
}

func TestHeapManager_Delete(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "heap_test_*.bin")
	if err != nil {
		t.Fatal(err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	hm, err := NewHeapManager(tmpPath)
	if err != nil {
		t.Fatal(err)
	}
	defer hm.Close()

	offset, err := hm.Write([]byte("to be deleted"), 50, -1)
	if err != nil {
		t.Fatal(err)
	}

	deleteLSN := uint64(55)
	if err := hm.Delete(offset, deleteLSN); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, header, err := hm.Read(offset)
	if err != nil {
		t.Fatal(err)
	}

	if header.Valid {
		t.Error("Expected Valid=false after delete")
	}
	if header.DeleteLSN != deleteLSN {
		t.Errorf("Expected DeleteLSN %d, got %d", deleteLSN, header.DeleteLSN)
	}
}

func TestHeapManager_Close(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "heap_test_*.bin")
	if err != nil {
		t.Fatal(err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	hm, err := NewHeapManager(tmpPath)
	if err != nil {
		t.Fatal(err)
	}

	if err := hm.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Double close should fail or be handled, but depending on os.File implementation.
	// We just ensure first close works.
}
