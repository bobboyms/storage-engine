package heap

import (
	"encoding/binary"
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
}

func TestNewHeapManager_InvalidPath(t *testing.T) {
	_, err := NewHeapManager("/invalid/path/to/heap.bin")
	if err == nil {
		t.Error("Expected error for invalid path")
	}
}

func TestNewHeapManager_InvalidMagic(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "heap_magic_*.bin")
	if err != nil {
		t.Fatal(err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	// Write invalid magic
	tmpFile.Write([]byte("BAD!"))
	tmpFile.Close()

	_, err = NewHeapManager(tmpPath)
	if err == nil {
		t.Error("Expected error for invalid magic")
	}
}

func TestNewHeapManager_InvalidVersion(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "heap_version_*.bin")
	if err != nil {
		t.Fatal(err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	// Write valid magic (4 bytes) + invalid version (2 bytes)
	// HeapMagic = 0x48454150 (Little Endian: 50 41 45 48)
	tmpFile.Write([]byte{0x50, 0x41, 0x45, 0x48}) // Magic
	tmpFile.Write([]byte{0x00, 0x00})             // Version 0
	tmpFile.Close()

	_, err = NewHeapManager(tmpPath)
	if err == nil {
		t.Error("Expected error for unsupported version")
	}
}

func TestHeapManager_WriteError(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "heap_write_err_*.bin")
	if err != nil {
		t.Fatal(err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	hm, _ := NewHeapManager(tmpPath)
	hm.Close() // Close to force error on next write

	_, err = hm.Write([]byte("data"), 1, -1)
	if err == nil {
		t.Error("Expected error writing to closed file")
	}
}

func TestHeapManager_ReadError(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "heap_read_err_*.bin")
	if err != nil {
		t.Fatal(err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	hm, _ := NewHeapManager(tmpPath)
	offset, _ := hm.Write([]byte("data"), 1, -1)
	hm.Close() // Close to force error

	_, _, err = hm.Read(offset)
	if err == nil {
		t.Error("Expected error reading from closed file")
	}
}

func TestHeapManager_DeleteError(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "heap_del_err_*.bin")
	if err != nil {
		t.Fatal(err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	hm, _ := NewHeapManager(tmpPath)
	offset, _ := hm.Write([]byte("data"), 1, -1)
	hm.Close() // Close to force error

	err = hm.Delete(offset, 2)
	if err == nil {
		t.Error("Expected error deleting in closed file")
	}
}

func TestHeapManager_RecoveryAfterCrash(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "heap_crash_*.bin")
	if err != nil {
		t.Fatal(err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	hm, _ := NewHeapManager(tmpPath)
	hm.Write([]byte("data1"), 1, -1)
	hm.Write([]byte("data2"), 2, -1)

	// Simulate "crash" where file grew but header wasn't updated
	// We do this by manually truncating the header nextOffset back, but keeping file size
	hm.file.Seek(6, 0)
	var oldOffset int64 = int64(HeaderSize)
	binary.Write(hm.file, binary.LittleEndian, oldOffset)
	hm.Close()

	// Reopen - should recover by using file size
	hm2, err := NewHeapManager(tmpPath)
	if err != nil {
		t.Fatal(err)
	}
	defer hm2.Close()

	info, _ := os.Stat(tmpPath)
	if hm2.nextOffset != info.Size() {
		t.Errorf("Expected nextOffset to be file size %d, got %d", info.Size(), hm2.nextOffset)
	}
}

func TestHeapManager_ReadHeaderPartial(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "heap_partial_*.bin")
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	// Write only 2 bytes of Magic (needs 4)
	tmpFile.Write([]byte{0x50, 0x41})
	tmpFile.Close()

	_, err := NewHeapManager(tmpPath)
	if err == nil {
		t.Error("Expected error for partial magic")
	}

	// Write Magic but partial version
	os.WriteFile(tmpPath, []byte{0x50, 0x41, 0x45, 0x48, 0x03}, 0666)
	_, err = NewHeapManager(tmpPath)
	if err == nil {
		t.Error("Expected error for partial version")
	}

	// Write Magic and Version but partial nextOffset
	os.WriteFile(tmpPath, []byte{0x50, 0x41, 0x45, 0x48, 0x03, 0x00, 0x01, 0x02}, 0666)
	_, err = NewHeapManager(tmpPath)
	if err == nil {
		t.Error("Expected error for partial nextOffset")
	}
}

func TestHeapManager_ReadPartial(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "heap_read_partial_*.bin")
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	hm, _ := NewHeapManager(tmpPath)
	data := []byte("some data")
	offset, _ := hm.Write(data, 1, -1)
	hm.Close()

	// Truncate file so it can't read the whole entry
	os.Truncate(tmpPath, offset+4) // Only enough for length

	hm2, _ := NewHeapManager(tmpPath)
	defer hm2.Close()

	_, _, err := hm2.Read(offset)
	if err == nil {
		t.Error("Expected error reading partial header")
	}

	// Truncate to partial doc length
	os.Truncate(tmpPath, offset+int64(EntryHeaderSize)+2)
	_, _, err = hm2.Read(offset)
	if err == nil {
		t.Error("Expected error reading partial data")
	}
}

func TestHeapManager_WriteInternalErrors(t *testing.T) {
	// To test internal errors in Write without closing the file immediately
	// is hard without mocking. But we covered Seek error and binary.Write via Close.
	// Let's add more scenarios if possible.
}

func TestHeapManager_WriteHeaderError(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "heap_hdr_err_*.bin")
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	hm, _ := NewHeapManager(tmpPath)
	hm.file.Close() // Force error

	err := hm.writeHeader()
	if err == nil {
		t.Error("Expected error writing header to closed file")
	}
}

func TestHeapManager_UpdateOffsetError(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "heap_off_err_*.bin")
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	hm, _ := NewHeapManager(tmpPath)
	hm.file.Close() // Force error

	err := hm.updateNextOffset()
	if err == nil {
		t.Error("Expected error updating offset in closed file")
	}
}

func TestHeapManager_WriteInternalFailure(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "heap_write_fail_*.bin")
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	hm, _ := NewHeapManager(tmpPath)
	
	// Force Seek error by passing invalid offset logic if possible, 
	// but here we just close it midway or before.
	hm.file.Close()
	_, err := hm.Write([]byte("data"), 1, -1)
	if err == nil {
		t.Error("Expected error in Write with closed file")
	}
}

func TestHeapManager_WriteReadOnlyError(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "heap_ro_*.bin")
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	hm, _ := NewHeapManager(tmpPath)
	hm.Write([]byte("initial"), 1, -1)
	
	// Close and reopen as read-only
	hm.Close()
	f, _ := os.OpenFile(tmpPath, os.O_RDONLY, 0444)
	hm.file = f // Manually swap
	
	_, err := hm.Write([]byte("data"), 2, -1)
	if err == nil {
		t.Error("Expected error writing to read-only file")
	}
}

func TestHeapManager_DeleteClosedError(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "heap_del_closed_*.bin")
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	hm, _ := NewHeapManager(tmpPath)
	hm.file.Close()

	err := hm.Delete(14, 100)
	if err == nil {
		t.Error("Expected error in Delete with closed file")
	}
}

func TestHeapManager_ReadClosedError(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "heap_read_closed_*.bin")
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	hm, _ := NewHeapManager(tmpPath)
	hm.file.Close()

	_, _, err := hm.Read(14)
	if err == nil {
		t.Error("Expected error in Read with closed file")
	}
}

func TestNewHeapManager_TooSmall(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "heap_small_*.bin")
	tmpPath := tmpFile.Name()
	os.WriteFile(tmpPath, []byte{1, 2}, 0644) // Only 2 bytes
	defer os.Remove(tmpPath)

	_, err := NewHeapManager(tmpPath)
	if err == nil {
		t.Error("Expected error for too small file")
	}
}

func TestNewHeapManager_InvalidMagicInternal(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "heap_magic_*.bin")
	tmpPath := tmpFile.Name()
	// Write wrong magic
	f, _ := os.OpenFile(tmpPath, os.O_WRONLY, 0644)
	binary.Write(f, binary.LittleEndian, uint32(0x12345678))
	f.Close()
	defer os.Remove(tmpPath)

	_, err := NewHeapManager(tmpPath)
	if err == nil {
		t.Error("Expected error for invalid magic")
	}
}

func TestHeapManager_WriteOffsetUpdateFail(t *testing.T) {
	tmpPath := "test_off_fail.bin"
	defer os.Remove(tmpPath)
	hm, _ := NewHeapManager(tmpPath)
	
	// We can't easily make JUST updateNextOffset fail while others succeed
	// But we can hit the failure of updateNextOffset inside Write.
	hm.file.Close()
	_, err := hm.Write([]byte("data"), 1, -1)
	if err == nil {
		t.Error("Expected error")
	}
}
