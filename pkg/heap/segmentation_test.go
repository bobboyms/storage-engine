package heap

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHeapManager_Rotation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "heap_rotation_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	basePath := filepath.Join(tmpDir, "test_heap")

	hm, err := NewHeapManager(basePath)
	if err != nil {
		t.Fatal(err)
	}

	// Force small segment size for testing
	// We use reflection or just access if in same package.
	// We are in 'package heap', so we can access private field.
	hm.maxSegmentSize = 100 // VERY small, rotation should happen quickly

	defer hm.Close()

	// 1. Write data smaller than limit
	doc1 := []byte("small doc 1") // ~11 bytes + 29 header = 40 bytes
	off1, err := hm.Write(doc1, 1, -1)
	if err != nil {
		t.Fatal(err)
	}

	if len(hm.segments) != 1 {
		t.Errorf("Expected 1 segment, got %d", len(hm.segments))
	}

	// 2. Write data to exceed limit
	doc2 := []byte("small doc 2") // +40 bytes -> 80 bytes. Still fits?
	// Next offset roughly 40 (header 14 + entry 40) is not right.
	// Initial offset = 14.
	// Write 1: 14 -> 54.
	// Write 2: 54 -> 94.
	// Write 3: 94 + 40 = 134 > 100. Rotate!

	off2, err := hm.Write(doc2, 2, -1)
	if err != nil {
		t.Fatal(err)
	}
	_ = off2 // Ignore for now, focused on rotation

	doc3 := []byte("small doc 3 causes rotation")
	off3, err := hm.Write(doc3, 3, -1)
	if err != nil {
		t.Fatal(err)
	}

	if len(hm.segments) != 2 {
		t.Errorf("Expected 2 segments after rotation, got %d", len(hm.segments))
	}

	// Verify files exist
	files, _ := filepath.Glob(basePath + "_*.data")
	if len(files) != 2 {
		t.Errorf("Expected 2 physical files, got %d: %v", len(files), files)
	}

	// Verify reading from both segments
	// Read doc1 from segment 1
	d1, _, err := hm.Read(off1)
	if err != nil {
		t.Error(err)
	}
	if string(d1) != string(doc1) {
		t.Errorf("Doc1 mismatch")
	}

	// Read doc3 from segment 2
	d3, _, err := hm.Read(off3)
	if err != nil {
		t.Error(err)
	}
	if string(d3) != string(doc3) {
		t.Errorf("Doc3 mismatch")
	}
}

func TestHeapManager_Rotation_Recovery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "heap_rotation_rec_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	basePath := filepath.Join(tmpDir, "test_heap")

	hm, err := NewHeapManager(basePath)
	if err != nil {
		t.Fatal(err)
	}
	hm.maxSegmentSize = 60 // Very small

	// Write enough to create multiple segments
	// Header=14. EntryHeader=29. Total overhead=43.
	// Write 1: "A" (1 byte). Size = 43+1 = 44. Total = 14+44=58.
	// Write 2: "B" (1 byte). Size = 44. Total = 58+44=102 > 60. Rotate.

	id1, _ := hm.Write([]byte("A"), 1, -1)
	id2, _ := hm.Write([]byte("B"), 2, -1)
	id3, _ := hm.Write([]byte("C"), 3, -1) // Segment 2 or 3?
	// Seg 1 [0-58)
	// Rotate to Seg 2. StartOffset=58. Header=14. Local=14.
	// Write 2: 58 + 44 = 102.
	// Seg 2: [58 - 102). Size 44.
	// Write 3: Size 44. 14+44 = 58 < 60?
	// Seg2 writes "C". 102 + 44 = 146.

	if len(hm.segments) < 2 {
		t.Errorf("Expected at least 2 segments, got %d", len(hm.segments))
	}

	hm.Close()

	// Reopen
	hm2, err := NewHeapManager(basePath)
	if err != nil {
		t.Fatal(err)
	}
	defer hm2.Close()

	if len(hm2.segments) != len(hm.segments) {
		t.Errorf("Expected %d segments after recovery, got %d", len(hm.segments), len(hm2.segments))
	}

	// Read all
	d1, _, err := hm2.Read(id1)
	if string(d1) != "A" {
		t.Error("Failed to read A")
	}
	d2, _, err := hm2.Read(id2)
	if string(d2) != "B" {
		t.Error("Failed to read B")
	}
	d3, _, err := hm2.Read(id3)
	if string(d3) != "C" {
		t.Error("Failed to read C")
	}

	// Write new data
	_, err = hm2.Write([]byte("D"), 4, -1)
	if err != nil {
		t.Fatal(err)
	}
}
