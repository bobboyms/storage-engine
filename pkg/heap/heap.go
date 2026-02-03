package heap

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	HeapMagic       = 0x48454150 // ASCII for "HEAP"
	HeapVersion     = 3          // Bump to version 3 for MVCC Version Chains
	HeaderSize      = 14         // Magic(4) + Version(2) + NextOffset(8)
	EntryHeaderSize = 29         // Length(4) + Valid(1) + CreateLSN(8) + DeleteLSN(8) + PrevOffset(8)
)

type RecordHeader struct {
	Valid      bool
	CreateLSN  uint64
	DeleteLSN  uint64 // LSN of the deletion (if valid=false)
	PrevOffset int64  // Pointer to the previous version (-1 if start of chain)
}

// HeapManager gerencia o armazenamento de documentos em disco
type HeapManager struct {
	file       *os.File
	filename   string
	mutex      sync.RWMutex
	nextOffset int64
}

// NewHeapManager abre ou cria um novo arquivo de heap no caminho especificado
func NewHeapManager(path string) (*HeapManager, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open heap file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	hm := &HeapManager{
		file:     file,
		filename: path,
	}

	if info.Size() == 0 {
		// New file, write header
		if err := hm.writeHeader(); err != nil {
			file.Close()
			return nil, err
		}
		hm.nextOffset = int64(HeaderSize)
	} else {
		// Existing file, recover next offset from header or scan
		if err := hm.readHeader(); err != nil {
			file.Close()
			return nil, err
		}

		// Ensure file pointer is at end for appending if we ever need to
		// but since we track nextOffset, we can seek as needed.
		// Let's verify file integrity vs header nextOffset broadly if we wanted,
		// but for now trust the header or file size.
		if info.Size() > hm.nextOffset {
			// Maybe a crash happened before header update?
			// We can set nextOffset to file size to be safe (append only)
			hm.nextOffset = info.Size()
		}
	}

	return hm, nil
}

// writeHeader initializes the file header
func (h *HeapManager) writeHeader() error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if _, err := h.file.Seek(0, 0); err != nil {
		return err
	}

	if err := binary.Write(h.file, binary.LittleEndian, uint32(HeapMagic)); err != nil {
		return err
	}
	if err := binary.Write(h.file, binary.LittleEndian, uint16(HeapVersion)); err != nil {
		return err
	}
	// Initial nextOffset starts after header
	if err := binary.Write(h.file, binary.LittleEndian, int64(HeaderSize)); err != nil {
		return err
	}

	// Ensure persist
	return h.file.Sync()
}

// readHeader reads initial metadata
func (h *HeapManager) readHeader() error {
	if _, err := h.file.Seek(0, 0); err != nil {
		return err
	}

	var magic uint32
	if err := binary.Read(h.file, binary.LittleEndian, &magic); err != nil {
		return err
	}
	if magic != HeapMagic {
		return fmt.Errorf("invalid heap file magic")
	}

	var version uint16
	if err := binary.Read(h.file, binary.LittleEndian, &version); err != nil {
		return err
	}
	if version != HeapVersion {
		return fmt.Errorf("unsupported heap version: %d (expected %d)", version, HeapVersion)
	}

	var nextOffset int64
	if err := binary.Read(h.file, binary.LittleEndian, &nextOffset); err != nil {
		return err
	}
	h.nextOffset = nextOffset
	return nil
}

// updateNextOffset updates the offset in the header
func (h *HeapManager) updateNextOffset() error {
	// Must be called under lock
	pos, err := h.file.Seek(6, 0) // Skip Magic(4) + Version(2)
	if err != nil {
		return err
	}
	if pos != 6 {
		return fmt.Errorf("seek failed")
	}
	return binary.Write(h.file, binary.LittleEndian, h.nextOffset)
}

// Write appends a document to the heap and returns its offset
// Updated for MVCC Phase 2: accepts prevOffset for chaining
func (h *HeapManager) Write(doc []byte, createLSN uint64, prevOffset int64) (int64, error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	offset := h.nextOffset
	fmt.Printf("Heap.Write: Offset=%d, CreateLSN=%d, PrevOffset=%d, DocLen=%d\n", offset, createLSN, prevOffset, len(doc))

	if _, err := h.file.Seek(offset, 0); err != nil {
		return 0, err
	}
	// ... rest of write

	docLen := uint32(len(doc))

	// Write Length
	if err := binary.Write(h.file, binary.LittleEndian, docLen); err != nil {
		return 0, err
	}

	// Write Valid flag (1 = Active/Valid)
	if err := binary.Write(h.file, binary.LittleEndian, uint8(1)); err != nil {
		return 0, err
	}

	// Write CreateLSN
	if err := binary.Write(h.file, binary.LittleEndian, createLSN); err != nil {
		return 0, err
	}

	// Write DeleteLSN (0 initially)
	if err := binary.Write(h.file, binary.LittleEndian, uint64(0)); err != nil {
		return 0, err
	}

	// Write PrevOffset
	if err := binary.Write(h.file, binary.LittleEndian, prevOffset); err != nil {
		return 0, err
	}

	// Write Data
	if _, err := h.file.Write(doc); err != nil {
		return 0, err
	}

	// Update next offset
	h.nextOffset += int64(EntryHeaderSize + int(docLen))

	if err := h.updateNextOffset(); err != nil {
		return 0, err
	}

	return offset, nil
}

// Read retrieves a document from the given offset
// Updated for MVCC: returns RecordHeader
func (h *HeapManager) Read(offset int64) ([]byte, *RecordHeader, error) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	if _, err := h.file.Seek(offset, 0); err != nil {
		return nil, nil, err
	}

	var docLen uint32
	if err := binary.Read(h.file, binary.LittleEndian, &docLen); err != nil {
		return nil, nil, err
	}

	var valid uint8
	if err := binary.Read(h.file, binary.LittleEndian, &valid); err != nil {
		return nil, nil, err
	}

	var createLSN uint64
	if err := binary.Read(h.file, binary.LittleEndian, &createLSN); err != nil {
		return nil, nil, err
	}

	var deleteLSN uint64
	if err := binary.Read(h.file, binary.LittleEndian, &deleteLSN); err != nil {
		return nil, nil, err
	}

	var prevOffset int64
	if err := binary.Read(h.file, binary.LittleEndian, &prevOffset); err != nil {
		return nil, nil, err
	}

	header := &RecordHeader{
		Valid:      valid == 1,
		CreateLSN:  createLSN,
		DeleteLSN:  deleteLSN,
		PrevOffset: prevOffset,
	}
	// fmt.Printf("Heap.Read: Offset=%d, Valid=%v, Create=%d, Delete=%d, Prev=%d\n", offset, header.Valid, header.CreateLSN, header.DeleteLSN, header.PrevOffset)

	doc := make([]byte, docLen)
	if _, err := io.ReadFull(h.file, doc); err != nil {
		return nil, nil, err
	}

	return doc, header, nil
}

// Delete marks a document as deleted (lazy deletion)
// Updated using LSN: writes DeleteLSN.
// Note: This modifies the record in-place.
func (h *HeapManager) Delete(offset int64, deleteLSN uint64) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	fmt.Printf("Heap.Delete: Offset=%d, DeleteLSN=%d\n", offset, deleteLSN)

	// Offset + 4 (Length) -> Valid byte
	validOffset := offset + 4
	// Offset + 4 (Length) + 1 (Valid) + 8 (CreateLSN) -> DeleteLSN
	deleteLSNOffset := offset + 4 + 1 + 8

	// 1. Mark Invalid
	if _, err := h.file.Seek(validOffset, 0); err != nil {
		return err
	}
	if err := binary.Write(h.file, binary.LittleEndian, uint8(0)); err != nil {
		return err
	}

	// 2. Set DeleteLSN
	if _, err := h.file.Seek(deleteLSNOffset, 0); err != nil {
		return err
	}
	if err := binary.Write(h.file, binary.LittleEndian, deleteLSN); err != nil {
		return err
	}

	return nil
}

func (h *HeapManager) Close() error {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.file.Close()
}

// Path returns the path of the heap file
func (h *HeapManager) Path() string {
	return h.filename
}
