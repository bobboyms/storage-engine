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
	HeapVersion     = 1
	HeaderSize      = 14 // Magic(4) + Version(2) + NextOffset(8)
	EntryHeaderSize = 5  // Length(4) + Deleted(1)
)

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
		return fmt.Errorf("unsupported heap version: %d", version)
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
func (h *HeapManager) Write(doc []byte) (int64, error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	offset := h.nextOffset

	if _, err := h.file.Seek(offset, 0); err != nil {
		return 0, err
	}

	docLen := uint32(len(doc))

	// Write Length
	if err := binary.Write(h.file, binary.LittleEndian, docLen); err != nil {
		return 0, err
	}

	// Write Deleted flag (0 = active)
	if err := binary.Write(h.file, binary.LittleEndian, uint8(0)); err != nil {
		return 0, err
	}

	// Write Data
	if _, err := h.file.Write(doc); err != nil {
		return 0, err
	}

	// Update next offset
	h.nextOffset += int64(EntryHeaderSize + int(docLen))

	// Optionally update header on disk periodically or always
	// For safety, let's update. Ideally we'd batch or rely on recovery via scanning.
	// But let's keep header updated for simplicity.
	if err := h.updateNextOffset(); err != nil {
		return 0, err
	}

	return offset, nil
}

// Read retrieves a document from the given offset
func (h *HeapManager) Read(offset int64) ([]byte, error) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	if _, err := h.file.Seek(offset, 0); err != nil {
		return nil, err
	}

	var docLen uint32
	if err := binary.Read(h.file, binary.LittleEndian, &docLen); err != nil {
		return nil, err
	}

	var deleted uint8
	if err := binary.Read(h.file, binary.LittleEndian, &deleted); err != nil {
		return nil, err
	}

	if deleted == 1 {
		return nil, fmt.Errorf("document deleted")
	}

	doc := make([]byte, docLen)
	if _, err := io.ReadFull(h.file, doc); err != nil {
		return nil, err
	}

	return doc, nil
}

// Delete marks a document as deleted (lazy deletion)
func (h *HeapManager) Delete(offset int64) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	// Offset + 4 bytes (Length) points to Deleted flag
	flagOffset := offset + 4

	if _, err := h.file.Seek(flagOffset, 0); err != nil {
		return err
	}

	// Set deleted flag to 1
	return binary.Write(h.file, binary.LittleEndian, uint8(1))
}

func (h *HeapManager) Close() error {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.file.Close()
}
