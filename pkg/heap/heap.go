package heap

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	HeapMagic             = 0x48454150       // ASCII for "HEAP"
	HeapVersion           = 3                // Bump to version 3 for MVCC Version Chains
	HeaderSize            = 14               // Magic(4) + Version(2) + NextOffset(8)
	EntryHeaderSize       = 29               // Length(4) + Valid(1) + CreateLSN(8) + DeleteLSN(8) + PrevOffset(8)
	DefaultMaxSegmentSize = 64 * 1024 * 1024 // 64MB
)

type RecordHeader struct {
	Valid      bool
	CreateLSN  uint64
	DeleteLSN  uint64 // LSN of the deletion (if valid=false)
	PrevOffset int64  // Pointer to the previous version (-1 if start of chain)
}

type Segment struct {
	ID          int
	Path        string
	StartOffset int64
	Size        int64
	File        *os.File
}

// HeapManager gerencia o armazenamento de documentos em disco, dividido em segmentos
type HeapManager struct {
	basePath       string
	segments       []*Segment
	activeSegment  *Segment
	nextOffset     int64 // Global next offset across all segments
	maxSegmentSize int64
	mutex          sync.RWMutex
}

// NewHeapManager abre ou cria um novo gerenciador de heap no caminho especificado
func NewHeapManager(path string) (*HeapManager, error) {
	// Ensure base directory exists
	// But 'path' is likely a file path prefix, e.g. "db/data/users" -> "db/data/users_001.data"
	// So we need to check the dir of the path.

	hm := &HeapManager{
		basePath:       path,
		segments:       make([]*Segment, 0),
		maxSegmentSize: DefaultMaxSegmentSize,
	}

	// Scan for existing segments
	// Assuming format: {path}_%03d.data
	// Simple approach: start from ID 1 and check existence

	var globalOffset int64 = 0
	id := 1

	for {
		segPath := fmt.Sprintf("%s_%03d.data", path, id)
		file, err := os.OpenFile(segPath, os.O_RDWR, 0666)
		if os.IsNotExist(err) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to open segment %s: %w", segPath, err)
		}

		info, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, err
		}

		size := info.Size()

		// If valid header exists, read it to verify/get info?
		// For simplicity, we just trust size for previous segments,
		// but we should verify the last segment strictly.

		seg := &Segment{
			ID:          id,
			Path:        segPath,
			StartOffset: globalOffset,
			Size:        size,
			File:        file,
		}
		hm.segments = append(hm.segments, seg)

		globalOffset += size
		id++
	}

	// If no segments found, create the first one
	if len(hm.segments) == 0 {
		return hm.createNewSegment(1, 0)
	}

	// Set active segment (last one)
	hm.activeSegment = hm.segments[len(hm.segments)-1]

	// Check integrity/header of active segment to properly set "nextOffset"
	// The globalOffset calculated above is basically the sum of sizes.
	// We need to read the `nextOffset` from the header of the *active* segment
	// to know where the actual write pointer is (it might be less than file size if pre-allocated,
	// though our implementation currently matches file size).
	// But wait, `nextOffset` in the file header is local to that file.
	// Our `hm.nextOffset` is global.

	err := hm.loadActiveSegmentState()
	if err != nil {
		return nil, err
	}

	return hm, nil
}

func (h *HeapManager) createNewSegment(id int, startOffset int64) (*HeapManager, error) {
	segPath := fmt.Sprintf("%s_%03d.data", h.basePath, id)
	file, err := os.OpenFile(segPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment %s: %w", segPath, err)
	}

	seg := &Segment{
		ID:          id,
		Path:        segPath,
		StartOffset: startOffset,
		Size:        0,
		File:        file,
	}

	h.segments = append(h.segments, seg)
	h.activeSegment = seg

	// Write Header
	if err := h.writeHeader(seg); err != nil {
		return nil, err
	}

	// The writeHeader sets the file size to HeaderSize
	seg.Size = int64(HeaderSize)
	h.nextOffset = startOffset + int64(HeaderSize)

	return h, nil
}

func (h *HeapManager) loadActiveSegmentState() error {
	// Read header of active semgent
	if _, err := h.activeSegment.File.Seek(0, 0); err != nil {
		return err
	}

	var magic uint32
	if err := binary.Read(h.activeSegment.File, binary.LittleEndian, &magic); err != nil {
		return err
	}
	if magic != HeapMagic {
		return fmt.Errorf("invalid heap file magic in segment %d", h.activeSegment.ID)
	}

	var version uint16
	if err := binary.Read(h.activeSegment.File, binary.LittleEndian, &version); err != nil {
		return err
	}
	if version != HeapVersion {
		// In a real app we might support upgrades, here we just error
		return fmt.Errorf("unsupported heap version: %d", version)
	}

	var localNextOffset int64
	if err := binary.Read(h.activeSegment.File, binary.LittleEndian, &localNextOffset); err != nil {
		return err
	}

	// Update global nextOffset
	h.nextOffset = h.activeSegment.StartOffset + localNextOffset

	// Verify file size just in case
	stat, _ := h.activeSegment.File.Stat()
	if stat.Size() > localNextOffset {
		// Recovery: File is larger than header says. Trust file size.
		// This happens if we wrote data but crashed before updating header.
		// Update global nextOffset based on file size.
		h.nextOffset = h.activeSegment.StartOffset + stat.Size()

		// Update header to match (to be clean for next time)
		// We can do this lazily or now. Let's do it now.
		if err := h.updateNextOffset(); err != nil {
			// Warn but don't fail?
		}
	}

	return nil
}

// writeHeader initializes the file header for a specific segment
func (h *HeapManager) writeHeader(seg *Segment) error {
	if _, err := seg.File.Seek(0, 0); err != nil {
		return err
	}

	if err := binary.Write(seg.File, binary.LittleEndian, uint32(HeapMagic)); err != nil {
		return err
	}
	if err := binary.Write(seg.File, binary.LittleEndian, uint16(HeapVersion)); err != nil {
		return err
	}
	// Initial nextOffset starts after header (local offset)
	if err := binary.Write(seg.File, binary.LittleEndian, int64(HeaderSize)); err != nil {
		return err
	}

	// Ensure persist
	return seg.File.Sync()
}

// readHeader reads initial metadata from the active segment (used during recovery)
// Note: This is now mostly used by loadActiveSegmentState, so we can deprecate or adapt.
// But let's keep it if needed, or simply remove if loadActiveSegmentState covers it.
// Given loadActiveSegmentState exists, we can remove readHeader or make it a helper for a segment.
func (h *HeapManager) readSegmentHeader(seg *Segment) (int64, error) {
	if _, err := seg.File.Seek(0, 0); err != nil {
		return 0, err
	}

	var magic uint32
	if err := binary.Read(seg.File, binary.LittleEndian, &magic); err != nil {
		return 0, err
	}
	if magic != HeapMagic {
		return 0, fmt.Errorf("invalid heap file magic")
	}

	var version uint16
	if err := binary.Read(seg.File, binary.LittleEndian, &version); err != nil {
		return 0, err
	}
	if version != HeapVersion {
		return 0, fmt.Errorf("unsupported heap version: %d (expected %d)", version, HeapVersion)
	}

	var nextOffset int64
	if err := binary.Read(seg.File, binary.LittleEndian, &nextOffset); err != nil {
		return 0, err
	}
	return nextOffset, nil
}

// updateNextOffset updates the offset in the header of the ACTIVE segment
func (h *HeapManager) updateNextOffset() error {
	// Must be called under lock
	seg := h.activeSegment
	pos, err := seg.File.Seek(6, 0) // Skip Magic(4) + Version(2)
	if err != nil {
		return err
	}
	if pos != 6 {
		return fmt.Errorf("seek failed")
	}

	// Convert global nextOffset to local offset
	localOffset := h.nextOffset - seg.StartOffset

	return binary.Write(seg.File, binary.LittleEndian, localOffset)
}

// Write appends a document to the heap and returns its offset
// Updated for MVCC Phase 2: accepts prevOffset for chaining
// Updated for Segmentation: Handles file rotation
func (h *HeapManager) Write(doc []byte, createLSN uint64, prevOffset int64) (int64, error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	// Calculate needed size: EntryHeader + DocLen
	neededSize := int64(EntryHeaderSize + len(doc))

	// Check if we need rotation
	// Local offset calculation
	currentLocalOffset := h.nextOffset - h.activeSegment.StartOffset

	if currentLocalOffset+neededSize > h.maxSegmentSize {
		// ROTATE
		// 1. Close current segment if we want to save file descriptors (optional, for now keep open)
		// 2. Create new segment
		newID := h.activeSegment.ID + 1
		// New segment starts at current global `h.nextOffset`
		// Because `h.nextOffset` points to where we WOULD write next.

		// Actually, before rotating, we should probably align or just start fresh.
		// The previous file might have some unused space at end, which is fine.
		// The `nextOffset` is the global pointer.

		// Create new segment
		if _, err := h.createNewSegment(newID, h.nextOffset); err != nil {
			return 0, fmt.Errorf("failed to rotate segment: %w", err)
		}

		// Reset local offset context for new segment
		currentLocalOffset = HeaderSize
		// h.nextOffset is already updated by createNewSegment to (StartOffset + HeaderSize)
	}

	offset := h.nextOffset // This is the GLOBAL offset we return

	// Write to active segment
	seg := h.activeSegment
	localOffset := offset - seg.StartOffset

	// fmt.Printf("Heap.Write: GlobalOffset=%d, SegID=%d, LocalOffset=%d, CreateLSN=%d\n", offset, seg.ID, localOffset, createLSN)

	if _, err := seg.File.Seek(localOffset, 0); err != nil {
		return 0, err
	}

	docLen := uint32(len(doc))

	// Write Length
	if err := binary.Write(seg.File, binary.LittleEndian, docLen); err != nil {
		return 0, err
	}

	// Write Valid flag (1 = Active/Valid)
	if err := binary.Write(seg.File, binary.LittleEndian, uint8(1)); err != nil {
		return 0, err
	}

	// Write CreateLSN
	if err := binary.Write(seg.File, binary.LittleEndian, createLSN); err != nil {
		return 0, err
	}

	// Write DeleteLSN (0 initially)
	if err := binary.Write(seg.File, binary.LittleEndian, uint64(0)); err != nil {
		return 0, err
	}

	// Write PrevOffset
	if err := binary.Write(seg.File, binary.LittleEndian, prevOffset); err != nil {
		return 0, err
	}

	// Write Data
	if _, err := seg.File.Write(doc); err != nil {
		return 0, err
	}

	// Update next offset
	h.nextOffset += int64(EntryHeaderSize + int(docLen))
	seg.Size = h.nextOffset - seg.StartOffset // Update segment size tracker

	if err := h.updateNextOffset(); err != nil {
		return 0, err
	}

	return offset, nil
}

// getSegmentForOffset finds the segment containing the given global offset
// We can optimize this using binary search if segments are many,
// for now linear scan is fast enough as segments are naturally ordered.
func (h *HeapManager) getSegmentForOffset(offset int64) (*Segment, error) {
	for _, seg := range h.segments {
		if offset >= seg.StartOffset && offset < (seg.StartOffset+seg.Size) {
			return seg, nil
		}
		// Special case: if offset is exactly at end of file (EOF), it might be valid if we are checking bounds,
		// but usually we Read() existing data.
	}
	// If reading the exact nextByte that hasn't been written yet?
	if offset < h.nextOffset {
		// It should be covered by segments unless there is a hole.
		// Fallback to active segment if it's within its range (even if size check fails tightly due to race?)
		if offset >= h.activeSegment.StartOffset {
			return h.activeSegment, nil
		}
	}

	return nil, fmt.Errorf("segment not found for offset %d", offset)
}

// Read retrieves a document from the given offset
// Updated for MVCC: returns RecordHeader
func (h *HeapManager) Read(offset int64) ([]byte, *RecordHeader, error) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	seg, err := h.getSegmentForOffset(offset)
	if err != nil {
		// Try to see if it belongs to active segment just vaguely?
		// No, stricter is better.
		return nil, nil, err
	}

	localOffset := offset - seg.StartOffset

	if _, err := seg.File.Seek(localOffset, 0); err != nil {
		return nil, nil, err
	}

	var docLen uint32
	if err := binary.Read(seg.File, binary.LittleEndian, &docLen); err != nil {
		return nil, nil, err
	}

	var valid uint8
	if err := binary.Read(seg.File, binary.LittleEndian, &valid); err != nil {
		return nil, nil, err
	}

	var createLSN uint64
	if err := binary.Read(seg.File, binary.LittleEndian, &createLSN); err != nil {
		return nil, nil, err
	}

	var deleteLSN uint64
	if err := binary.Read(seg.File, binary.LittleEndian, &deleteLSN); err != nil {
		return nil, nil, err
	}

	var prevOffset int64
	if err := binary.Read(seg.File, binary.LittleEndian, &prevOffset); err != nil {
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
	if _, err := io.ReadFull(seg.File, doc); err != nil {
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

	// fmt.Printf("Heap.Delete: Offset=%d, DeleteLSN=%d\n", offset, deleteLSN)

	seg, err := h.getSegmentForOffset(offset)
	if err != nil {
		return err
	}

	localOffset := offset - seg.StartOffset
	// Offset + 4 (Length) -> Valid byte
	validOffset := localOffset + 4
	// Offset + 4 (Length) + 1 (Valid) + 8 (CreateLSN) -> DeleteLSN
	deleteLSNOffset := localOffset + 4 + 1 + 8

	// 1. Mark Invalid
	if _, err := seg.File.Seek(validOffset, 0); err != nil {
		return err
	}
	if err := binary.Write(seg.File, binary.LittleEndian, uint8(0)); err != nil {
		return err
	}

	// 2. Set DeleteLSN
	if _, err := seg.File.Seek(deleteLSNOffset, 0); err != nil {
		return err
	}
	if err := binary.Write(seg.File, binary.LittleEndian, deleteLSN); err != nil {
		return err
	}

	return nil
}

func (h *HeapManager) Close() error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	var firstErr error
	for _, seg := range h.segments {
		if seg.File != nil {
			if err := seg.File.Close(); err != nil {
				if firstErr == nil {
					firstErr = err
				}
			}
		}
	}
	return firstErr
}

// Path returns the base path of the heap
func (h *HeapManager) Path() string {
	return h.basePath
}

// ByteIterator iterates over all records in the heap
type HeapIterator struct {
	hm          *HeapManager
	cancel      chan struct{}
	segmentIdx  int
	currentFile *os.File
	currentPos  int64 // Local offset in current file
}

func (h *HeapManager) NewIterator() (*HeapIterator, error) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	if len(h.segments) == 0 {
		return nil, fmt.Errorf("no segments to iterate")
	}

	// Start with first segment
	seg := h.segments[0]
	f, err := os.Open(seg.Path) // Open independent handle for iteration
	if err != nil {
		return nil, err
	}

	return &HeapIterator{
		hm:          h,
		segmentIdx:  0,
		currentFile: f,
		currentPos:  HeaderSize, // Skip header
	}, nil
}

// Next returns the next record's doc, header, and GLOBAL offset.
// Returns io.EOF when done.
func (it *HeapIterator) Next() ([]byte, *RecordHeader, int64, error) {
	for {
		// Calculate GLOBAL offset of current position
		// We need to access segment info safely.
		it.hm.mutex.RLock()
		if it.segmentIdx >= len(it.hm.segments) {
			it.hm.mutex.RUnlock()
			return nil, nil, 0, io.EOF
		}
		seg := it.hm.segments[it.segmentIdx]
		startOffset := seg.StartOffset
		it.hm.mutex.RUnlock()

		globalOffset := startOffset + it.currentPos

		// Seek to position
		if _, err := it.currentFile.Seek(it.currentPos, 0); err != nil {
			return nil, nil, 0, err
		}

		// Read Entry Header
		// Length(4), Valid(1), Create(8), Delete(8), Prev(8) = 29 bytes
		headerBuf := make([]byte, 29)
		if _, err := io.ReadFull(it.currentFile, headerBuf); err != nil {
			if err == io.EOF {
				// End of this segment, move to next
				if err := it.nextSegment(); err != nil {
					return nil, nil, 0, err // Could be real EOF
				}
				continue
			}
			return nil, nil, 0, err
		}

		docLen := binary.LittleEndian.Uint32(headerBuf[0:4])
		valid := headerBuf[4]
		createLSN := binary.LittleEndian.Uint64(headerBuf[5:13])
		deleteLSN := binary.LittleEndian.Uint64(headerBuf[13:21])
		prevOffset := int64(binary.LittleEndian.Uint64(headerBuf[21:29]))

		// Read Doc
		doc := make([]byte, docLen)
		if _, err := io.ReadFull(it.currentFile, doc); err != nil {
			return nil, nil, 0, err
		}

		// Update position for next call
		it.currentPos += int64(29 + docLen)

		// Construct header
		header := &RecordHeader{
			Valid:      valid == 1,
			CreateLSN:  createLSN,
			DeleteLSN:  deleteLSN,
			PrevOffset: prevOffset,
		}

		return doc, header, globalOffset, nil
	}
}

func (it *HeapIterator) nextSegment() error {
	it.currentFile.Close()
	it.segmentIdx++

	it.hm.mutex.RLock()
	defer it.hm.mutex.RUnlock()

	if it.segmentIdx >= len(it.hm.segments) {
		return io.EOF
	}

	seg := it.hm.segments[it.segmentIdx]
	f, err := os.Open(seg.Path)
	if err != nil {
		return err
	}
	it.currentFile = f
	it.currentPos = HeaderSize
	return nil
}

func (it *HeapIterator) Close() {
	if it.currentFile != nil {
		it.currentFile.Close()
	}
}
