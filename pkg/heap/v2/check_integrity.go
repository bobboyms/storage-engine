package v2

import (
	"context"
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

// IntegrityIssue is one violation found by CheckIntegrity. Slot is -1 for
// page-level issues.
type IntegrityIssue struct {
	PageID pagestore.PageID
	Slot   int
	Detail string
}

func (i IntegrityIssue) String() string {
	if i.Slot < 0 {
		return fmt.Sprintf("page %d: %s", i.PageID, i.Detail)
	}
	return fmt.Sprintf("page %d slot %d: %s", i.PageID, i.Slot, i.Detail)
}

// CheckIntegrity is the read-only scrub pass over every page of the heap. It
// validates the slotted-page structure, record headers, version-chain
// pointers, and LSN sanity, returning one issue per violation instead of
// stopping at the first. It never mutates the heap; an unreadable page is
// reported as an issue, not an error.
//
// The heap must be quiescent (no concurrent writers): pages are inspected one
// at a time under the read latch, so a concurrent multi-page mutation could
// be observed half-applied and reported as a false positive.
func (h *HeapV2) CheckIntegrity(ctx context.Context) ([]IntegrityIssue, error) {
	// Flush so pages created via NewPage but never written are visible to
	// NumPages, mirroring the Vacuum traversal contract.
	if err := h.bp.FlushAll(); err != nil {
		return nil, err
	}

	var issues []IntegrityIssue
	numPages := h.pf.NumPages()
	// numSlots per page, collected on the first pass so PrevRecordID targets
	// can be bounds-checked on the second.
	slotsPerPage := make(map[pagestore.PageID]uint16, numPages)
	type prevRef struct {
		page pagestore.PageID
		slot int
		prev int64
	}
	var prevRefs []prevRef

	for pageID := pagestore.PageID(1); uint64(pageID) < numPages; pageID++ {
		if pageID%64 == 0 {
			if err := ctx.Err(); err != nil {
				return issues, err
			}
		}
		handle, err := h.bp.Fetch(pageID)
		if err != nil {
			issues = append(issues, IntegrityIssue{PageID: pageID, Slot: -1, Detail: fmt.Sprintf("page unreadable: %v", err)})
			continue
		}

		if hdr, err := handle.Page().GetHeader(); err != nil {
			issues = append(issues, IntegrityIssue{PageID: pageID, Slot: -1, Detail: fmt.Sprintf("page header undecodable: %v", err)})
		} else if pagestore.IsPoisonedPageLSN(hdr.PageLSN) {
			issues = append(issues, IntegrityIssue{PageID: pageID, Slot: -1, Detail: "poisoned PageLSN (MaxUint64 sentinel)"})
		}

		sp := OpenSlottedPage(handle.Page(), h.maxBodySize)
		pageIssues, refs := checkSlottedPage(sp, h.maxBodySize)
		for _, issue := range pageIssues {
			issue.PageID = pageID
			issues = append(issues, issue)
		}
		for slot, prev := range refs {
			prevRefs = append(prevRefs, prevRef{page: pageID, slot: slot, prev: prev})
		}
		slotsPerPage[pageID] = sp.header().numSlots
		handle.Release()
	}

	for _, ref := range prevRefs {
		prevPage, prevSlot := DecodeRecordID(ref.prev)
		numSlots, ok := slotsPerPage[prevPage]
		if !ok || prevSlot >= numSlots {
			issues = append(issues, IntegrityIssue{
				PageID: ref.page,
				Slot:   ref.slot,
				Detail: fmt.Sprintf("PrevRecordID %d points at nonexistent page %d slot %d", ref.prev, prevPage, prevSlot),
			})
		}
	}

	return issues, nil
}

// checkSlottedPage validates one page's slotted structure and record headers.
// It returns the issues (with PageID left zero for the caller to fill) and a
// map slot → PrevRecordID for every record that chains to a previous version.
func checkSlottedPage(sp *SlottedPage, maxBodySize int) ([]IntegrityIssue, map[int]int64) {
	var issues []IntegrityIssue
	prevs := make(map[int]int64)

	hdr := sp.header()
	expectedSlotDirEnd := uint16(SlottedHeaderSize) + hdr.numSlots*SlotSize //nolint:gosec // bounded by page layout
	if int(expectedSlotDirEnd) > maxBodySize || hdr.freeSpaceStart != expectedSlotDirEnd {
		issues = append(issues, IntegrityIssue{Slot: -1, Detail: fmt.Sprintf(
			"slotted header inconsistent: numSlots=%d freeSpaceStart=%d (want %d)",
			hdr.numSlots, hdr.freeSpaceStart, expectedSlotDirEnd)})
		return issues, prevs
	}
	if hdr.freeSpaceEnd < hdr.freeSpaceStart || int(hdr.freeSpaceEnd) > maxBodySize {
		issues = append(issues, IntegrityIssue{Slot: -1, Detail: fmt.Sprintf(
			"slotted header inconsistent: freeSpaceEnd=%d outside [%d, %d]",
			hdr.freeSpaceEnd, hdr.freeSpaceStart, maxBodySize)})
		return issues, prevs
	}

	validCount := uint16(0)
	for i := uint16(0); i < hdr.numSlots; i++ {
		offset, length := sp.readSlot(i)
		if length == 0 {
			continue // vacuumed slot: directory entry retained, record gone.
		}
		if length < RecordHeaderSize || offset < hdr.freeSpaceEnd || int(offset)+int(length) > maxBodySize {
			issues = append(issues, IntegrityIssue{Slot: int(i), Detail: fmt.Sprintf(
				"slot out of bounds: offset=%d length=%d (records live in [%d, %d])",
				offset, length, hdr.freeSpaceEnd, maxBodySize)})
			continue
		}

		var rh RecordHeader
		decodeRecordHeader(&rh, sp.body[offset:offset+RecordHeaderSize])
		if rh.Valid {
			validCount++
			if rh.DeleteLSN != 0 {
				issues = append(issues, IntegrityIssue{Slot: int(i), Detail: fmt.Sprintf(
					"live record carries DeleteLSN %d", rh.DeleteLSN)})
			}
		} else if rh.DeleteLSN == 0 {
			issues = append(issues, IntegrityIssue{Slot: int(i), Detail: "tombstone without DeleteLSN"})
		}
		if pagestore.IsPoisonedPageLSN(rh.CreateLSN) {
			issues = append(issues, IntegrityIssue{Slot: int(i), Detail: "poisoned CreateLSN (MaxUint64 sentinel)"})
		}
		if pagestore.IsPoisonedPageLSN(rh.DeleteLSN) {
			issues = append(issues, IntegrityIssue{Slot: int(i), Detail: "poisoned DeleteLSN (MaxUint64 sentinel)"})
		}
		if rh.PrevRecordID != NoRecordID {
			prevs[int(i)] = rh.PrevRecordID
		}
	}

	if validCount != hdr.numValid {
		issues = append(issues, IntegrityIssue{Slot: -1, Detail: fmt.Sprintf(
			"numValid=%d but %d live records found", hdr.numValid, validCount)})
	}
	return issues, prevs
}
