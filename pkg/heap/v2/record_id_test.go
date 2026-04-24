package v2

import (
	"testing"

	"github.com/bobboyms/storage-engine/pkg/pagestore"
)

func TestRecordID_Encode_Decode_Roundtrip(t *testing.T) {
	cases := []struct {
		pageID pagestore.PageID
		slotID uint16
	}{
		{1, 0},
		{1, 1},
		{42, 3},
		{1 << 10, 500},
		{MaxPageID, MaxSlotID},
	}
	for _, c := range cases {
		rid := EncodeRecordID(c.pageID, c.slotID)
		gotPage, gotSlot := DecodeRecordID(rid)
		if gotPage != c.pageID || gotSlot != c.slotID {
			t.Fatalf("roundtrip: in=(%d,%d) rid=%d out=(%d,%d)", c.pageID, c.slotID, rid, gotPage, gotSlot)
		}
		if rid <= 0 {
			t.Fatalf("RecordID válido deveria ser > 0, recebi %d", rid)
		}
	}
}

func TestRecordID_NoRecordIDSentinel(t *testing.T) {
	// O sentinela -1 nunca deve coincidir com algum encode válido.
	// (Encode produz valores >= 1<<16 = 65536, muito longe de -1.)
	if NoRecordID != -1 {
		t.Fatalf("NoRecordID deveria ser -1, é %d", NoRecordID)
	}
}

func TestRecordID_KnownValue_Sanity(t *testing.T) {
	// PageID=1, SlotID=0 → 1 << 16 = 65536
	if rid := EncodeRecordID(1, 0); rid != 65536 {
		t.Fatalf("EncodeRecordID(1,0) esperado 65536, recebi %d", rid)
	}
	// PageID=1, SlotID=1 → 65537
	if rid := EncodeRecordID(1, 1); rid != 65537 {
		t.Fatalf("EncodeRecordID(1,1) esperado 65537, recebi %d", rid)
	}
}
