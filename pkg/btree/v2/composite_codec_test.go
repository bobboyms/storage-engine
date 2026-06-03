package v2

import (
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestCompositeKeyCodec_Roundtrip(t *testing.T) {
	codec := CompositeKeyCodec{}
	secondaries := []types.Comparable{
		types.IntKey(-42),
		types.VarcharKey("shared@example.com"),
		types.BoolKey(true),
		types.FloatKey(3.5),
		types.DateKey(time.Unix(0, 1700000000123456789)),
	}
	for _, secondary := range secondaries {
		key := types.NewCompositeKey(secondary, types.IntKey(7))
		enc, err := codec.Encode(key)
		if err != nil {
			t.Fatalf("Encode(%T): %v", secondary, err)
		}
		decoded, ok := codec.Decode(enc).(types.CompositeKey)
		if !ok {
			t.Fatalf("Decode(%T) did not return CompositeKey", secondary)
		}
		if cmp, err := decoded.Compare(key); err != nil || cmp != 0 {
			t.Fatalf("roundtrip mismatch for %T: cmp=%d err=%v", secondary, cmp, err)
		}
	}
}

func TestCompositeKeyCodec_CompareMatchesLogicalOrder(t *testing.T) {
	codec := CompositeKeyCodec{}
	low := mustEncodeComposite(t, codec, types.NewCompositeKey(types.VarcharKey("a"), types.IntKey(1)))
	highPrimary := mustEncodeComposite(t, codec, types.NewCompositeKey(types.VarcharKey("a"), types.IntKey(2)))
	highSecondary := mustEncodeComposite(t, codec, types.NewCompositeKey(types.VarcharKey("b"), types.IntKey(1)))

	if got := codec.Compare(low, highPrimary); got >= 0 {
		t.Fatalf("same secondary, lower primary must sort first: got %d", got)
	}
	if got := codec.Compare(low, highSecondary); got >= 0 {
		t.Fatalf("lower secondary must sort first: got %d", got)
	}
	if got := codec.Compare(low, low); got != 0 {
		t.Fatalf("equal keys must compare equal: got %d", got)
	}
}

func TestCompositeKeyCodec_BoundsRoundtrip(t *testing.T) {
	codec := CompositeKeyCodec{}
	lower := types.CompositeLowerBound(types.VarcharKey("a"))
	enc, err := codec.Encode(lower)
	if err != nil {
		t.Fatalf("Encode bound: %v", err)
	}
	decoded, ok := codec.Decode(enc).(types.CompositeKey)
	if !ok {
		t.Fatalf("Decode bound did not return CompositeKey")
	}
	if decoded.PrimaryBound != types.CompositePrimaryMin {
		t.Fatalf("bound PrimaryBound = %d, want %d", decoded.PrimaryBound, types.CompositePrimaryMin)
	}
}

func TestCompositeKeyCodec_EncodeRejectsInvalid(t *testing.T) {
	codec := CompositeKeyCodec{}
	if _, err := codec.Encode(types.IntKey(1)); err == nil {
		t.Fatalf("Encode of non-composite key must error")
	}
	if _, err := codec.Encode(types.CompositeKey{PrimaryBound: types.CompositePrimaryKey}); err == nil {
		t.Fatalf("Encode with nil secondary must error")
	}
	if _, err := codec.Encode(types.CompositeKey{Secondary: types.VarcharKey("a"), PrimaryBound: types.CompositePrimaryKey}); err == nil {
		t.Fatalf("Encode of full key with nil primary must error")
	}
}

func TestCompositeKeyCodec_DecodeRejectsCorrupt(t *testing.T) {
	codec := CompositeKeyCodec{}
	if got := codec.Decode(nil); got != (types.CompositeKey{}) {
		t.Fatalf("Decode(nil) = %v, want zero CompositeKey", got)
	}
	if got := codec.Decode([]byte{0xFF}); got != (types.CompositeKey{}) {
		t.Fatalf("Decode of bad version = %v, want zero CompositeKey", got)
	}
	if got := codec.Decode([]byte{compositeCodecVersion, compositeTypeInt}); got != (types.CompositeKey{}) {
		t.Fatalf("Decode of truncated payload = %v, want zero CompositeKey", got)
	}
}

func mustEncodeComposite(t *testing.T, codec CompositeKeyCodec, key types.CompositeKey) []byte {
	t.Helper()
	enc, err := codec.Encode(key)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return enc
}
