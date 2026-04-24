package v2

import (
	"math"
	"testing"
	"time"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestCodec_Int_Roundtrip(t *testing.T) {
	c := IntKeyCodec{}
	for _, v := range []int64{0, 1, -1, 42, -42, math.MaxInt64, math.MinInt64} {
		enc := c.Encode(types.IntKey(v))
		dec := c.Decode(enc).(types.IntKey)
		if int64(dec) != v {
			t.Errorf("Int roundtrip falhou: in=%d out=%d", v, int64(dec))
		}
	}
}

func TestCodec_Int_CompareOrdering(t *testing.T) {
	c := IntKeyCodec{}
	neg := c.Encode(types.IntKey(-100))
	zero := c.Encode(types.IntKey(0))
	pos := c.Encode(types.IntKey(100))

	if c.Compare(neg, zero) != -1 {
		t.Error("Int: -100 deveria ser < 0")
	}
	if c.Compare(zero, pos) != -1 {
		t.Error("Int: 0 deveria ser < 100")
	}
	if c.Compare(neg, pos) != -1 {
		t.Error("Int: -100 deveria ser < 100 (crítico — comparação uint64 direta falharia)")
	}
	if c.Compare(pos, neg) != 1 {
		t.Error("Int: 100 deveria ser > -100")
	}
	if c.Compare(zero, zero) != 0 {
		t.Error("Int: 0 == 0")
	}
}

func TestCodec_Float_Roundtrip(t *testing.T) {
	c := FloatKeyCodec{}
	for _, v := range []float64{0.0, 1.5, -1.5, 3.14, -3.14, math.MaxFloat64, -math.MaxFloat64} {
		enc := c.Encode(types.FloatKey(v))
		dec := c.Decode(enc).(types.FloatKey)
		if float64(dec) != v {
			t.Errorf("Float roundtrip: in=%v out=%v", v, float64(dec))
		}
	}
}

func TestCodec_Float_CompareOrdering(t *testing.T) {
	c := FloatKeyCodec{}
	neg := c.Encode(types.FloatKey(-1.5))
	zero := c.Encode(types.FloatKey(0.0))
	pos := c.Encode(types.FloatKey(1.5))

	if c.Compare(neg, pos) != -1 {
		t.Error("Float: -1.5 < 1.5 (crítico — bits IEEE754 uint64 diretos falhariam)")
	}
	if c.Compare(zero, zero) != 0 {
		t.Error("Float: 0 == 0")
	}
}

func TestCodec_Bool_Roundtrip(t *testing.T) {
	c := BoolKeyCodec{}
	for _, v := range []bool{true, false} {
		enc := c.Encode(types.BoolKey(v))
		dec := c.Decode(enc).(types.BoolKey)
		if bool(dec) != v {
			t.Errorf("Bool roundtrip: in=%v out=%v", v, bool(dec))
		}
	}
}

func TestCodec_Bool_CompareOrdering(t *testing.T) {
	c := BoolKeyCodec{}
	f := c.Encode(types.BoolKey(false))
	tr := c.Encode(types.BoolKey(true))
	if c.Compare(f, tr) != -1 {
		t.Error("Bool: false < true")
	}
	if c.Compare(tr, f) != 1 {
		t.Error("Bool: true > false")
	}
}

func TestCodec_Date_Roundtrip(t *testing.T) {
	c := DateKeyCodec{}
	now := time.Now()
	epoch := time.Unix(0, 0)
	far := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, v := range []time.Time{now, epoch, far} {
		enc := c.Encode(types.DateKey(v))
		dec := c.Decode(enc).(types.DateKey)
		if time.Time(dec).UnixNano() != v.UnixNano() {
			t.Errorf("Date roundtrip: in=%v out=%v", v, time.Time(dec))
		}
	}
}

func TestCodec_Varchar_Roundtrip(t *testing.T) {
	c := VarcharKeyCodec{}
	for _, v := range []string{"", "a", "hello", "unicode-pt-br: ção", "very long key " + string(make([]byte, 100))} {
		enc := c.Encode(types.VarcharKey(v))
		dec := c.Decode(enc).(types.VarcharKey)
		if string(dec) != v {
			t.Errorf("Varchar roundtrip: in=%q out=%q", v, string(dec))
		}
	}
}

func TestCodec_Varchar_CompareOrdering(t *testing.T) {
	c := VarcharKeyCodec{}
	a := c.Encode(types.VarcharKey("apple"))
	b := c.Encode(types.VarcharKey("banana"))
	ab := c.Encode(types.VarcharKey("apple"))

	if c.Compare(a, b) != -1 {
		t.Error("Varchar: apple < banana")
	}
	if c.Compare(b, a) != 1 {
		t.Error("Varchar: banana > apple")
	}
	if c.Compare(a, ab) != 0 {
		t.Error("Varchar: apple == apple")
	}
	// Prefixo: "app" < "apple"
	pref := c.Encode(types.VarcharKey("app"))
	if c.Compare(pref, a) != -1 {
		t.Error("Varchar: prefixo < string completa")
	}
}

func TestCodec_Date_CompareOrdering(t *testing.T) {
	c := DateKeyCodec{}
	past := c.Encode(types.DateKey(time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)))
	now := c.Encode(types.DateKey(time.Now()))
	future := c.Encode(types.DateKey(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)))

	if c.Compare(past, now) != -1 {
		t.Error("Date: past < now")
	}
	if c.Compare(now, future) != -1 {
		t.Error("Date: now < future")
	}
	if c.Compare(past, future) != -1 {
		t.Error("Date: past < future")
	}
}
