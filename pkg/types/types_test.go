package types

import (
	"testing"
	"time"
)

func TestComparableStrings(t *testing.T) {
	now := time.Now()
	cases := []struct {
		key      Comparable
		expected string
	}{
		{IntKey(10), "10"},
		{VarcharKey("test"), "test"},
		{FloatKey(3.14), "3.140000"},
		{BoolKey(true), "true"},
		{BoolKey(false), "false"},
		{DateKey(now), now.Format("2006-01-02 15:04:05")},
	}

	for _, tc := range cases {
		if s := tc.key.(interface{ String() string }).String(); s != tc.expected {
			t.Errorf("Expected %q, got %q", tc.expected, s)
		}
	}
}

// =============================================
// TESTES PARA IntKey.Compare
// =============================================

func TestIntKey_Compare_LessThan(t *testing.T) {
	k := IntKey(5)
	result := k.Compare(IntKey(10))
	if result != -1 {
		t.Errorf("Expected -1 for 5 < 10, got %d", result)
	}
}

func TestIntKey_Compare_GreaterThan(t *testing.T) {
	k := IntKey(10)
	result := k.Compare(IntKey(5))
	if result != 1 {
		t.Errorf("Expected 1 for 10 > 5, got %d", result)
	}
}

func TestIntKey_Compare_Equal(t *testing.T) {
	k := IntKey(10)
	result := k.Compare(IntKey(10))
	if result != 0 {
		t.Errorf("Expected 0 for 10 == 10, got %d", result)
	}
}

func TestIntKey_Compare_Negative(t *testing.T) {
	k := IntKey(-5)
	result := k.Compare(IntKey(5))
	if result != -1 {
		t.Errorf("Expected -1 for -5 < 5, got %d", result)
	}
}

// =============================================
// TESTES PARA VarcharKey.Compare
// =============================================

func TestVarcharKey_Compare_LessThan(t *testing.T) {
	k := VarcharKey("apple")
	result := k.Compare(VarcharKey("banana"))
	if result != -1 {
		t.Errorf("Expected -1 for 'apple' < 'banana', got %d", result)
	}
}

func TestVarcharKey_Compare_GreaterThan(t *testing.T) {
	k := VarcharKey("cherry")
	result := k.Compare(VarcharKey("banana"))
	if result != 1 {
		t.Errorf("Expected 1 for 'cherry' > 'banana', got %d", result)
	}
}

func TestVarcharKey_Compare_Equal(t *testing.T) {
	k := VarcharKey("test")
	result := k.Compare(VarcharKey("test"))
	if result != 0 {
		t.Errorf("Expected 0 for 'test' == 'test', got %d", result)
	}
}

func TestVarcharKey_Compare_CaseSensitive(t *testing.T) {
	k := VarcharKey("Apple")
	result := k.Compare(VarcharKey("apple"))
	// 'A' < 'a' em ASCII
	if result != -1 {
		t.Errorf("Expected -1 for 'Apple' < 'apple', got %d", result)
	}
}

func TestVarcharKey_Compare_EmptyString(t *testing.T) {
	k := VarcharKey("")
	result := k.Compare(VarcharKey("a"))
	if result != -1 {
		t.Errorf("Expected -1 for '' < 'a', got %d", result)
	}
}

// =============================================
// TESTES PARA FloatKey.Compare
// =============================================

func TestFloatKey_Compare_LessThan(t *testing.T) {
	k := FloatKey(1.5)
	result := k.Compare(FloatKey(2.5))
	if result != -1 {
		t.Errorf("Expected -1 for 1.5 < 2.5, got %d", result)
	}
}

func TestFloatKey_Compare_GreaterThan(t *testing.T) {
	k := FloatKey(3.14)
	result := k.Compare(FloatKey(2.71))
	if result != 1 {
		t.Errorf("Expected 1 for 3.14 > 2.71, got %d", result)
	}
}

func TestFloatKey_Compare_Equal(t *testing.T) {
	k := FloatKey(3.14)
	result := k.Compare(FloatKey(3.14))
	if result != 0 {
		t.Errorf("Expected 0 for 3.14 == 3.14, got %d", result)
	}
}

func TestFloatKey_Compare_NegativeNumbers(t *testing.T) {
	k := FloatKey(-1.5)
	result := k.Compare(FloatKey(1.5))
	if result != -1 {
		t.Errorf("Expected -1 for -1.5 < 1.5, got %d", result)
	}
}

func TestFloatKey_Compare_SmallDifference(t *testing.T) {
	k := FloatKey(0.001)
	result := k.Compare(FloatKey(0.002))
	if result != -1 {
		t.Errorf("Expected -1 for 0.001 < 0.002, got %d", result)
	}
}

// =============================================
// TESTES PARA BoolKey.Compare
// =============================================

func TestBoolKey_Compare_FalseLessThanTrue(t *testing.T) {
	k := BoolKey(false)
	result := k.Compare(BoolKey(true))
	if result != -1 {
		t.Errorf("Expected -1 for false < true, got %d", result)
	}
}

func TestBoolKey_Compare_TrueGreaterThanFalse(t *testing.T) {
	k := BoolKey(true)
	result := k.Compare(BoolKey(false))
	if result != 1 {
		t.Errorf("Expected 1 for true > false, got %d", result)
	}
}

func TestBoolKey_Compare_TrueEqualsTrue(t *testing.T) {
	k := BoolKey(true)
	result := k.Compare(BoolKey(true))
	if result != 0 {
		t.Errorf("Expected 0 for true == true, got %d", result)
	}
}

func TestBoolKey_Compare_FalseEqualsFalse(t *testing.T) {
	k := BoolKey(false)
	result := k.Compare(BoolKey(false))
	if result != 0 {
		t.Errorf("Expected 0 for false == false, got %d", result)
	}
}

// =============================================
// TESTES PARA DateKey.Compare
// =============================================

func TestDateKey_Compare_Before(t *testing.T) {
	earlier := DateKey(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	later := DateKey(time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC))

	result := earlier.Compare(later)
	if result != -1 {
		t.Errorf("Expected -1 for earlier < later, got %d", result)
	}
}

func TestDateKey_Compare_After(t *testing.T) {
	earlier := DateKey(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	later := DateKey(time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC))

	result := later.Compare(earlier)
	if result != 1 {
		t.Errorf("Expected 1 for later > earlier, got %d", result)
	}
}

func TestDateKey_Compare_Equal(t *testing.T) {
	date1 := DateKey(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))
	date2 := DateKey(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))

	result := date1.Compare(date2)
	if result != 0 {
		t.Errorf("Expected 0 for equal dates, got %d", result)
	}
}

func TestDateKey_Compare_DifferentYears(t *testing.T) {
	date2025 := DateKey(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	date2024 := DateKey(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	result := date2024.Compare(date2025)
	if result != -1 {
		t.Errorf("Expected -1 for 2024 < 2025, got %d", result)
	}
}

func TestDateKey_Compare_DifferentTimes(t *testing.T) {
	morning := DateKey(time.Date(2025, 1, 1, 8, 0, 0, 0, time.UTC))
	evening := DateKey(time.Date(2025, 1, 1, 20, 0, 0, 0, time.UTC))

	result := morning.Compare(evening)
	if result != -1 {
		t.Errorf("Expected -1 for morning < evening, got %d", result)
	}
}
