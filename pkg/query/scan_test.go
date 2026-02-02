package query_test

import (
	"testing"

	"github.com/bobboyms/storage-engine/pkg/query"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// =============================================
// TESTES PARA CONSTRUTORES DE SCAN CONDITION
// =============================================

func TestEqual_Constructor(t *testing.T) {
	cond := query.Equal(types.IntKey(10))
	if cond == nil {
		t.Fatal("Expected non-nil condition")
	}
	if cond.Operator != query.OpEqual {
		t.Fatalf("Expected OpEqual, got %v", cond.Operator)
	}
	if cond.Value.Compare(types.IntKey(10)) != 0 {
		t.Fatalf("Expected value 10, got %v", cond.Value)
	}
}

func TestNotEqual_Constructor(t *testing.T) {
	cond := query.NotEqual(types.IntKey(20))
	if cond.Operator != query.OpNotEqual {
		t.Fatalf("Expected OpNotEqual, got %v", cond.Operator)
	}
}

func TestGreaterThan_Constructor(t *testing.T) {
	cond := query.GreaterThan(types.IntKey(30))
	if cond.Operator != query.OpGreaterThan {
		t.Fatalf("Expected OpGreaterThan, got %v", cond.Operator)
	}
}

func TestGreaterOrEqual_Constructor(t *testing.T) {
	cond := query.GreaterOrEqual(types.IntKey(40))
	if cond.Operator != query.OpGreaterOrEqual {
		t.Fatalf("Expected OpGreaterOrEqual, got %v", cond.Operator)
	}
}

func TestLessThan_Constructor(t *testing.T) {
	cond := query.LessThan(types.IntKey(50))
	if cond.Operator != query.OpLessThan {
		t.Fatalf("Expected OpLessThan, got %v", cond.Operator)
	}
}

func TestLessOrEqual_Constructor(t *testing.T) {
	cond := query.LessOrEqual(types.IntKey(60))
	if cond.Operator != query.OpLessOrEqual {
		t.Fatalf("Expected OpLessOrEqual, got %v", cond.Operator)
	}
}

func TestBetween_Constructor(t *testing.T) {
	cond := query.Between(types.IntKey(10), types.IntKey(20))
	if cond.Operator != query.OpBetween {
		t.Fatalf("Expected OpBetween, got %v", cond.Operator)
	}
	if cond.Value.Compare(types.IntKey(10)) != 0 {
		t.Fatalf("Expected start value 10, got %v", cond.Value)
	}
	if cond.ValueEnd.Compare(types.IntKey(20)) != 0 {
		t.Fatalf("Expected end value 20, got %v", cond.ValueEnd)
	}
}

// =============================================
// TESTES PARA Matches
// =============================================

func TestMatches_Equal(t *testing.T) {
	cond := query.Equal(types.IntKey(10))

	if !cond.Matches(types.IntKey(10)) {
		t.Error("Expected 10 to match")
	}
	if cond.Matches(types.IntKey(5)) {
		t.Error("Expected 5 to not match")
	}
	if cond.Matches(types.IntKey(15)) {
		t.Error("Expected 15 to not match")
	}
}

func TestMatches_NotEqual(t *testing.T) {
	cond := query.NotEqual(types.IntKey(10))

	if cond.Matches(types.IntKey(10)) {
		t.Error("Expected 10 to not match")
	}
	if !cond.Matches(types.IntKey(5)) {
		t.Error("Expected 5 to match")
	}
	if !cond.Matches(types.IntKey(15)) {
		t.Error("Expected 15 to match")
	}
}

func TestMatches_GreaterThan(t *testing.T) {
	cond := query.GreaterThan(types.IntKey(10))

	if cond.Matches(types.IntKey(10)) {
		t.Error("Expected 10 to not match (not greater)")
	}
	if cond.Matches(types.IntKey(5)) {
		t.Error("Expected 5 to not match")
	}
	if !cond.Matches(types.IntKey(15)) {
		t.Error("Expected 15 to match")
	}
}

func TestMatches_GreaterOrEqual(t *testing.T) {
	cond := query.GreaterOrEqual(types.IntKey(10))

	if !cond.Matches(types.IntKey(10)) {
		t.Error("Expected 10 to match (equal)")
	}
	if cond.Matches(types.IntKey(5)) {
		t.Error("Expected 5 to not match")
	}
	if !cond.Matches(types.IntKey(15)) {
		t.Error("Expected 15 to match")
	}
}

func TestMatches_LessThan(t *testing.T) {
	cond := query.LessThan(types.IntKey(10))

	if cond.Matches(types.IntKey(10)) {
		t.Error("Expected 10 to not match (not less)")
	}
	if !cond.Matches(types.IntKey(5)) {
		t.Error("Expected 5 to match")
	}
	if cond.Matches(types.IntKey(15)) {
		t.Error("Expected 15 to not match")
	}
}

func TestMatches_LessOrEqual(t *testing.T) {
	cond := query.LessOrEqual(types.IntKey(10))

	if !cond.Matches(types.IntKey(10)) {
		t.Error("Expected 10 to match (equal)")
	}
	if !cond.Matches(types.IntKey(5)) {
		t.Error("Expected 5 to match")
	}
	if cond.Matches(types.IntKey(15)) {
		t.Error("Expected 15 to not match")
	}
}

func TestMatches_Between(t *testing.T) {
	cond := query.Between(types.IntKey(10), types.IntKey(20))

	if !cond.Matches(types.IntKey(10)) {
		t.Error("Expected 10 to match (lower bound)")
	}
	if !cond.Matches(types.IntKey(15)) {
		t.Error("Expected 15 to match (in range)")
	}
	if !cond.Matches(types.IntKey(20)) {
		t.Error("Expected 20 to match (upper bound)")
	}
	if cond.Matches(types.IntKey(5)) {
		t.Error("Expected 5 to not match (below range)")
	}
	if cond.Matches(types.IntKey(25)) {
		t.Error("Expected 25 to not match (above range)")
	}
}

func TestMatches_DefaultFalse(t *testing.T) {
	// Teste para o case default do switch
	cond := &query.ScanCondition{Operator: query.ScanOperator(99)} // Operador inválido
	if cond.Matches(types.IntKey(10)) {
		t.Error("Expected default case to return false")
	}
}

// =============================================
// TESTES PARA GetStartKey
// =============================================

func TestGetStartKey_Equal(t *testing.T) {
	cond := query.Equal(types.IntKey(10))
	key := cond.GetStartKey()
	if key == nil || key.Compare(types.IntKey(10)) != 0 {
		t.Fatalf("Expected start key 10, got %v", key)
	}
}

func TestGetStartKey_GreaterThan(t *testing.T) {
	cond := query.GreaterThan(types.IntKey(10))
	key := cond.GetStartKey()
	if key == nil || key.Compare(types.IntKey(10)) != 0 {
		t.Fatalf("Expected start key 10, got %v", key)
	}
}

func TestGetStartKey_GreaterOrEqual(t *testing.T) {
	cond := query.GreaterOrEqual(types.IntKey(10))
	key := cond.GetStartKey()
	if key == nil || key.Compare(types.IntKey(10)) != 0 {
		t.Fatalf("Expected start key 10, got %v", key)
	}
}

func TestGetStartKey_Between(t *testing.T) {
	cond := query.Between(types.IntKey(10), types.IntKey(20))
	key := cond.GetStartKey()
	if key == nil || key.Compare(types.IntKey(10)) != 0 {
		t.Fatalf("Expected start key 10, got %v", key)
	}
}

func TestGetStartKey_LessThan(t *testing.T) {
	cond := query.LessThan(types.IntKey(10))
	key := cond.GetStartKey()
	if key != nil {
		t.Fatalf("Expected nil start key for LessThan, got %v", key)
	}
}

func TestGetStartKey_LessOrEqual(t *testing.T) {
	cond := query.LessOrEqual(types.IntKey(10))
	key := cond.GetStartKey()
	if key != nil {
		t.Fatalf("Expected nil start key for LessOrEqual, got %v", key)
	}
}

func TestGetStartKey_NotEqual(t *testing.T) {
	cond := query.NotEqual(types.IntKey(10))
	key := cond.GetStartKey()
	if key != nil {
		t.Fatalf("Expected nil start key for NotEqual, got %v", key)
	}
}

// =============================================
// TESTES PARA ShouldSeek
// =============================================

func TestShouldSeek_Equal(t *testing.T) {
	cond := query.Equal(types.IntKey(10))
	if !cond.ShouldSeek() {
		t.Error("Expected ShouldSeek=true for Equal")
	}
}

func TestShouldSeek_GreaterThan(t *testing.T) {
	cond := query.GreaterThan(types.IntKey(10))
	if !cond.ShouldSeek() {
		t.Error("Expected ShouldSeek=true for GreaterThan")
	}
}

func TestShouldSeek_GreaterOrEqual(t *testing.T) {
	cond := query.GreaterOrEqual(types.IntKey(10))
	if !cond.ShouldSeek() {
		t.Error("Expected ShouldSeek=true for GreaterOrEqual")
	}
}

func TestShouldSeek_Between(t *testing.T) {
	cond := query.Between(types.IntKey(10), types.IntKey(20))
	if !cond.ShouldSeek() {
		t.Error("Expected ShouldSeek=true for Between")
	}
}

func TestShouldSeek_LessThan(t *testing.T) {
	cond := query.LessThan(types.IntKey(10))
	if cond.ShouldSeek() {
		t.Error("Expected ShouldSeek=false for LessThan")
	}
}

func TestShouldSeek_LessOrEqual(t *testing.T) {
	cond := query.LessOrEqual(types.IntKey(10))
	if cond.ShouldSeek() {
		t.Error("Expected ShouldSeek=false for LessOrEqual")
	}
}

func TestShouldSeek_NotEqual(t *testing.T) {
	cond := query.NotEqual(types.IntKey(10))
	if cond.ShouldSeek() {
		t.Error("Expected ShouldSeek=false for NotEqual")
	}
}

// =============================================
// TESTES PARA ShouldContinue
// =============================================

func TestShouldContinue_Equal(t *testing.T) {
	cond := query.Equal(types.IntKey(10))

	// Antes ou igual a 10 -> continue
	if !cond.ShouldContinue(types.IntKey(5)) {
		t.Error("Expected continue for key < value")
	}
	if !cond.ShouldContinue(types.IntKey(10)) {
		t.Error("Expected continue for key == value")
	}
	// Depois de 10 -> stop
	if cond.ShouldContinue(types.IntKey(15)) {
		t.Error("Expected stop for key > value")
	}
}

func TestShouldContinue_LessThan(t *testing.T) {
	cond := query.LessThan(types.IntKey(10))

	if !cond.ShouldContinue(types.IntKey(5)) {
		t.Error("Expected continue for key < value")
	}
	if cond.ShouldContinue(types.IntKey(10)) {
		t.Error("Expected stop for key == value")
	}
	if cond.ShouldContinue(types.IntKey(15)) {
		t.Error("Expected stop for key > value")
	}
}

func TestShouldContinue_LessOrEqual(t *testing.T) {
	cond := query.LessOrEqual(types.IntKey(10))

	if !cond.ShouldContinue(types.IntKey(5)) {
		t.Error("Expected continue for key < value")
	}
	if !cond.ShouldContinue(types.IntKey(10)) {
		t.Error("Expected continue for key == value")
	}
	if cond.ShouldContinue(types.IntKey(15)) {
		t.Error("Expected stop for key > value")
	}
}

func TestShouldContinue_Between(t *testing.T) {
	cond := query.Between(types.IntKey(10), types.IntKey(20))

	if !cond.ShouldContinue(types.IntKey(15)) {
		t.Error("Expected continue for key in range")
	}
	if !cond.ShouldContinue(types.IntKey(20)) {
		t.Error("Expected continue for key == end")
	}
	if cond.ShouldContinue(types.IntKey(25)) {
		t.Error("Expected stop for key > end")
	}
}

func TestShouldContinue_GreaterThan(t *testing.T) {
	cond := query.GreaterThan(types.IntKey(10))

	// Para >, >= e !=, sempre continua
	if !cond.ShouldContinue(types.IntKey(5)) {
		t.Error("Expected continue for GreaterThan")
	}
	if !cond.ShouldContinue(types.IntKey(100)) {
		t.Error("Expected continue for GreaterThan")
	}
}

func TestShouldContinue_GreaterOrEqual(t *testing.T) {
	cond := query.GreaterOrEqual(types.IntKey(10))

	if !cond.ShouldContinue(types.IntKey(5)) {
		t.Error("Expected continue for GreaterOrEqual")
	}
	if !cond.ShouldContinue(types.IntKey(100)) {
		t.Error("Expected continue for GreaterOrEqual")
	}
}

func TestShouldContinue_NotEqual(t *testing.T) {
	cond := query.NotEqual(types.IntKey(10))

	if !cond.ShouldContinue(types.IntKey(5)) {
		t.Error("Expected continue for NotEqual")
	}
	if !cond.ShouldContinue(types.IntKey(10)) {
		t.Error("Expected continue for NotEqual")
	}
	if !cond.ShouldContinue(types.IntKey(100)) {
		t.Error("Expected continue for NotEqual")
	}
}

// =============================================
// TESTES COM DIFERENTES TIPOS DE DADOS
// =============================================

func TestMatches_WithVarcharKey(t *testing.T) {
	cond := query.Equal(types.VarcharKey("hello"))

	if !cond.Matches(types.VarcharKey("hello")) {
		t.Error("Expected 'hello' to match")
	}
	if cond.Matches(types.VarcharKey("world")) {
		t.Error("Expected 'world' to not match")
	}
}

func TestMatches_WithFloatKey(t *testing.T) {
	cond := query.GreaterThan(types.FloatKey(3.14))

	if cond.Matches(types.FloatKey(3.14)) {
		t.Error("Expected 3.14 to not match")
	}
	if !cond.Matches(types.FloatKey(4.0)) {
		t.Error("Expected 4.0 to match")
	}
}

func TestBetween_WithVarchar(t *testing.T) {
	cond := query.Between(types.VarcharKey("apple"), types.VarcharKey("cherry"))

	if !cond.Matches(types.VarcharKey("banana")) {
		t.Error("Expected 'banana' to match (in range)")
	}
	if cond.Matches(types.VarcharKey("date")) {
		t.Error("Expected 'date' to not match (out of range)")
	}
}
