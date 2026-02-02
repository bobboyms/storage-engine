package query

import (
	"github.com/bobboyms/storage-engine/pkg/types"
)

// Operadores de comparação para scans
type ScanOperator int

const (
	OpEqual          ScanOperator = iota // =
	OpNotEqual                           // !=
	OpGreaterThan                        // >
	OpGreaterOrEqual                     // >=
	OpLessThan                           // <
	OpLessOrEqual                        // <=
	OpBetween                            // BETWEEN x AND y
)

// Condição de scan
type ScanCondition struct {
	Operator ScanOperator
	Value    types.Comparable // Para operadores unários (=, !=, >, <, >=, <=)
	ValueEnd types.Comparable // Para BETWEEN (range)
}

// Construtores convenientes
func Equal(value types.Comparable) *ScanCondition {
	return &ScanCondition{Operator: OpEqual, Value: value}
}

func NotEqual(value types.Comparable) *ScanCondition {
	return &ScanCondition{Operator: OpNotEqual, Value: value}
}

func GreaterThan(value types.Comparable) *ScanCondition {
	return &ScanCondition{Operator: OpGreaterThan, Value: value}
}

func GreaterOrEqual(value types.Comparable) *ScanCondition {
	return &ScanCondition{Operator: OpGreaterOrEqual, Value: value}
}

func LessThan(value types.Comparable) *ScanCondition {
	return &ScanCondition{Operator: OpLessThan, Value: value}
}

func LessOrEqual(value types.Comparable) *ScanCondition {
	return &ScanCondition{Operator: OpLessOrEqual, Value: value}
}

func Between(start, end types.Comparable) *ScanCondition {
	return &ScanCondition{Operator: OpBetween, Value: start, ValueEnd: end}
}

// Matches verifica se uma chave satisfaz a condição
func (sc *ScanCondition) Matches(key types.Comparable) bool {
	switch sc.Operator {
	case OpEqual:
		return key.Compare(sc.Value) == 0
	case OpNotEqual:
		return key.Compare(sc.Value) != 0
	case OpGreaterThan:
		return key.Compare(sc.Value) > 0
	case OpGreaterOrEqual:
		return key.Compare(sc.Value) >= 0
	case OpLessThan:
		return key.Compare(sc.Value) < 0
	case OpLessOrEqual:
		return key.Compare(sc.Value) <= 0
	case OpBetween:
		return key.Compare(sc.Value) >= 0 && key.Compare(sc.ValueEnd) <= 0
	default:
		return false
	}
}

// GetStartKey retorna a chave inicial para otimizar o scan
func (sc *ScanCondition) GetStartKey() types.Comparable {
	switch sc.Operator {
	case OpEqual, OpGreaterThan, OpGreaterOrEqual, OpBetween:
		return sc.Value
	default:
		return nil // Full scan necessário
	}
}

// ShouldSeek indica se podemos usar Seek() para otimizar
func (sc *ScanCondition) ShouldSeek() bool {
	switch sc.Operator {
	case OpEqual, OpGreaterThan, OpGreaterOrEqual, OpBetween:
		return true
	default:
		return false // Operadores como != e < requerem full scan
	}
}

// ShouldContinue indica se devemos continuar o scan após encontrar uma chave
func (sc *ScanCondition) ShouldContinue(key types.Comparable) bool {
	switch sc.Operator {
	case OpEqual:
		// Para =, paramos após encontrar a chave (ou quando ultrapassar)
		return key.Compare(sc.Value) <= 0
	case OpLessThan, OpLessOrEqual:
		// Para < e <=, paramos quando ultrapassar o limite
		if sc.Operator == OpLessThan {
			return key.Compare(sc.Value) < 0
		}
		return key.Compare(sc.Value) <= 0
	case OpBetween:
		// Para BETWEEN, paramos quando ultrapassar o fim do range
		return key.Compare(sc.ValueEnd) <= 0
	default:
		// Para >, >=, != precisamos continuar até o fim
		return true
	}
}
