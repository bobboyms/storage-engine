package types

import (
	"fmt"
	"time"
)

// Comparable é a interface que todas as chaves devem implementar
type Comparable interface {
	Compare(other Comparable) int // Retorna -1 se <, 0 se ==, 1 se >
}

// === Implementações de Chave ===

// IntKey: Chave de Inteiro
type IntKey int

func (k IntKey) Compare(other Comparable) int {
	o := other.(IntKey)
	if k < o {
		return -1
	}
	if k > o {
		return 1
	}
	return 0
}

// VarcharKey: Chave de String
type VarcharKey string

func (k VarcharKey) Compare(other Comparable) int {
	o := other.(VarcharKey)
	if k < o {
		return -1
	}
	if k > o {
		return 1
	}
	return 0
}

// FloatKey: Chave de Float
type FloatKey float64

func (k FloatKey) Compare(other Comparable) int {
	o := other.(FloatKey)
	if k < o {
		return -1
	}
	if k > o {
		return 1
	}
	return 0
}

// BoolKey: Chave Booleana (false < true)
type BoolKey bool

func (k BoolKey) Compare(other Comparable) int {
	o := other.(BoolKey)
	if k == o {
		return 0
	}
	if !k && o {
		return -1
	}
	return 1
}

// DateKey: Chave de Data/Hora
type DateKey time.Time

func (k DateKey) Compare(other Comparable) int {
	o := time.Time(other.(DateKey))
	t := time.Time(k)
	if t.Before(o) {
		return -1
	}
	if t.After(o) {
		return 1
	}
	return 0
}

func (k DateKey) String() string {
	return time.Time(k).Format("2006-01-02 15:04:05")
}

func (k IntKey) String() string     { return fmt.Sprintf("%d", k) }
func (k VarcharKey) String() string { return string(k) }
func (k FloatKey) String() string   { return fmt.Sprintf("%f", k) }
func (k BoolKey) String() string    { return fmt.Sprintf("%t", bool(k)) }
