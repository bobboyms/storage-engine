package v2

import (
	"bytes"
	"math"
	"time"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// VariableKeyCodec é a interface paralela pra keys de tamanho variável
// (hoje só VarcharKey). Usa representação byte-slice em vez de uint64.
// BTreeV2 detecta via type switch qual layout de page usar.
type VariableKeyCodec interface {
	// Encode serializa k em bytes. Tamanho varia.
	Encode(k types.Comparable) []byte

	// Decode inverte Encode.
	Decode(b []byte) types.Comparable

	// Compare é a comparação semântica (-1/0/1).
	Compare(a, b []byte) int
}

// VarcharKeyCodec: serializa strings como UTF-8 puro (sem prefixo de tamanho
// — o tamanho vem do slot dir). Comparação lexicográfica bytewise.
type VarcharKeyCodec struct{}

func (VarcharKeyCodec) Encode(k types.Comparable) []byte {
	return []byte(string(k.(types.VarcharKey)))
}

func (VarcharKeyCodec) Decode(b []byte) types.Comparable {
	return types.VarcharKey(string(b))
}

func (VarcharKeyCodec) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// KeyCodec abstrai encoding/decoding/comparison de keys para a B+ tree v2.
//
// Todas as keys são armazenadas em 8 bytes no page (uint64). O codec é
// responsável pela conversão types.Comparable ↔ uint64 e pela comparação
// semântica — NOT podemos confiar em comparação uint64 direta porque:
//   - IntKey negativo (bits com sign-bit 1) compararia maior que positivo
//   - FloatKey bits not seguem ordem numérica (negativos em sign-magnitude)
//
// Types de key de tamanho variável (VarcharKey) NOT são suportados
// nesta versão — requerem layout de slot diferente (indirection ou
// overflow pages). Fica pra sub-etapa futura.
type KeyCodec interface {
	// Encode converte k pra representação binária de 8 bytes.
	Encode(k types.Comparable) uint64

	// Decode inverte Encode.
	Decode(u uint64) types.Comparable

	// Compare retorna -1/0/1 pra ordem semântica dos types.Comparable
	// correspondentes a `a` e `b` (ambos representações encoded).
	Compare(a, b uint64) int
}

// ─────────────────────────────────────────────────────────────────────
// IntKeyCodec — IntKey ↔ uint64 (preserva bits do int64)
// Comparação via cast pra int64 (respeita sign bit).
// ─────────────────────────────────────────────────────────────────────

type IntKeyCodec struct{}

func (IntKeyCodec) Encode(k types.Comparable) uint64 {
	return uint64(int64(k.(types.IntKey)))
}

func (IntKeyCodec) Decode(u uint64) types.Comparable {
	return types.IntKey(int64(u))
}

func (IntKeyCodec) Compare(a, b uint64) int {
	ai, bi := int64(a), int64(b)
	if ai < bi {
		return -1
	}
	if ai > bi {
		return 1
	}
	return 0
}

// ─────────────────────────────────────────────────────────────────────
// FloatKeyCodec — FloatKey ↔ uint64 (bits IEEE 754)
// Comparação via float64 (respeita NaN/-0 conforme Comparable.Compare).
// ─────────────────────────────────────────────────────────────────────

type FloatKeyCodec struct{}

func (FloatKeyCodec) Encode(k types.Comparable) uint64 {
	return math.Float64bits(float64(k.(types.FloatKey)))
}

func (FloatKeyCodec) Decode(u uint64) types.Comparable {
	return types.FloatKey(math.Float64frombits(u))
}

func (FloatKeyCodec) Compare(a, b uint64) int {
	af, bf := math.Float64frombits(a), math.Float64frombits(b)
	if af < bf {
		return -1
	}
	if af > bf {
		return 1
	}
	return 0
}

// ─────────────────────────────────────────────────────────────────────
// BoolKeyCodec — BoolKey ↔ 0/1
// ─────────────────────────────────────────────────────────────────────

type BoolKeyCodec struct{}

func (BoolKeyCodec) Encode(k types.Comparable) uint64 {
	if bool(k.(types.BoolKey)) {
		return 1
	}
	return 0
}

func (BoolKeyCodec) Decode(u uint64) types.Comparable {
	return types.BoolKey(u != 0)
}

func (BoolKeyCodec) Compare(a, b uint64) int {
	// false (0) < true (1) — ordem uint64 direta já vale
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// ─────────────────────────────────────────────────────────────────────
// DateKeyCodec — DateKey ↔ UnixNano (int64)
// ─────────────────────────────────────────────────────────────────────

type DateKeyCodec struct{}

func (DateKeyCodec) Encode(k types.Comparable) uint64 {
	return uint64(time.Time(k.(types.DateKey)).UnixNano())
}

func (DateKeyCodec) Decode(u uint64) types.Comparable {
	return types.DateKey(time.Unix(0, int64(u)))
}

func (DateKeyCodec) Compare(a, b uint64) int {
	ai, bi := int64(a), int64(b)
	if ai < bi {
		return -1
	}
	if ai > bi {
		return 1
	}
	return 0
}
