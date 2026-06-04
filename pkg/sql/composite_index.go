package sql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// Composite indexes are implemented entirely in the SQL layer as a derived
// single-column VARCHAR secondary index whose key is a deterministic encoding
// of the indexed column tuple. This reuses the engine's existing VARCHAR index
// without adding a multi-column key type to the storage/btree layers. The
// encoding only needs to be injective (distinct tuples map to distinct keys),
// which is enough for the exact-match lookups a composite index serves; the
// residual WHERE predicate re-checks every row, so correctness never depends on
// the encoding's ordering.

// compositeIndexName derives a stable index name from its column list.
func compositeIndexName(columns []string) string {
	return "idx_" + strings.Join(columns, "_")
}

// encodeCompositeKey builds the VARCHAR index key for an ordered tuple of column
// values. Each component is length-prefixed so no two distinct tuples can share
// an encoding (e.g. ["a","bc"] and ["ab","c"] differ).
func encodeCompositeKey(values []types.Comparable) types.VarcharKey {
	var sb strings.Builder
	for _, v := range values {
		s := componentString(v)
		sb.WriteString(strconv.Itoa(len(s)))
		sb.WriteByte(':')
		sb.WriteString(s)
	}
	return types.VarcharKey(sb.String())
}

// componentString renders one tuple component. NULL gets a marker distinct from
// any encodable value (which always carries a type tag prefix).
func componentString(v types.Comparable) string {
	if isNull(v) {
		return "\x00NULL"
	}
	return fmt.Sprintf("%T=%v", v, v)
}

// compositeKeyForValues converts the literals of a composite index's columns
// (in index order) into the encoded VARCHAR key. Every column must be present.
func compositeKeyForValues(schema *TableSchema, idx IndexDef, values map[string]*Literal) (types.VarcharKey, error) {
	parts := make([]types.Comparable, len(idx.Columns))
	for i, name := range idx.Columns {
		lit, ok := values[name]
		if !ok {
			return "", fmt.Errorf("%w: INSERT must provide indexed column %q", ErrExec, name)
		}
		col, _ := schema.Column(name)
		key, err := ColumnValue(lit, col.Type)
		if err != nil {
			return "", fmt.Errorf("%w: %v", ErrExec, err)
		}
		parts[i] = key
	}
	return encodeCompositeKey(parts), nil
}

// addCompositeKeyFields injects each composite index's encoded key into the
// document under the index name. The storage engine derives every index's key
// from a document field named after the index, so the encoded tuple key must be
// stored as a synthetic field. These fields are ignored when decoding rows
// (decodeRow reads only declared schema columns).
func addCompositeKeyFields(doc map[string]any, schema *TableSchema, keys map[string]types.Comparable) {
	for _, idx := range schema.Indexes {
		if !idx.composite() {
			continue
		}
		if k, ok := keys[idx.Name].(types.VarcharKey); ok {
			doc[idx.Name] = string(k)
		}
	}
}

// compositeKeyFromDoc encodes a composite index key from a decoded document map
// (used by the UPSERT path), converting each column value to its key type.
func compositeKeyFromDoc(schema *TableSchema, idx IndexDef, doc map[string]any) (types.VarcharKey, error) {
	parts := make([]types.Comparable, len(idx.Columns))
	for i, name := range idx.Columns {
		col, _ := schema.Column(name)
		key, err := jsonValueToKey(doc[name], col.Type)
		if err != nil {
			return "", err
		}
		parts[i] = key
	}
	return encodeCompositeKey(parts), nil
}
