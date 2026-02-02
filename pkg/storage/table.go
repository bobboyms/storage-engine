package storage

import (
	"github.com/bobboyms/storage-engine/pkg/btree"
	"github.com/bobboyms/storage-engine/pkg/errors"
)

type DataType int

const (
	TypeInt     DataType = iota // 0: Inteiro (int64)
	TypeVarchar                 // 1: String variável
	TypeBoolean                 // 2: Bool
	TypeFloat                   // 3: Float64
	TypeDate                    // 4: Timestamp
)

// Função auxiliar útil para debug
func (d DataType) String() string {
	return [...]string{"INT", "VARCHAR", "BOOL", "FLOAT", "DATE"}[d]
}

type Index struct {
	Name    string
	Primary bool
	Type    DataType
	Tree    *btree.BPlusTree
}

type Table struct {
	Name    string
	Heap    map[int]string
	Indices map[string]*Index
}

type TableMetaData struct {
	tables map[string]*Table
}

func NewTableMenager() *TableMetaData {
	return &TableMetaData{
		tables: make(map[string]*Table),
	}
}

func (tb *TableMetaData) NewTable(tableName string, indices []Index, t int) error {
	// Verifica se a tabela já existe
	if _, exists := tb.tables[tableName]; exists {
		return &errors.TableAlreadyExistsError{
			Name: tableName,
		}
	}

	tempIndices := make(map[string]*Index, len(indices))

	primaryCount := 0
	for _, value := range indices {
		// Cria árvore única se for chave primária
		var tree *btree.BPlusTree
		if value.Primary {
			tree = btree.NewUniqueTree(t)
			primaryCount++
		} else {
			tree = btree.NewTree(t)
		}

		idxPtr := &Index{
			Name:    value.Name,
			Primary: value.Primary,
			Type:    value.Type,
			Tree:    tree,
		}

		tempIndices[value.Name] = idxPtr

	}

	if primaryCount == 0 {
		return &errors.PrimarykeyNotDefinedError{
			TableName: tableName,
		}
	}

	if primaryCount > 1 {
		return &errors.TwoPrimarykeysError{
			Total: primaryCount,
		}
	}

	tb.tables[tableName] = &Table{
		Name:    tableName,
		Heap:    make(map[int]string),
		Indices: tempIndices,
	}

	return nil
}

func (tb *TableMetaData) GetTableByName(name string) (*Table, error) {
	table, ok := tb.tables[name]
	if !ok {
		return nil, &errors.TableNotFoundError{
			Name: name,
		}
	}
	return table, nil
}

func (tb *TableMetaData) GetIndexByName(tableName string, indexName string) (*Index, error) {
	table, err := tb.GetTableByName(tableName)
	if err != nil {
		return nil, err
	}

	index, ok := table.Indices[indexName]
	if !ok {
		return nil, &errors.IndexNotFoundError{
			Name: indexName,
		}
	}
	return index, nil
}
