package storage

import (
	"sync"

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

// Table representa uma tabela no banco de dados com seu próprio lock
// para permitir operações concorrentes em tabelas diferentes
type Table struct {
	Name    string
	Indices map[string]*Index
	mu      sync.RWMutex // Lock por tabela para concorrência granular
}

// Lock adquire write lock na tabela
func (t *Table) Lock() {
	t.mu.Lock()
}

// Unlock libera write lock na tabela
func (t *Table) Unlock() {
	t.mu.Unlock()
}

// RLock adquire read lock na tabela
func (t *Table) RLock() {
	t.mu.RLock()
}

// RUnlock libera read lock na tabela
func (t *Table) RUnlock() {
	t.mu.RUnlock()
}

// GetIndex retorna o índice pelo nome de forma thread-safe (Schema Lock)
func (t *Table) GetIndex(indexName string) (*Index, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	index, ok := t.Indices[indexName]
	if !ok {
		return nil, &errors.IndexNotFoundError{
			Name: indexName,
		}
	}
	return index, nil
}

// GetIndices retorna todos os índices da tabela de forma thread-safe (Schema Lock)
func (t *Table) GetIndices() []*Index {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.GetIndicesUnsafe()
}

// GetIndicesUnsafe retorna todos os índices sem adquirir lock.
// O CHAMADOR DEVE GARANTIR QUE JÁ POSSUI RLOCK OU LOCK NA TABELA!
func (t *Table) GetIndicesUnsafe() []*Index {
	indices := make([]*Index, 0, len(t.Indices))
	for _, idx := range t.Indices {
		indices = append(indices, idx)
	}
	return indices
}

// TableMetaData gerencia os metadados das tabelas com thread-safety
type TableMetaData struct {
	tables map[string]*Table
	mu     sync.RWMutex // Protege acesso ao mapa de tabelas
}

func NewTableMenager() *TableMetaData {
	return &TableMetaData{
		tables: make(map[string]*Table),
	}
}

func (tb *TableMetaData) NewTable(tableName string, indices []Index, t int) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()

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
		Indices: tempIndices,
	}

	return nil
}

func (tb *TableMetaData) GetTableByName(name string) (*Table, error) {
	tb.mu.RLock()
	defer tb.mu.RUnlock()

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

	// Protege acesso ao mapa de índices da tabela
	table.mu.RLock()
	defer table.mu.RUnlock()

	index, ok := table.Indices[indexName]
	if !ok {
		return nil, &errors.IndexNotFoundError{
			Name: indexName,
		}
	}
	return index, nil
}

func (tb *TableMetaData) ListTables() []string {
	tb.mu.RLock()
	defer tb.mu.RUnlock()

	names := make([]string, 0, len(tb.tables))
	for name := range tb.tables {
		names = append(names, name)
	}
	return names
}

func (tb *TableMetaData) GetIndexes(tableName string) ([]*Index, error) {
	table, err := tb.GetTableByName(tableName)
	if err != nil {
		return nil, err
	}
	indices := make([]*Index, 0, len(table.Indices))
	for _, idx := range table.Indices {
		indices = append(indices, idx)
	}
	return indices, nil
}
