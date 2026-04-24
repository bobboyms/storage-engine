package storage

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/bobboyms/storage-engine/pkg/btree"
	btreev2 "github.com/bobboyms/storage-engine/pkg/btree/v2"
	"github.com/bobboyms/storage-engine/pkg/crypto"
	"github.com/bobboyms/storage-engine/pkg/errors"
	"github.com/bobboyms/storage-engine/pkg/heap"
	v2 "github.com/bobboyms/storage-engine/pkg/heap/v2"
)

// HeapFormat seleciona a implementação de heap a ser usada por uma tabela.
type HeapFormat int

const (
	// HeapFormatV2 usa pkg/heap/v2 (page-based com BufferPool).
	HeapFormatV2 HeapFormat = iota
)

// NewHeapForTable cria um heap da implementação escolhida no caminho
// `path`, devolvendo a interface heap.Heap. O cipher é opcional.
func NewHeapForTable(format HeapFormat, path string, cipher ...crypto.Cipher) (heap.Heap, error) {
	var c crypto.Cipher
	if len(cipher) > 0 {
		c = cipher[0]
	}

	switch format {
	case HeapFormatV2:
		// BufferPool default: 64 páginas = 512KB de RAM por tabela.
		return v2.NewHeapV2(path, 64, c)
	default:
		return nil, fmt.Errorf("heap format desconhecido: %d", format)
	}
}

// BTreeFormat seleciona a implementação de B+ tree por índice.
type BTreeFormat int

const (
	// BTreeFormatV2 usa pkg/btree/v2 (page-based com BufferPool + TDE).
	BTreeFormatV2 BTreeFormat = iota
)

// NewBTreeForIndex cria uma B+ tree da implementação escolhida.
// Usa path + cipher. `keyType` determina o codec. TypeVarchar usa
// layout variable-key; demais usam fixed-key.
func NewBTreeForIndex(format BTreeFormat, primary bool, keyType DataType, path string, cipher crypto.Cipher) (btree.Tree, error) {
	switch format {
	case BTreeFormatV2:
		if keyType == TypeVarchar {
			return btreev2.NewBTreeV2Varchar(path, 16, cipher, btreev2.VarcharKeyCodec{})
		}
		codec, err := codecForDataType(keyType)
		if err != nil {
			return nil, err
		}
		return btreev2.NewBTreeV2Typed(path, 16, cipher, codec)
	default:
		return nil, fmt.Errorf("btree format desconhecido: %d", format)
	}
}

func defaultV2IndexPath(heapPath, tableName, indexName string) string {
	dir := filepath.Dir(heapPath)
	base := filepath.Base(heapPath)
	return filepath.Join(dir, fmt.Sprintf("%s.%s.%s.btree.v2", base, tableName, indexName))
}

// codecForDataType mapeia DataType fixo → btreev2.KeyCodec.
// Varchar tem path separado (NewBTreeV2Varchar) e não passa aqui.
func codecForDataType(t DataType) (btreev2.KeyCodec, error) {
	switch t {
	case TypeInt:
		return btreev2.IntKeyCodec{}, nil
	case TypeFloat:
		return btreev2.FloatKeyCodec{}, nil
	case TypeBoolean:
		return btreev2.BoolKeyCodec{}, nil
	case TypeDate:
		return btreev2.DateKeyCodec{}, nil
	case TypeVarchar:
		return nil, fmt.Errorf("codecForDataType: TypeVarchar não aceita aqui — use NewBTreeV2Varchar")
	default:
		return nil, fmt.Errorf("DataType não reconhecida: %d", t)
	}
}

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
	// Tree é a implementação page-based do índice.
	Tree btree.Tree
}

// Table representa uma tabela no banco de dados com seu próprio lock
// para permitir operações concorrentes em tabelas diferentes.
//
// Heap é a implementação page-based associada à tabela.
type Table struct {
	Name    string
	Indices map[string]*Index
	mu      sync.RWMutex // Lock por tabela para concorrência granular
	Heap    heap.Heap
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
	tables             map[string]*Table
	defaultIndexCipher crypto.Cipher
	mu                 sync.RWMutex // Protege acesso ao mapa de tabelas
}

func NewTableMenager() *TableMetaData {
	return &TableMetaData{
		tables: make(map[string]*Table),
	}
}

// NewEncryptedTableMenager cria metadados de tabela cujo índice BTreeV2
// automático herda o cipher informado. Use quando quiser TDE em índices
// criados implicitamente por NewTable.
func NewEncryptedTableMenager(indexCipher crypto.Cipher) *TableMetaData {
	return &TableMetaData{
		tables:             make(map[string]*Table),
		defaultIndexCipher: indexCipher,
	}
}

// SetDefaultIndexCipher configura o cipher usado por índices BTreeV2 criados
// automaticamente por NewTable. Índices fornecidos explicitamente em Index.Tree
// preservam o cipher com que foram abertos.
func (tb *TableMetaData) SetDefaultIndexCipher(indexCipher crypto.Cipher) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.defaultIndexCipher = indexCipher
}

func (tb *TableMetaData) NewTable(tableName string, indices []Index, t int, hm heap.Heap) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	if hm == nil {
		return &errors.HeapManagerRequiredError{
			TableName: tableName,
		}
	}

	// Verifica se a tabela já existe
	if _, exists := tb.tables[tableName]; exists {
		return &errors.TableAlreadyExistsError{
			Name: tableName,
		}
	}

	tempIndices := make(map[string]*Index, len(indices))

	primaryCount := 0
	for _, value := range indices {
		// Se o caller já forneceu uma Tree, usamos ela. Caso contrário,
		// criamos automaticamente um índice BTreeV2 sidecar para a tabela.
		var tree btree.Tree
		if value.Tree != nil {
			tree = value.Tree
		} else if _, ok := hm.(*v2.HeapV2); ok {
			treePath := defaultV2IndexPath(hm.Path(), tableName, value.Name)
			var err error
			tree, err = NewBTreeForIndex(BTreeFormatV2, value.Primary, value.Type, treePath, tb.defaultIndexCipher)
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("storage: heap legado não é mais suportado; use NewHeapForTable(HeapFormatV2, ...)")
		}

		if value.Primary {
			primaryCount++
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
		Heap:    hm,
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
