package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/bobboyms/storage-engine/pkg/btree"
	btreev2 "github.com/bobboyms/storage-engine/pkg/btree/v2"
	"github.com/bobboyms/storage-engine/pkg/crypto"
	"github.com/bobboyms/storage-engine/pkg/errors"
	"github.com/bobboyms/storage-engine/pkg/heap"
	v2 "github.com/bobboyms/storage-engine/pkg/heap/v2"
)

// HeapFormat selects the heap implementation to be used by a table.
type HeapFormat int

const (
	// HeapFormatV2 uses pkg/heap/v2 (page-based with BufferPool).
	HeapFormatV2 HeapFormat = iota
)

// NewHeapForTable creates a heap of the chosen implementation at path
// `path`, returning the heap.Heap interface. The cipher is optional.
func NewHeapForTable(format HeapFormat, path string, cipher ...crypto.Cipher) (heap.Heap, error) {
	var c crypto.Cipher
	if len(cipher) > 0 {
		c = cipher[0]
	}

	switch format {
	case HeapFormatV2:
		// BufferPool default: 64 pages = 512KB of RAM per table.
		return v2.NewHeapV2(path, 64, c)
	default:
		return nil, fmt.Errorf("unknown heap format: %d", format)
	}
}

// BTreeFormat selects the B+ tree implementation per index.
type BTreeFormat int

const (
	// BTreeFormatV2 uses pkg/btree/v2 (page-based with BufferPool + TDE).
	BTreeFormatV2 BTreeFormat = iota
)

// NewBTreeForIndex creates a B+ tree of the chosen implementation.
// Uses path + cipher. `keyType` determines the codec. TypeVarchar uses the
// variable-key layout; others use fixed-key.
func NewBTreeForIndex(format BTreeFormat, primary bool, keyType DataType, path string, cipher crypto.Cipher) (btree.Tree, error) {
	switch format {
	case BTreeFormatV2:
		if keyType == TypeVarchar {
			return btreev2.NewBTreeV2Varchar(path, 16, cipher, btreev2.VarcharKeyCodec{})
		}
		if keyType == TypeBytes {
			return btreev2.NewBTreeV2Varchar(path, 16, cipher, btreev2.BytesKeyCodec{})
		}
		if keyType == TypeUUID {
			return btreev2.NewBTreeV2Varchar(path, 16, cipher, btreev2.UUIDKeyCodec{})
		}
		if keyType == TypeDecimal {
			return btreev2.NewBTreeV2Varchar(path, 16, cipher, btreev2.DecimalKeyCodec{})
		}
		codec, err := codecForDataType(keyType)
		if err != nil {
			return nil, err
		}
		return btreev2.NewBTreeV2Typed(path, 16, cipher, codec)
	default:
		return nil, fmt.Errorf("unknown btree format: %d", format)
	}
}

func defaultV2IndexPath(heapPath, tableName, indexName string) string {
	dir := filepath.Dir(heapPath)
	base := filepath.Base(heapPath)
	return filepath.Join(dir, fmt.Sprintf("%s.%s.%s.btree.v2", base, tableName, indexName))
}

// codecForDataType mapeia DataType fixo → btreev2.KeyCodec.
// Varchar tem path separado (NewBTreeV2Varchar) e does not go through aqui.
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
	case TypeDateOnly:
		return btreev2.DateOnlyKeyCodec{}, nil
	case TypeVarchar:
		return nil, fmt.Errorf("codecForDataType: TypeVarchar is not accepted here - use NewBTreeV2Varchar")
	default:
		return nil, fmt.Errorf("unrecognized DataType: %d", t)
	}
}

type DataType int

const (
	TypeInt      DataType = iota // 0: Inteiro (int64)
	TypeVarchar                  // 1: String variável
	TypeBoolean                  // 2: Bool
	TypeFloat                    // 3: Float64
	TypeDate                     // 4: Timestamp
	TypeBytes                    // 5: Binary (BLOB)
	TypeUUID                     // 6: 16-byte UUID
	TypeDecimal                  // 7: Exact-precision decimal
	TypeDateOnly                 // 8: Calendar date (no time, no timezone)
)

// Função auxiliar útil para debug
func (d DataType) String() string {
	return [...]string{"INT", "VARCHAR", "BOOL", "FLOAT", "DATE", "BYTES", "UUID", "DECIMAL", "DATEONLY"}[d]
}

type Index struct {
	Name    string
	Primary bool
	Type    DataType
	// Unique, when set on a secondary index, makes writes reject a row whose
	// logical key already belongs to a different (visible) row. Enforcement is
	// MVCC-aware and happens under the table write lock; the physical storage is
	// unchanged (still composite (logical, primary) entries).
	Unique bool
	// Secondary indexes store physical composite keys:
	// (logical_secondary_key, primary_key) -> record pointer. This keeps each
	// B+ tree entry unique while allowing duplicate logical secondary keys.
	// Tree is the page-based index implementation.
	Tree btree.Tree
}

// Table represents a table in the database with its own lock to allow
// concurrent operations on different tables.
//
// Heap is the page-based implementation associated with the table.
type Table struct {
	Name    string
	Indices map[string]*Index
	mu      sync.RWMutex // Per-table lock for granular concurrency
	Heap    heap.Heap
}

// Lock acquires the table's write lock
func (t *Table) Lock() {
	t.mu.Lock()
}

// Unlock releases the table's write lock
func (t *Table) Unlock() {
	t.mu.Unlock()
}

// RLock acquires the table's read lock
func (t *Table) RLock() {
	t.mu.RLock()
}

// RUnlock releases the table's read lock
func (t *Table) RUnlock() {
	t.mu.RUnlock()
}

// GetIndex returns the index by name in a thread-safe way (Schema Lock)
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

// GetIndices returns all of the table's indexes in a thread-safe way (Schema Lock)
func (t *Table) GetIndices() []*Index {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.GetIndicesUnsafe()
}

// GetIndicesUnsafe returns all indexes without acquiring a lock.
// THE CALLER MUST GUARANTEE IT ALREADY HOLDS RLOCK OR LOCK ON THE TABLE!
func (t *Table) GetIndicesUnsafe() []*Index {
	indices := make([]*Index, 0, len(t.Indices))
	for _, idx := range t.Indices {
		indices = append(indices, idx)
	}
	return indices
}

// TableMetaData manages the tables' metadata with thread-safety
type TableMetaData struct {
	tables             map[string]*Table
	defaultIndexCipher crypto.Cipher
	mu                 sync.RWMutex // Protects access to the table map
}

func NewTableMenager() *TableMetaData {
	return &TableMetaData{
		tables: make(map[string]*Table),
	}
}

// NewEncryptedTableMenager creates table metadata whose automatic BTreeV2
// index inherits the provided cipher. Use it when you want TDE on indexes
// created implicitly by NewTable.
func NewEncryptedTableMenager(indexCipher crypto.Cipher) *TableMetaData {
	return &TableMetaData{
		tables:             make(map[string]*Table),
		defaultIndexCipher: indexCipher,
	}
}

// SetDefaultIndexCipher configures the cipher used by BTreeV2 indexes created
// automatically by NewTable. Indexes provided explicitly in Index.Tree
// preserve the cipher they were opened with.
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

	// Check whether the table already exists
	if _, exists := tb.tables[tableName]; exists {
		return &errors.TableAlreadyExistsError{
			Name: tableName,
		}
	}

	primaryCount := 0
	for _, value := range indices {
		if value.Primary {
			primaryCount++
		}
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

	tempIndices := make(map[string]*Index, len(indices))

	for _, value := range indices {
		// If the caller already provided a Tree, we use it. Otherwise,
		// we automatically create a sidecar BTreeV2 index for the table.
		var tree btree.Tree
		if value.Tree != nil {
			tree = value.Tree
		} else if _, ok := hm.(*v2.HeapV2); ok {
			treePath := defaultV2IndexPath(hm.Path(), tableName, value.Name)
			var err error
			if value.Primary {
				tree, err = NewBTreeForIndex(BTreeFormatV2, true, value.Type, treePath, tb.defaultIndexCipher)
			} else {
				tree, err = btreev2.NewBTreeV2Varchar(treePath, 16, tb.defaultIndexCipher, btreev2.CompositeKeyCodec{})
			}
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("storage: legacy heap is no longer supported; use NewHeapForTable(HeapFormatV2, ...)")
		}

		idxPtr := &Index{
			Name:    value.Name,
			Primary: value.Primary,
			Type:    value.Type,
			Unique:  value.Unique,
			Tree:    tree,
		}

		tempIndices[value.Name] = idxPtr

	}

	tb.tables[tableName] = &Table{
		Name:    tableName,
		Indices: tempIndices,
		Heap:    hm,
	}

	return nil
}

// AddIndex builds and registers a new secondary index on an existing table.
// The sidecar BTreeV2 is created exactly like NewTable does for secondary
// indexes, inheriting the manager's default index cipher so TDE-enabled
// databases keep encrypting their indexes. Primary indexes cannot be added to
// a table that already has one.
func (tb *TableMetaData) AddIndex(tableName string, idx Index) error {
	if idx.Primary {
		return fmt.Errorf("storage: cannot add a primary index to existing table %q", tableName)
	}

	tb.mu.RLock()
	table, ok := tb.tables[tableName]
	cipher := tb.defaultIndexCipher
	tb.mu.RUnlock()
	if !ok {
		return &errors.TableNotFoundError{Name: tableName}
	}

	table.mu.Lock()
	defer table.mu.Unlock()

	if _, exists := table.Indices[idx.Name]; exists {
		return fmt.Errorf("storage: index %q already exists on table %q", idx.Name, tableName)
	}
	if _, ok := table.Heap.(*v2.HeapV2); !ok {
		return fmt.Errorf("storage: legacy heap is no longer supported; use NewHeapForTable(HeapFormatV2, ...)")
	}

	treePath := defaultV2IndexPath(table.Heap.Path(), tableName, idx.Name)
	tree, err := btreev2.NewBTreeV2Varchar(treePath, 16, cipher, btreev2.CompositeKeyCodec{})
	if err != nil {
		return err
	}

	table.Indices[idx.Name] = &Index{
		Name:    idx.Name,
		Primary: false,
		Type:    idx.Type,
		Unique:  idx.Unique,
		Tree:    tree,
	}
	return nil
}

// DropTable closes every index tree and the heap of an existing table, deletes
// their backing files, and unregisters the table. Trees shared by multiple
// index names (composite aliases) are closed and deleted once. The first error
// is reported, but removal proceeds so a partial failure never leaves a
// half-registered table.
func (tb *TableMetaData) DropTable(tableName string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	table, ok := tb.tables[tableName]
	if !ok {
		return &errors.TableNotFoundError{Name: tableName}
	}

	table.mu.Lock()
	defer table.mu.Unlock()

	var firstErr error
	keep := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	closedTrees := make(map[btree.Tree]bool)
	for _, idx := range table.Indices {
		if idx.Tree == nil || closedTrees[idx.Tree] {
			continue
		}
		closedTrees[idx.Tree] = true
		var path string
		if treeV2, ok := idx.Tree.(*btreev2.BTreeV2); ok {
			path = treeV2.Path()
		}
		keep(idx.Tree.Close())
		if path != "" {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				keep(err)
			}
		}
	}

	if table.Heap != nil {
		heapPath := table.Heap.Path()
		keep(table.Heap.Close())
		if heapPath != "" {
			if err := os.Remove(heapPath); err != nil && !os.IsNotExist(err) {
				keep(err)
			}
		}
	}

	delete(tb.tables, tableName)
	return firstErr
}

// DropIndex detaches a secondary index from a table, closes its B+ tree, and
// deletes the backing file. Primary indexes back the table's heap access path
// and cannot be dropped.
func (tb *TableMetaData) DropIndex(tableName, indexName string) error {
	table, err := tb.GetTableByName(tableName)
	if err != nil {
		return err
	}

	table.mu.Lock()
	defer table.mu.Unlock()

	idx, ok := table.Indices[indexName]
	if !ok {
		return &errors.IndexNotFoundError{Name: indexName}
	}
	if idx.Primary {
		return fmt.Errorf("storage: cannot drop primary index %q on table %q", indexName, tableName)
	}

	var path string
	if treeV2, ok := idx.Tree.(*btreev2.BTreeV2); ok {
		path = treeV2.Path()
	}
	if err := idx.Tree.Close(); err != nil {
		return err
	}
	delete(table.Indices, indexName)

	if path != "" {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
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

	// Protect access to the table's index map
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
