package storage_test

import (
	"testing"

	"github.com/bobboyms/storage-engine/pkg/errors"
	"github.com/bobboyms/storage-engine/pkg/query"
	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// =============================================
// TESTES PARA TableMetaData (NewTableMenager)
// =============================================

func TestNewTableMenager_Creation(t *testing.T) {
	mgr := storage.NewTableMenager()
	if mgr == nil {
		t.Fatal("NewTableMenager should not return nil")
	}
}

func TestNewTable_Success_SinglePrimaryKey(t *testing.T) {
	mgr := storage.NewTableMenager()

	err := mgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)

	if err != nil {
		t.Fatalf("NewTable should succeed with single primary key, got error: %v", err)
	}

	// Verifica que a tabela foi criada
	table, err := mgr.GetTableByName("users")
	if err != nil {
		t.Fatalf("GetTableByName should succeed: %v", err)
	}
	if table.Name != "users" {
		t.Fatalf("Expected table name 'users', got '%s'", table.Name)
	}
}

func TestNewTable_Success_MultipleIndices(t *testing.T) {
	mgr := storage.NewTableMenager()

	err := mgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
		{Name: "email", Primary: false, Type: storage.TypeVarchar},
		{Name: "age", Primary: false, Type: storage.TypeInt},
	}, 3)

	if err != nil {
		t.Fatalf("NewTable should succeed with multiple indices, got error: %v", err)
	}

	table, _ := mgr.GetTableByName("users")

	// Verifica que todos os índices foram criados
	if len(table.Indices) != 3 {
		t.Fatalf("Expected 3 indices, got %d", len(table.Indices))
	}

	// Verifica índice primário
	idIndex, err := mgr.GetIndexByName("users", "id")
	if err != nil {
		t.Fatalf("Primary index 'id' should exist: %v", err)
	}
	if !idIndex.Primary {
		t.Fatal("Index 'id' should be primary")
	}
	if idIndex.Tree == nil {
		t.Fatal("Index tree should be initialized")
	}
}

func TestNewTable_Error_NoPrimaryKey(t *testing.T) {
	mgr := storage.NewTableMenager()

	err := mgr.NewTable("users", []storage.Index{
		{Name: "email", Primary: false, Type: storage.TypeVarchar},
		{Name: "age", Primary: false, Type: storage.TypeInt},
	}, 3)

	if err == nil {
		t.Fatal("NewTable should fail when no primary key is defined")
	}

	// Verifica que é o erro correto
	if _, ok := err.(*errors.PrimarykeyNotDefinedError); !ok {
		t.Fatalf("Expected PrimarykeyNotDefinedError, got %T: %v", err, err)
	}
}

func TestNewTable_Error_MultiplePrimaryKeys(t *testing.T) {
	mgr := storage.NewTableMenager()

	err := mgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
		{Name: "email", Primary: true, Type: storage.TypeVarchar}, // ERRO: segunda PK
	}, 3)

	if err == nil {
		t.Fatal("NewTable should fail when multiple primary keys are defined")
	}

	// Verifica que é o erro correto
	if _, ok := err.(*errors.TwoPrimarykeysError); !ok {
		t.Fatalf("Expected TwoPrimarykeysError, got %T: %v", err, err)
	}
}

func TestNewTable_Error_DuplicateTableName(t *testing.T) {
	mgr := storage.NewTableMenager()

	// Primeira criação - deve funcionar
	err := mgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)
	if err != nil {
		t.Fatalf("First table creation should succeed: %v", err)
	}

	// Segunda criação com mesmo nome - deve falhar
	err = mgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)

	if err == nil {
		t.Fatal("Expected error for duplicate table name")
	}

	if _, ok := err.(*errors.TableAlreadyExistsError); !ok {
		t.Fatalf("Expected TableAlreadyExistsError, got %T: %v", err, err)
	}
}

func TestGetTableByName_Error_NotFound(t *testing.T) {
	mgr := storage.NewTableMenager()

	_, err := mgr.GetTableByName("nonexistent")
	if err == nil {
		t.Fatal("GetTableByName should fail for nonexistent table")
	}

	if _, ok := err.(*errors.TableNotFoundError); !ok {
		t.Fatalf("Expected TableNotFoundError, got %T: %v", err, err)
	}
}

func TestGetIndexByName_Error_IndexNotFound(t *testing.T) {
	mgr := storage.NewTableMenager()

	mgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
	}, 3)

	_, err := mgr.GetIndexByName("users", "nonexistent")
	if err == nil {
		t.Fatal("GetIndexByName should fail for nonexistent index")
	}

	if _, ok := err.(*errors.IndexNotFoundError); !ok {
		t.Fatalf("Expected IndexNotFoundError, got %T: %v", err, err)
	}
}

// Teste de integração: Criar tabela e inserir dados
func TestTableManager_Integration(t *testing.T) {
	mgr := storage.NewTableMenager()

	// Cria tabela
	err := mgr.NewTable("users", []storage.Index{
		{Name: "id", Primary: true, Type: storage.TypeInt},
		{Name: "age", Primary: false, Type: storage.TypeInt},
	}, 3)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Cria storage engine
	se := storage.NewStorageEngine(mgr)

	// Insere dados na PK
	err = se.Put("users", "id", types.IntKey(1), 100)
	if err != nil {
		t.Fatalf("Failed to insert into primary key: %v", err)
	}

	err = se.Put("users", "id", types.IntKey(2), 200)
	if err != nil {
		t.Fatalf("Failed to insert into primary key: %v", err)
	}

	// Tenta inserir duplicata na PK - deve falhar
	err = se.Put("users", "id", types.IntKey(1), 300)
	if err == nil {
		t.Fatal("Inserting duplicate primary key should fail")
	}

	// Insere dados no índice secundário (permite duplicatas)
	err = se.Put("users", "age", types.IntKey(25), 1)
	if err != nil {
		t.Fatalf("Failed to insert into secondary index: %v", err)
	}

	err = se.Put("users", "age", types.IntKey(25), 2) // Duplicata OK em índice secundário
	if err != nil {
		t.Fatalf("Secondary index should allow duplicates: %v", err)
	}

	// Verifica busca
	results, err := se.Scan("users", "id", query.Equal(types.IntKey(1)))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(results) != 1 || results[0] != 100 {
		t.Fatalf("Expected [100], got %v", results)
	}
}

func TestDataTypeString(t *testing.T) {
	cases := []struct {
		dt       storage.DataType
		expected string
	}{
		{storage.TypeInt, "INT"},
		{storage.TypeVarchar, "VARCHAR"},
		{storage.TypeBoolean, "BOOL"},
		{storage.TypeFloat, "FLOAT"},
		{storage.TypeDate, "DATE"},
	}

	for _, tc := range cases {
		if tc.dt.String() != tc.expected {
			t.Errorf("Expected %q, got %q", tc.expected, tc.dt.String())
		}
	}
}

func TestGetIndexByName_Error_TableNotFound(t *testing.T) {
	mgr := storage.NewTableMenager()
	_, err := mgr.GetIndexByName("nonexistent", "id")
	if err == nil {
		t.Fatal("Expected error for nonexistent table")
	}
}
