package sql

import (
	"errors"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/storage"
)

func sampleSchema() TableSchema {
	return TableSchema{
		Name: "users",
		Columns: []Column{
			{Name: "id", Type: storage.TypeInt},
			{Name: "name", Type: storage.TypeVarchar},
			{Name: "age", Type: storage.TypeInt},
		},
		Indexes: []IndexDef{
			{Name: "pk_users", Column: "id", Primary: true},
			{Name: "idx_age", Column: "age"},
		},
	}
}

func TestCatalogAddAndLookupTable(t *testing.T) {
	c := NewCatalog()
	if err := c.AddTable(sampleSchema()); err != nil {
		t.Fatalf("AddTable returned error: %v", err)
	}

	ts, ok := c.Table("users")
	if !ok {
		t.Fatal("Table(users) not found after AddTable")
	}
	if ts.Name != "users" {
		t.Fatalf("Table name = %q, want users", ts.Name)
	}

	if _, ok := c.Table("missing"); ok {
		t.Fatal("Table(missing) should not be found")
	}
}

func TestTableSchemaColumnLookup(t *testing.T) {
	ts := sampleSchema()

	col, ok := ts.Column("name")
	if !ok {
		t.Fatal("Column(name) not found")
	}
	if col.Type != storage.TypeVarchar {
		t.Fatalf("Column(name).Type = %v, want VARCHAR", col.Type)
	}

	if _, ok := ts.Column("unknown"); ok {
		t.Fatal("Column(unknown) should not be found")
	}
}

func TestTableSchemaPrimaryIndex(t *testing.T) {
	ts := sampleSchema()

	pk, ok := ts.PrimaryIndex()
	if !ok {
		t.Fatal("PrimaryIndex not found")
	}
	if pk.Name != "pk_users" || pk.Column != "id" || !pk.Primary {
		t.Fatalf("PrimaryIndex = %+v, want pk_users on id", pk)
	}
}

func TestTableSchemaIndexForColumn(t *testing.T) {
	ts := sampleSchema()

	idx, ok := ts.IndexForColumn("age")
	if !ok {
		t.Fatal("IndexForColumn(age) not found")
	}
	if idx.Name != "idx_age" {
		t.Fatalf("IndexForColumn(age) = %q, want idx_age", idx.Name)
	}

	if _, ok := ts.IndexForColumn("name"); ok {
		t.Fatal("IndexForColumn(name) should not be found (no index)")
	}
}

func TestCatalogAddTableValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*TableSchema)
		want   error
	}{
		{
			name:   "empty name",
			mutate: func(s *TableSchema) { s.Name = "" },
			want:   ErrInvalidSchema,
		},
		{
			name:   "no columns",
			mutate: func(s *TableSchema) { s.Columns = nil },
			want:   ErrInvalidSchema,
		},
		{
			name: "duplicate column",
			mutate: func(s *TableSchema) {
				s.Columns = append(s.Columns, Column{Name: "id", Type: storage.TypeInt})
			},
			want: ErrInvalidSchema,
		},
		{
			name: "index on unknown column",
			mutate: func(s *TableSchema) {
				s.Indexes = append(s.Indexes, IndexDef{Name: "idx_x", Column: "nope"})
			},
			want: ErrInvalidSchema,
		},
		{
			name: "no primary index",
			mutate: func(s *TableSchema) {
				s.Indexes = []IndexDef{{Name: "idx_age", Column: "age"}}
			},
			want: ErrInvalidSchema,
		},
		{
			name: "duplicate index name",
			mutate: func(s *TableSchema) {
				s.Indexes = append(s.Indexes, IndexDef{Name: "pk_users", Column: "age"})
			},
			want: ErrInvalidSchema,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := sampleSchema()
			tt.mutate(&s)
			err := NewCatalog().AddTable(s)
			if !errors.Is(err, tt.want) {
				t.Fatalf("AddTable error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestCatalogAddDuplicateTable(t *testing.T) {
	c := NewCatalog()
	if err := c.AddTable(sampleSchema()); err != nil {
		t.Fatalf("first AddTable error: %v", err)
	}
	err := c.AddTable(sampleSchema())
	if !errors.Is(err, ErrDuplicateTable) {
		t.Fatalf("duplicate AddTable error = %v, want ErrDuplicateTable", err)
	}
}
