package sql

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/storage"
)

func sampleVersionSchemas() []TableSchema {
	return []TableSchema{{
		Name:    "users",
		Columns: []Column{{Name: "id", Type: storage.TypeInt}},
		Indexes: []IndexDef{{Name: "id", Column: "id", Primary: true}},
	}}
}

func TestCatalogRoundTripVersioned(t *testing.T) {
	dir := t.TempDir()
	if err := saveSchemas(dir, sampleVersionSchemas()); err != nil {
		t.Fatalf("saveSchemas: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, schemaFileName))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(data), "format_version") {
		t.Fatalf("schema file must record a format version, got: %s", data)
	}

	got, err := loadSchemas(dir)
	if err != nil {
		t.Fatalf("loadSchemas: %v", err)
	}
	if len(got) != 1 || got[0].Name != "users" {
		t.Fatalf("round trip = %+v", got)
	}
}

func TestCatalogReadsLegacyBareArray(t *testing.T) {
	dir := t.TempDir()
	// The pre-envelope format: a bare array of tables. It must still load
	// (treated as format version 1) so existing databases keep opening.
	legacy := `[{"name":"users","columns":[{"name":"id","type":"INT"}],"indexes":[{"name":"id","column":"id","primary":true}]}]`
	if err := os.WriteFile(filepath.Join(dir, schemaFileName), []byte(legacy), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := loadSchemas(dir)
	if err != nil {
		t.Fatalf("legacy loadSchemas: %v", err)
	}
	if len(got) != 1 || got[0].Name != "users" {
		t.Fatalf("legacy parse = %+v", got)
	}
}

func TestCatalogRejectsNewerFormatVersion(t *testing.T) {
	dir := t.TempDir()
	future := `{"format_version": 999, "tables": []}`
	if err := os.WriteFile(filepath.Join(dir, schemaFileName), []byte(future), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, err := loadSchemas(dir)
	if !errors.Is(err, ErrUnsupportedCatalogVersion) {
		t.Fatalf("expected ErrUnsupportedCatalogVersion for a newer catalog, got %v", err)
	}
}
