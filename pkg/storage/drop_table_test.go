package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	storageerrors "github.com/bobboyms/storage-engine/pkg/errors"
	"github.com/bobboyms/storage-engine/pkg/types"
	"github.com/bobboyms/storage-engine/pkg/wal"
)

// TestEngineDropTable locks in the physical contract of DropTable: the table
// disappears from the metadata, its heap and index tree files are deleted from
// disk, and dropping an unknown table reports TableNotFoundError.
func TestEngineDropTable(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	heapPath := filepath.Join(tmpDir, "users.heap")

	hm, err := NewHeapForTable(HeapFormatV2, heapPath)
	if err != nil {
		t.Fatalf("heap: %v", err)
	}
	tm := NewTableMenager()
	indices := []Index{
		{Name: "id", Primary: true, Type: TypeInt},
		{Name: "email", Type: TypeVarchar},
	}
	if err := tm.NewTable("users", indices, 4, hm); err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	ww, err := wal.NewWALWriter(walPath, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("wal writer: %v", err)
	}
	se, err := NewStorageEngine(tm, ww)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	t.Cleanup(func() { _ = se.Close() })

	ctx := context.Background()
	keys := map[string]types.Comparable{
		"id":    types.IntKey(1),
		"email": types.VarcharKey("a@b.c"),
	}
	if err := se.InsertRow(ctx, "users", `{"id":1,"email":"a@b.c"}`, keys); err != nil {
		t.Fatalf("InsertRow: %v", err)
	}

	// The index sidecar files must exist before the drop for the file-removal
	// assertion below to be meaningful.
	table, err := tm.GetTableByName("users")
	if err != nil {
		t.Fatalf("GetTableByName: %v", err)
	}
	treePaths := make([]string, 0, len(table.Indices))
	for name := range table.Indices {
		treePaths = append(treePaths, defaultV2IndexPath(heapPath, "users", name))
	}
	for _, p := range append([]string{heapPath}, treePaths...) {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("expected %s to exist before drop: %v", p, err)
		}
	}

	if err := se.DropTable(ctx, "users"); err != nil {
		t.Fatalf("DropTable: %v", err)
	}

	if _, err := tm.GetTableByName("users"); err == nil {
		t.Fatal("table still registered after DropTable")
	}
	if got := tm.ListTables(); len(got) != 0 {
		t.Fatalf("ListTables = %v, want empty", got)
	}
	for _, p := range append([]string{heapPath}, treePaths...) {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("expected %s to be deleted after drop, stat err = %v", p, err)
		}
	}

	var notFound *storageerrors.TableNotFoundError
	if err := se.DropTable(ctx, "users"); !errors.As(err, &notFound) {
		t.Fatalf("second DropTable err = %v, want TableNotFoundError", err)
	}
}
