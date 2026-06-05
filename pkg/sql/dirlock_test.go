package sql

import (
	"context"
	"errors"
	"testing"
)

// TestOpenDatabaseRejectsConcurrentOpen verifies that a second open of the same
// directory while the first is still live is rejected with ErrDatabaseLocked.
// Without a cross-process directory lock two live handles would write to the
// same heap and B-tree files and silently corrupt the indexes.
func TestOpenDatabaseRejectsConcurrentOpen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabase(ctx, dir)
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer db.Close()

	if _, err := OpenDatabase(ctx, dir); !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("second OpenDatabase error = %v, want ErrDatabaseLocked", err)
	}
}

// TestOpenDatabaseReopenAfterClose verifies that closing a database releases the
// directory lock so the directory can be opened again by a later handle.
func TestOpenDatabaseReopenAfterClose(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := OpenDatabase(ctx, dir)
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := OpenDatabase(ctx, dir)
	if err != nil {
		t.Fatalf("reopen after Close: %v", err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("Close reopened: %v", err)
	}
}
