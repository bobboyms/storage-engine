package sql

import (
	"context"
	"errors"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func openUUIDDB(t *testing.T) (*Executor, context.Context) {
	t.Helper()
	ctx := context.Background()
	db, err := OpenDatabaseWithOptions(ctx, t.TempDir(), OpenOptions{DisableMaintenance: true})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(ctx, "CREATE TABLE docs (id UUID PRIMARY KEY, label VARCHAR)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	return db, ctx
}

// TestUUIDBindRoundTrip binds a UUID value through every Go argument form the
// driver should accept and asserts the stored value decodes back to the same
// typed types.UUIDKey, so a UUID column is fully writable and readable via SQL.
// Each form uses a distinct UUID so the cases are independent.
func TestUUIDBindRoundTrip(t *testing.T) {
	db, ctx := openUUIDDB(t)

	mk := func(s string) types.UUIDKey {
		k, err := types.ParseUUID(s)
		if err != nil {
			t.Fatalf("ParseUUID(%s): %v", s, err)
		}
		return k
	}
	idKey := mk("3b241101-e2bb-4255-8caf-4136c566a962")
	idArr := mk("00000000-0000-4000-8000-000000000001")
	idSlice := mk("11111111-1111-4111-8111-111111111111")
	idPtr := mk("22222222-2222-4222-8222-222222222222")
	arr := [16]byte(idArr)
	slice := idSlice[:]

	cases := []struct {
		label string
		arg   any
		want  types.UUIDKey
	}{
		{"uuidkey", idKey, idKey},
		{"array16", arr, idArr},
		{"slice", slice, idSlice},
		{"pointer", &idPtr, idPtr},
	}
	for _, c := range cases {
		if _, err := db.Exec(ctx,
			"INSERT INTO docs (id, label) VALUES (?, ?)", c.arg, c.label); err != nil {
			t.Fatalf("INSERT %s arg %T: %v", c.label, c.arg, err)
		}
		rs, err := db.Query(ctx, "SELECT id, label FROM docs WHERE id = ?", c.arg)
		if err != nil {
			t.Fatalf("SELECT %s: %v", c.label, err)
		}
		if len(rs.Rows) != 1 {
			t.Fatalf("%s: rows = %d, want 1", c.label, len(rs.Rows))
		}
		got := rs.Rows[0][colIndex(rs, "id")]
		key, ok := got.(types.UUIDKey)
		if !ok {
			t.Fatalf("%s: decoded id type = %T, want types.UUIDKey", c.label, got)
		}
		if key != c.want {
			t.Fatalf("%s: id = %s, want %s", c.label, key, c.want)
		}
		if got := strColumn(t, rs, "label"); got[0] != c.label {
			t.Fatalf("%s: label = %q, want %q", c.label, got[0], c.label)
		}
	}
}

// TestUUIDUpdateAndDelete exercises mutation paths on a UUID-keyed table: an
// UPDATE that rewrites the document (the row is re-derived through the codec)
// and a DELETE selected by the UUID primary key.
func TestUUIDUpdateAndDelete(t *testing.T) {
	db, ctx := openUUIDDB(t)
	id, _ := types.ParseUUID("3b241101-e2bb-4255-8caf-4136c566a962")

	if _, err := db.Exec(ctx, "INSERT INTO docs (id, label) VALUES (?, ?)", id, "before"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := db.Exec(ctx, "UPDATE docs SET label = ? WHERE id = ?", "after", id); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	rs, err := db.Query(ctx, "SELECT id, label FROM docs WHERE id = ?", id)
	if err != nil {
		t.Fatalf("SELECT after update: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %d, want 1 after update", len(rs.Rows))
	}
	if got := strColumn(t, rs, "label"); got[0] != "after" {
		t.Fatalf("label = %q, want after", got[0])
	}
	if key, ok := rs.Rows[0][colIndex(rs, "id")].(types.UUIDKey); !ok || key != id {
		t.Fatalf("id after update = %v, want %s (UUID preserved across rewrite)", rs.Rows[0][colIndex(rs, "id")], id)
	}

	if _, err := db.Exec(ctx, "DELETE FROM docs WHERE id = ?", id); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	rs, _ = db.Query(ctx, "SELECT id FROM docs WHERE id = ?", id)
	if len(rs.Rows) != 0 {
		t.Fatalf("rows = %d, want 0 after delete", len(rs.Rows))
	}
}

// TestUUIDStringLiteralInsert writes a UUID column from a quoted SQL string
// literal (no placeholder), the raw-SQL path that must also resolve to a UUID.
func TestUUIDStringLiteralInsert(t *testing.T) {
	db, ctx := openUUIDDB(t)

	const text = "3b241101-e2bb-4255-8caf-4136c566a962"
	if _, err := db.Exec(ctx,
		"INSERT INTO docs (id, label) VALUES ('"+text+"', 'lit')"); err != nil {
		t.Fatalf("INSERT string literal: %v", err)
	}
	want, _ := types.ParseUUID(text)
	rs, err := db.Query(ctx, "SELECT id FROM docs WHERE id = ?", want)
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rs.Rows))
	}
	if got := rs.Rows[0][colIndex(rs, "id")]; got != types.Comparable(want) {
		t.Fatalf("id = %v, want %s", got, want)
	}
}

// TestUUIDBindRejectsBadLength keeps the bind error contract: a []byte that is
// not exactly 16 bytes cannot be a UUID and must wrap ErrBind.
func TestUUIDBindRejectsBadLength(t *testing.T) {
	db, ctx := openUUIDDB(t)
	_, err := db.Exec(ctx,
		"INSERT INTO docs (id, label) VALUES (?, ?)", []byte{1, 2, 3}, "bad")
	if err == nil {
		t.Fatal("expected error for 3-byte UUID arg")
	}
	if !errors.Is(err, ErrBind) {
		t.Fatalf("expected ErrBind, got %v", err)
	}
}
