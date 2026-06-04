package sql

import (
	"errors"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/storage"
)

func TestParseAlterTableAddColumn(t *testing.T) {
	stmt, err := Parse("ALTER TABLE users ADD COLUMN age INT")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	at, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("got %T, want *AlterTableStmt", stmt)
	}
	if at.Table != "users" || at.Drop {
		t.Fatalf("stmt = %+v, want ADD on users", at)
	}
	if at.Column.Name != "age" || at.Column.Type != storage.TypeInt || at.Column.Index || at.Column.Primary {
		t.Fatalf("column = %+v, want age INT", at.Column)
	}
}

func TestParseAlterTableAddColumnIndex(t *testing.T) {
	stmt, err := Parse("ALTER TABLE users ADD COLUMN email VARCHAR INDEX")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	at := stmt.(*AlterTableStmt)
	if at.Column.Name != "email" || at.Column.Type != storage.TypeVarchar || !at.Column.Index {
		t.Fatalf("column = %+v, want email VARCHAR INDEX", at.Column)
	}
}

func TestParseAlterTableAddColumnWithoutKeyword(t *testing.T) {
	// COLUMN is optional after ADD.
	stmt, err := Parse("ALTER TABLE users ADD score FLOAT")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	at := stmt.(*AlterTableStmt)
	if at.Column.Name != "score" || at.Column.Type != storage.TypeFloat {
		t.Fatalf("column = %+v, want score FLOAT", at.Column)
	}
}

func TestParseAlterTableDropColumn(t *testing.T) {
	stmt, err := Parse("ALTER TABLE users DROP COLUMN age")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	at := stmt.(*AlterTableStmt)
	if at.Table != "users" || !at.Drop || at.Column.Name != "age" {
		t.Fatalf("stmt = %+v, want DROP COLUMN age on users", at)
	}
}

func TestParseAlterTableErrors(t *testing.T) {
	inputs := []string{
		"ALTER users ADD COLUMN age INT",        // missing TABLE
		"ALTER TABLE users age INT",             // missing ADD/DROP
		"ALTER TABLE users ADD COLUMN age",      // missing type
		"ALTER TABLE users DROP",                // missing column name
		"ALTER TABLE users ADD COLUMN age NOPE", // unknown type
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			if _, err := Parse(in); !errors.Is(err, ErrParse) {
				t.Fatalf("Parse(%q) error = %v, want ErrParse", in, err)
			}
		})
	}
}
