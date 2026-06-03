package sql

import (
	"errors"
	"testing"
)

func TestParseInsert(t *testing.T) {
	stmt, err := Parse("INSERT INTO users (id, name, age) VALUES (1, 'bob', 30)")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	ins, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("got %T, want *InsertStmt", stmt)
	}
	if ins.Table != "users" {
		t.Fatalf("Table = %q, want users", ins.Table)
	}
	if len(ins.Columns) != 3 || ins.Columns[0] != "id" || ins.Columns[2] != "age" {
		t.Fatalf("Columns = %v", ins.Columns)
	}
	if len(ins.Values) != 3 {
		t.Fatalf("Values len = %d, want 3", len(ins.Values))
	}
	if ins.Values[0].String() != "1" || ins.Values[1].String() != "'bob'" || ins.Values[2].String() != "30" {
		t.Fatalf("Values = [%s %s %s]", ins.Values[0], ins.Values[1], ins.Values[2])
	}
}

func TestParseUpdate(t *testing.T) {
	stmt, err := Parse("UPDATE users SET name = 'al', age = 41 WHERE id = 1")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	upd, ok := stmt.(*UpdateStmt)
	if !ok {
		t.Fatalf("got %T, want *UpdateStmt", stmt)
	}
	if upd.Table != "users" {
		t.Fatalf("Table = %q, want users", upd.Table)
	}
	if len(upd.Assignments) != 2 {
		t.Fatalf("Assignments len = %d, want 2", len(upd.Assignments))
	}
	if upd.Assignments[0].Column != "name" || upd.Assignments[0].Value.String() != "'al'" {
		t.Fatalf("Assignment[0] = %+v", upd.Assignments[0])
	}
	if upd.Assignments[1].Column != "age" || upd.Assignments[1].Value.String() != "41" {
		t.Fatalf("Assignment[1] = %+v", upd.Assignments[1])
	}
	if upd.Where == nil || upd.Where.String() != "(id = 1)" {
		t.Fatalf("Where = %v, want (id = 1)", upd.Where)
	}
}

func TestParseDelete(t *testing.T) {
	stmt, err := Parse("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	del, ok := stmt.(*DeleteStmt)
	if !ok {
		t.Fatalf("got %T, want *DeleteStmt", stmt)
	}
	if del.Table != "users" {
		t.Fatalf("Table = %q, want users", del.Table)
	}
	if del.Where == nil || del.Where.String() != "(id = 1)" {
		t.Fatalf("Where = %v, want (id = 1)", del.Where)
	}
}

func TestParseDeleteWithoutWhere(t *testing.T) {
	stmt, err := Parse("DELETE FROM users")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	del, ok := stmt.(*DeleteStmt)
	if !ok {
		t.Fatalf("got %T, want *DeleteStmt", stmt)
	}
	if del.Where != nil {
		t.Fatalf("Where = %v, want nil", del.Where)
	}
}

func TestParseDMLErrors(t *testing.T) {
	inputs := []string{
		"INSERT users (id) VALUES (1)",          // missing INTO
		"INSERT INTO users (id) (1)",            // missing VALUES
		"INSERT INTO users (id) VALUES (a)",     // non-literal value
		"INSERT INTO users VALUES (1)",          // missing column list
		"UPDATE users name = 'x'",               // missing SET
		"UPDATE users SET name 'x'",             // missing = in assignment
		"UPDATE users SET name = id WHERE id=1", // non-literal assignment value
		"DELETE users WHERE id = 1",             // missing FROM
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			if _, err := Parse(in); !errors.Is(err, ErrParse) {
				t.Fatalf("Parse(%q) error = %v, want ErrParse", in, err)
			}
		})
	}
}
