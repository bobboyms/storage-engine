package sql

import (
	"context"
	"strings"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// Seed data (see executor_select_test.go): alice/30, bob/25, carol/40, dave/30.

func TestUpdateSetArithmetic(t *testing.T) {
	e := newExecutor(t)
	n, err := e.Exec(context.Background(), "UPDATE users SET age = age + 10 WHERE id = 2")
	if err != nil {
		t.Fatalf("Exec UPDATE: %v", err)
	}
	if n != 1 {
		t.Fatalf("affected = %d, want 1", n)
	}
	rs, err := e.Query(context.Background(), "SELECT age FROM users WHERE id = 2")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := intColumn(t, rs, "age"); len(got) != 1 || got[0] != 35 {
		t.Fatalf("age after update = %v, want [35]", got)
	}
}

func TestUpdateSetArithmeticMultipleColumns(t *testing.T) {
	e := newExecutor(t)
	// age * 2 - 10 mixes precedence levels.
	if _, err := e.Exec(context.Background(), "UPDATE users SET age = age * 2 - 10 WHERE id = 3"); err != nil {
		t.Fatalf("Exec UPDATE: %v", err)
	}
	rs, _ := e.Query(context.Background(), "SELECT age FROM users WHERE id = 3")
	if got := intColumn(t, rs, "age"); len(got) != 1 || got[0] != 70 {
		t.Fatalf("age = %v, want [70]", got)
	}
}

func TestSelectProjectionArithmetic(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT age * 2 AS double_age FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := intColumn(t, rs, "double_age"); len(got) != 1 || got[0] != 60 {
		t.Fatalf("double_age = %v, want [60]", got)
	}
}

func TestSelectProjectionArithmeticTwoColumns(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT id + age AS total FROM users WHERE id = 4")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := intColumn(t, rs, "total"); len(got) != 1 || got[0] != 34 {
		t.Fatalf("total = %v, want [34]", got)
	}
}

func TestWhereArithmetic(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT name FROM users WHERE age + 10 > 45")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	names := strColumn(t, rs, "name")
	if len(names) != 1 || names[0] != "carol" {
		t.Fatalf("names = %v, want [carol]", names)
	}
}

func TestWhereParenthesizedArithmetic(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT name FROM users WHERE (age - 5) * 2 = 50")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	names := strColumn(t, rs, "name")
	if len(names) != 2 {
		t.Fatalf("names = %v, want [alice dave]", names)
	}
}

func TestIntegerDivisionAndModulo(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT age / 7 AS d, age % 7 AS m FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := intColumn(t, rs, "d"); got[0] != 4 {
		t.Fatalf("30 / 7 = %v, want 4", got[0])
	}
	if got := intColumn(t, rs, "m"); got[0] != 2 {
		t.Fatalf("30 %% 7 = %v, want 2", got[0])
	}
}

func TestDivisionByZeroFails(t *testing.T) {
	e := newExecutor(t)
	_, err := e.Query(context.Background(), "SELECT age / 0 AS d FROM users WHERE id = 1")
	if err == nil {
		t.Fatal("expected division-by-zero error, got nil")
	}
}

func TestUnaryMinusLiteral(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT age + -5 AS v FROM users WHERE id = 2")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := intColumn(t, rs, "v"); got[0] != 20 {
		t.Fatalf("25 + -5 = %v, want 20", got[0])
	}
}

func TestScalarFunctionUpperLower(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT UPPER(name) AS up, LOWER(name) AS lo FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := strColumn(t, rs, "up"); got[0] != "ALICE" {
		t.Fatalf("UPPER = %q, want ALICE", got[0])
	}
	if got := strColumn(t, rs, "lo"); got[0] != "alice" {
		t.Fatalf("LOWER = %q, want alice", got[0])
	}
}

func TestScalarFunctionInWhere(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT name FROM users WHERE UPPER(name) = 'BOB'")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	names := strColumn(t, rs, "name")
	if len(names) != 1 || names[0] != "bob" {
		t.Fatalf("names = %v, want [bob]", names)
	}
}

func TestScalarFunctionLengthAbs(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT LENGTH(name) AS n, ABS(0 - age) AS a FROM users WHERE id = 3")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := intColumn(t, rs, "n"); got[0] != 5 {
		t.Fatalf("LENGTH(carol) = %v, want 5", got[0])
	}
	if got := intColumn(t, rs, "a"); got[0] != 40 {
		t.Fatalf("ABS(-40) = %v, want 40", got[0])
	}
}

func TestScalarFunctionCoalesce(t *testing.T) {
	e := newExecutor(t)
	// nickname does not exist; use a NULL-able expression through CASE instead:
	// COALESCE over a NULL literal falls through to the fallback.
	rs, err := e.Query(context.Background(), "SELECT COALESCE(NULL, name) AS v FROM users WHERE id = 4")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := strColumn(t, rs, "v"); got[0] != "dave" {
		t.Fatalf("COALESCE = %q, want dave", got[0])
	}
}

func TestUnknownFunctionFails(t *testing.T) {
	e := newExecutor(t)
	_, err := e.Query(context.Background(), "SELECT NOSUCHFN(name) AS v FROM users WHERE id = 1")
	if err == nil {
		t.Fatal("expected unknown-function error, got nil")
	}
}

func TestCaseWhenProjection(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(),
		"SELECT name, CASE WHEN age >= 30 THEN 'senior' ELSE 'junior' END AS level FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	levels := strColumn(t, rs, "level")
	want := []string{"senior", "junior", "senior", "senior"}
	for i, w := range want {
		if levels[i] != w {
			t.Fatalf("level[%d] = %q, want %q (all: %v)", i, levels[i], w, levels)
		}
	}
}

func TestCaseWhenWithoutElseYieldsNull(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(),
		"SELECT CASE WHEN age > 100 THEN 'old' END AS v FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if _, ok := rs.Rows[0][0].(types.NullKey); !ok {
		t.Fatalf("CASE without ELSE = %T(%v), want NullKey", rs.Rows[0][0], rs.Rows[0][0])
	}
}

func TestCaseWhenInWhere(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(),
		"SELECT name FROM users WHERE CASE WHEN age > 35 THEN 1 ELSE 0 END = 1")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	names := strColumn(t, rs, "name")
	if len(names) != 1 || names[0] != "carol" {
		t.Fatalf("names = %v, want [carol]", names)
	}
}

func TestUpdateSetFunction(t *testing.T) {
	e := newExecutor(t)
	if _, err := e.Exec(context.Background(), "UPDATE users SET name = UPPER(name) WHERE id = 1"); err != nil {
		t.Fatalf("Exec UPDATE: %v", err)
	}
	rs, _ := e.Query(context.Background(), "SELECT name FROM users WHERE id = 1")
	if got := strColumn(t, rs, "name"); got[0] != "ALICE" {
		t.Fatalf("name = %q, want ALICE", got[0])
	}
}

func TestTxUpdateSetArithmetic(t *testing.T) {
	e := newExecutor(t)
	tx := e.Begin()
	if _, err := tx.Exec(context.Background(), "UPDATE users SET age = age + 1 WHERE id = 2"); err != nil {
		t.Fatalf("tx UPDATE: %v", err)
	}
	rs, err := tx.Query(context.Background(), "SELECT age FROM users WHERE id = 2")
	if err != nil {
		t.Fatalf("tx Query: %v", err)
	}
	if got := intColumn(t, rs, "age"); got[0] != 26 {
		t.Fatalf("staged age = %v, want 26", got[0])
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestArithmeticOnTextFails(t *testing.T) {
	e := newExecutor(t)
	_, err := e.Query(context.Background(), "SELECT name + 1 AS v FROM users WHERE id = 1")
	if err == nil {
		t.Fatal("expected type error for text arithmetic, got nil")
	}
	if !strings.Contains(err.Error(), "numeric") {
		t.Fatalf("error = %v, want mention of numeric operands", err)
	}
}

func TestArithmeticNullPropagates(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT age + NULL AS v FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if _, ok := rs.Rows[0][0].(types.NullKey); !ok {
		t.Fatalf("age + NULL = %T(%v), want NullKey", rs.Rows[0][0], rs.Rows[0][0])
	}
}

func TestExpressionDefaultOutputName(t *testing.T) {
	e := newExecutor(t)
	rs, err := e.Query(context.Background(), "SELECT age + 1 FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rs.Columns) != 1 || rs.Columns[0] == "" {
		t.Fatalf("expression projection needs a non-empty default column name, got %v", rs.Columns)
	}
	if v, ok := rs.Rows[0][0].(types.IntKey); !ok || int64(v) != 31 {
		t.Fatalf("value = %v, want 31", rs.Rows[0][0])
	}
}
