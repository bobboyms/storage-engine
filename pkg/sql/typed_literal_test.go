package sql

import (
	"errors"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// TestParseTypedLiterals verifies that DATE/DECIMAL/UUID prefixed string
// literals parse into typed Literal nodes with the expected canonical form.
func TestParseTypedLiterals(t *testing.T) {
	sel := parseSelect(t, "SELECT * FROM t WHERE d = DATE '2024-01-15' AND n = DECIMAL '1.50' AND u = UUID '00000000-0000-0000-0000-000000000001'")
	want := "(((d = DATE '2024-01-15') AND (n = DECIMAL '1.50')) AND (u = UUID '00000000-0000-0000-0000-000000000001'))"
	if got := sel.Where.String(); got != want {
		t.Fatalf("Where = %q, want %q", got, want)
	}
}

// TestParseTypedLiteralInvalid rejects malformed typed literals at parse time.
func TestParseTypedLiteralInvalid(t *testing.T) {
	for _, in := range []string{
		"SELECT * FROM t WHERE d = DATE '2024-13-40'",
		"SELECT * FROM t WHERE n = DECIMAL 'abc'",
		"SELECT * FROM t WHERE u = UUID 'not-a-uuid'",
	} {
		if _, err := Parse(in); !errors.Is(err, ErrParse) {
			t.Fatalf("Parse(%q) error = %v, want ErrParse", in, err)
		}
	}
}

// TestTypedLiteralColumnValue converts typed literals to the matching engine
// key type for DATEONLY, DECIMAL, and UUID columns.
func TestTypedLiteralColumnValue(t *testing.T) {
	day, _ := types.NewDateOnly(2024, 1, 15)
	dec, _ := types.ParseDecimal("1.50")
	uid, _ := types.ParseUUID("00000000-0000-0000-0000-000000000001")

	cases := []struct {
		lit  *Literal
		dt   storage.DataType
		want types.Comparable
	}{
		{&Literal{Kind: LitDate, Date: day}, storage.TypeDateOnly, day},
		{&Literal{Kind: LitDecimal, Dec: dec}, storage.TypeDecimal, dec},
		{&Literal{Kind: LitUUID, UUID: uid}, storage.TypeUUID, uid},
	}
	for _, c := range cases {
		got, err := ColumnValue(c.lit, c.dt)
		if err != nil {
			t.Fatalf("ColumnValue(%s, %s): %v", c.lit, c.dt, err)
		}
		cmp, err := got.Compare(c.want)
		if err != nil || cmp != 0 {
			t.Fatalf("ColumnValue(%s) = %v, want %v (cmp=%d err=%v)", c.lit, got, c.want, cmp, err)
		}
	}
}

// TestTypedLiteralEvaluate exercises the comparison path: a typed literal is
// coerced to the concrete type of the column value it is compared against.
func TestTypedLiteralEvaluate(t *testing.T) {
	day, _ := types.NewDateOnly(2024, 1, 15)
	dec, _ := types.ParseDecimal("1.50")
	row := Row{
		"d": day,
		"n": dec,
	}
	cases := []struct {
		sql  string
		want bool
	}{
		{"SELECT * FROM t WHERE d = DATE '2024-01-15'", true},
		{"SELECT * FROM t WHERE d > DATE '2024-01-16'", false},
		{"SELECT * FROM t WHERE n = DECIMAL '1.50'", true},
		{"SELECT * FROM t WHERE n < DECIMAL '1.50'", false},
	}
	for _, c := range cases {
		sel := parseSelect(t, c.sql)
		got, err := Evaluate(sel.Where, row)
		if err != nil {
			t.Fatalf("Evaluate(%q): %v", c.sql, err)
		}
		if got != c.want {
			t.Fatalf("Evaluate(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}

// TestTypedLiteralNaturalValue checks the natural (unhinted) Comparable type of
// each typed literal.
func TestTypedLiteralNaturalValue(t *testing.T) {
	dec, _ := types.ParseDecimal("2.25")
	v, err := LiteralValue(&Literal{Kind: LitDecimal, Dec: dec}, nil)
	if err != nil {
		t.Fatalf("LiteralValue: %v", err)
	}
	if _, ok := v.(types.DecimalKey); !ok {
		t.Fatalf("natural decimal = %T, want types.DecimalKey", v)
	}
}
