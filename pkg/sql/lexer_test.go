package sql

import (
	"errors"
	"testing"
)

func tokenTypes(toks []Token) []TokenType {
	out := make([]TokenType, len(toks))
	for i, t := range toks {
		out[i] = t.Type
	}
	return out
}

func TestLexSelectStatement(t *testing.T) {
	toks, err := Lex("SELECT id, name FROM users WHERE age >= 18;")
	if err != nil {
		t.Fatalf("Lex error: %v", err)
	}

	want := []TokenType{
		TokenKeyword, // SELECT
		TokenIdent,   // id
		TokenComma,
		TokenIdent,    // name
		TokenKeyword,  // FROM
		TokenIdent,    // users
		TokenKeyword,  // WHERE
		TokenIdent,    // age
		TokenOperator, // >=
		TokenInt,      // 18
		TokenSemicolon,
		TokenEOF,
	}
	got := tokenTypes(toks)
	if len(got) != len(want) {
		t.Fatalf("token count = %d (%v), want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("token[%d] type = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestLexKeywordsAreCaseInsensitiveAndCanonical(t *testing.T) {
	toks, err := Lex("select")
	if err != nil {
		t.Fatalf("Lex error: %v", err)
	}
	if toks[0].Type != TokenKeyword {
		t.Fatalf("token type = %v, want keyword", toks[0].Type)
	}
	if toks[0].Literal != "SELECT" {
		t.Fatalf("keyword literal = %q, want canonical SELECT", toks[0].Literal)
	}
}

func TestLexStringLiteralWithEscapedQuote(t *testing.T) {
	toks, err := Lex("'O''Brien'")
	if err != nil {
		t.Fatalf("Lex error: %v", err)
	}
	if toks[0].Type != TokenString {
		t.Fatalf("token type = %v, want string", toks[0].Type)
	}
	if toks[0].Literal != "O'Brien" {
		t.Fatalf("string literal = %q, want O'Brien", toks[0].Literal)
	}
}

func TestLexNumbers(t *testing.T) {
	toks, err := Lex("42 3.14")
	if err != nil {
		t.Fatalf("Lex error: %v", err)
	}
	if toks[0].Type != TokenInt || toks[0].Literal != "42" {
		t.Fatalf("token[0] = {%v %q}, want int 42", toks[0].Type, toks[0].Literal)
	}
	if toks[1].Type != TokenFloat || toks[1].Literal != "3.14" {
		t.Fatalf("token[1] = {%v %q}, want float 3.14", toks[1].Type, toks[1].Literal)
	}
}

func TestLexOperators(t *testing.T) {
	toks, err := Lex("= <> != < <= > >=")
	if err != nil {
		t.Fatalf("Lex error: %v", err)
	}
	want := []string{"=", "<>", "!=", "<", "<=", ">", ">="}
	for i, w := range want {
		if toks[i].Type != TokenOperator {
			t.Fatalf("token[%d] type = %v, want operator", i, toks[i].Type)
		}
		if toks[i].Literal != w {
			t.Fatalf("token[%d] literal = %q, want %q", i, toks[i].Literal, w)
		}
	}
}

func TestLexBoolAndNull(t *testing.T) {
	toks, err := Lex("TRUE false NULL")
	if err != nil {
		t.Fatalf("Lex error: %v", err)
	}
	if toks[0].Type != TokenBool || toks[0].Literal != "true" {
		t.Fatalf("token[0] = {%v %q}, want bool true", toks[0].Type, toks[0].Literal)
	}
	if toks[1].Type != TokenBool || toks[1].Literal != "false" {
		t.Fatalf("token[1] = {%v %q}, want bool false", toks[1].Type, toks[1].Literal)
	}
	if toks[2].Type != TokenKeyword || toks[2].Literal != "NULL" {
		t.Fatalf("token[2] = {%v %q}, want keyword NULL", toks[2].Type, toks[2].Literal)
	}
}

func TestLexUnterminatedString(t *testing.T) {
	_, err := Lex("'abc")
	if !errors.Is(err, ErrLex) {
		t.Fatalf("Lex error = %v, want ErrLex", err)
	}
}

func TestLexUnexpectedCharacter(t *testing.T) {
	_, err := Lex("SELECT @")
	if !errors.Is(err, ErrLex) {
		t.Fatalf("Lex error = %v, want ErrLex", err)
	}
}
