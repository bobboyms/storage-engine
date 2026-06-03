package sql

import (
	"errors"
	"fmt"
	"strings"
)

// ErrLex is the sentinel wrapped by all lexer errors.
var ErrLex = errors.New("sql: lex error")

// TokenType enumerates the lexical token categories produced by Lex.
type TokenType int

const (
	// TokenEOF marks the end of the input. Lex always appends one.
	TokenEOF TokenType = iota
	// TokenIdent is an unquoted identifier (table or column name).
	TokenIdent
	// TokenKeyword is a reserved word; Literal holds its canonical uppercase form.
	TokenKeyword
	// TokenInt is an integer literal.
	TokenInt
	// TokenFloat is a floating-point literal.
	TokenFloat
	// TokenString is a single-quoted string literal with quotes/escapes resolved.
	TokenString
	// TokenBool is a boolean literal; Literal is "true" or "false".
	TokenBool
	// TokenOperator is a comparison operator (= <> != < <= > >=).
	TokenOperator
	// TokenComma is ",".
	TokenComma
	// TokenLParen is "(".
	TokenLParen
	// TokenRParen is ")".
	TokenRParen
	// TokenStar is "*".
	TokenStar
	// TokenSemicolon is ";".
	TokenSemicolon
)

// Token is a single lexical unit. Pos is the byte offset where it begins.
type Token struct {
	Type    TokenType
	Literal string
	Pos     int
}

var keywords = map[string]struct{}{
	"SELECT": {}, "FROM": {}, "WHERE": {}, "AND": {}, "OR": {},
	"ORDER": {}, "BY": {}, "ASC": {}, "DESC": {}, "LIMIT": {}, "OFFSET": {},
	"INSERT": {}, "INTO": {}, "VALUES": {}, "UPDATE": {}, "SET": {},
	"DELETE": {}, "NULL": {},
}

// Lex tokenizes input into a slice of tokens terminated by a TokenEOF token.
// It returns an error wrapping ErrLex on an unterminated string or an
// unexpected character.
func Lex(input string) ([]Token, error) {
	var toks []Token
	i := 0
	for i < len(input) {
		c := input[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == ',':
			toks = append(toks, Token{Type: TokenComma, Literal: ",", Pos: i})
			i++
		case c == '(':
			toks = append(toks, Token{Type: TokenLParen, Literal: "(", Pos: i})
			i++
		case c == ')':
			toks = append(toks, Token{Type: TokenRParen, Literal: ")", Pos: i})
			i++
		case c == '*':
			toks = append(toks, Token{Type: TokenStar, Literal: "*", Pos: i})
			i++
		case c == ';':
			toks = append(toks, Token{Type: TokenSemicolon, Literal: ";", Pos: i})
			i++
		case c == '=' || c == '<' || c == '>' || c == '!':
			tok, next, err := lexOperator(input, i)
			if err != nil {
				return nil, err
			}
			toks = append(toks, tok)
			i = next
		case c == '\'':
			tok, next, err := lexString(input, i)
			if err != nil {
				return nil, err
			}
			toks = append(toks, tok)
			i = next
		case isDigit(c):
			tok, next := lexNumber(input, i)
			toks = append(toks, tok)
			i = next
		case isIdentStart(c):
			tok, next := lexWord(input, i)
			toks = append(toks, tok)
			i = next
		default:
			return nil, fmt.Errorf("%w: unexpected character %q at position %d", ErrLex, string(c), i)
		}
	}
	toks = append(toks, Token{Type: TokenEOF, Pos: len(input)})
	return toks, nil
}

func lexOperator(input string, start int) (Token, int, error) {
	c := input[start]
	switch c {
	case '=':
		return Token{Type: TokenOperator, Literal: "=", Pos: start}, start + 1, nil
	case '<':
		if start+1 < len(input) && input[start+1] == '=' {
			return Token{Type: TokenOperator, Literal: "<=", Pos: start}, start + 2, nil
		}
		if start+1 < len(input) && input[start+1] == '>' {
			return Token{Type: TokenOperator, Literal: "<>", Pos: start}, start + 2, nil
		}
		return Token{Type: TokenOperator, Literal: "<", Pos: start}, start + 1, nil
	case '>':
		if start+1 < len(input) && input[start+1] == '=' {
			return Token{Type: TokenOperator, Literal: ">=", Pos: start}, start + 2, nil
		}
		return Token{Type: TokenOperator, Literal: ">", Pos: start}, start + 1, nil
	default: // '!'
		if start+1 < len(input) && input[start+1] == '=' {
			return Token{Type: TokenOperator, Literal: "!=", Pos: start}, start + 2, nil
		}
		return Token{}, start, fmt.Errorf("%w: unexpected character %q at position %d", ErrLex, "!", start)
	}
}

func lexString(input string, start int) (Token, int, error) {
	var sb strings.Builder
	i := start + 1
	for i < len(input) {
		if input[i] == '\'' {
			// Doubled quote is an escaped single quote.
			if i+1 < len(input) && input[i+1] == '\'' {
				sb.WriteByte('\'')
				i += 2
				continue
			}
			return Token{Type: TokenString, Literal: sb.String(), Pos: start}, i + 1, nil
		}
		sb.WriteByte(input[i])
		i++
	}
	return Token{}, start, fmt.Errorf("%w: unterminated string literal starting at position %d", ErrLex, start)
}

func lexNumber(input string, start int) (Token, int) {
	i := start
	isFloat := false
	for i < len(input) {
		c := input[i]
		if isDigit(c) {
			i++
			continue
		}
		if c == '.' && !isFloat {
			isFloat = true
			i++
			continue
		}
		break
	}
	typ := TokenInt
	if isFloat {
		typ = TokenFloat
	}
	return Token{Type: typ, Literal: input[start:i], Pos: start}, i
}

func lexWord(input string, start int) (Token, int) {
	i := start
	for i < len(input) && isIdentPart(input[i]) {
		i++
	}
	word := input[start:i]
	upper := strings.ToUpper(word)
	switch upper {
	case "TRUE":
		return Token{Type: TokenBool, Literal: "true", Pos: start}, i
	case "FALSE":
		return Token{Type: TokenBool, Literal: "false", Pos: start}, i
	}
	if _, ok := keywords[upper]; ok {
		return Token{Type: TokenKeyword, Literal: upper, Pos: start}, i
	}
	return Token{Type: TokenIdent, Literal: word, Pos: start}, i
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentPart(c byte) bool { return isIdentStart(c) || isDigit(c) }
