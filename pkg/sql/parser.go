package sql

import (
	"errors"
	"fmt"
	"strconv"
)

// ErrParse is the sentinel wrapped by all parser errors.
var ErrParse = errors.New("sql: parse error")

// Parse lexes and parses a single SQL statement. The trailing semicolon is
// optional. Errors wrap ErrParse (and may wrap ErrLex for lexical failures).
func Parse(input string) (Statement, error) {
	toks, err := Lex(input)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	// Allow a single trailing semicolon.
	if p.peek().Type == TokenSemicolon {
		p.next()
	}
	if p.peek().Type != TokenEOF {
		return nil, fmt.Errorf("%w: unexpected token %q after statement", ErrParse, p.peek().Literal)
	}
	return stmt, nil
}

type parser struct {
	toks []Token
	pos  int
}

func (p *parser) peek() Token { return p.toks[p.pos] }

func (p *parser) next() Token {
	t := p.toks[p.pos]
	if p.pos < len(p.toks)-1 {
		p.pos++
	}
	return t
}

func (p *parser) expectKeyword(kw string) error {
	t := p.peek()
	if t.Type != TokenKeyword || t.Literal != kw {
		return fmt.Errorf("%w: expected %s, got %q", ErrParse, kw, t.Literal)
	}
	p.next()
	return nil
}

func (p *parser) isKeyword(kw string) bool {
	t := p.peek()
	return t.Type == TokenKeyword && t.Literal == kw
}

func (p *parser) parseStatement() (Statement, error) {
	t := p.peek()
	if t.Type != TokenKeyword {
		return nil, fmt.Errorf("%w: expected a statement keyword, got %q", ErrParse, t.Literal)
	}
	switch t.Literal {
	case "SELECT":
		return p.parseSelect()
	case "INSERT":
		return p.parseInsert()
	case "UPDATE":
		return p.parseUpdate()
	case "DELETE":
		return p.parseDelete()
	default:
		return nil, fmt.Errorf("%w: unsupported statement %q", ErrParse, t.Literal)
	}
}

func (p *parser) parseInsert() (*InsertStmt, error) {
	if err := p.expectKeyword("INSERT"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("INTO"); err != nil {
		return nil, err
	}
	if p.peek().Type != TokenIdent {
		return nil, fmt.Errorf("%w: expected table name, got %q", ErrParse, p.peek().Literal)
	}
	ins := &InsertStmt{Table: p.next().Literal}

	cols, err := p.parseParenColumnList()
	if err != nil {
		return nil, err
	}
	ins.Columns = cols

	if err := p.expectKeyword("VALUES"); err != nil {
		return nil, err
	}
	vals, err := p.parseParenLiteralList()
	if err != nil {
		return nil, err
	}
	ins.Values = vals
	return ins, nil
}

func (p *parser) parseUpdate() (*UpdateStmt, error) {
	if err := p.expectKeyword("UPDATE"); err != nil {
		return nil, err
	}
	if p.peek().Type != TokenIdent {
		return nil, fmt.Errorf("%w: expected table name, got %q", ErrParse, p.peek().Literal)
	}
	upd := &UpdateStmt{Table: p.next().Literal}

	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}
	for {
		if p.peek().Type != TokenIdent {
			return nil, fmt.Errorf("%w: expected column in SET, got %q", ErrParse, p.peek().Literal)
		}
		col := p.next().Literal
		if t := p.peek(); t.Type != TokenOperator || t.Literal != "=" {
			return nil, fmt.Errorf("%w: expected = in assignment, got %q", ErrParse, t.Literal)
		}
		p.next()
		val, err := p.parseLiteralOperand()
		if err != nil {
			return nil, err
		}
		upd.Assignments = append(upd.Assignments, Assignment{Column: col, Value: val})
		if p.peek().Type != TokenComma {
			break
		}
		p.next()
	}

	if p.isKeyword("WHERE") {
		p.next()
		where, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		upd.Where = where
	}
	return upd, nil
}

func (p *parser) parseDelete() (*DeleteStmt, error) {
	if err := p.expectKeyword("DELETE"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	if p.peek().Type != TokenIdent {
		return nil, fmt.Errorf("%w: expected table name, got %q", ErrParse, p.peek().Literal)
	}
	del := &DeleteStmt{Table: p.next().Literal}

	if p.isKeyword("WHERE") {
		p.next()
		where, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		del.Where = where
	}
	return del, nil
}

func (p *parser) parseParenColumnList() ([]string, error) {
	if p.peek().Type != TokenLParen {
		return nil, fmt.Errorf("%w: expected ( before column list, got %q", ErrParse, p.peek().Literal)
	}
	p.next()
	cols, err := p.parseColumnList()
	if err != nil {
		return nil, err
	}
	if p.peek().Type != TokenRParen {
		return nil, fmt.Errorf("%w: expected ) after column list, got %q", ErrParse, p.peek().Literal)
	}
	p.next()
	return cols, nil
}

func (p *parser) parseParenLiteralList() ([]Expr, error) {
	if p.peek().Type != TokenLParen {
		return nil, fmt.Errorf("%w: expected ( before VALUES list, got %q", ErrParse, p.peek().Literal)
	}
	p.next()
	var vals []Expr
	for {
		lit, err := p.parseLiteralOperand()
		if err != nil {
			return nil, err
		}
		vals = append(vals, lit)
		if p.peek().Type != TokenComma {
			break
		}
		p.next()
	}
	if p.peek().Type != TokenRParen {
		return nil, fmt.Errorf("%w: expected ) after VALUES list, got %q", ErrParse, p.peek().Literal)
	}
	p.next()
	return vals, nil
}

func (p *parser) parseLiteralOperand() (Expr, error) {
	operand, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	if _, ok := operand.(*Literal); !ok {
		return nil, fmt.Errorf("%w: expected a literal value, got %s", ErrParse, operand.String())
	}
	return operand, nil
}

func (p *parser) parseSelect() (*SelectStmt, error) {
	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}
	sel := &SelectStmt{}

	if p.peek().Type == TokenStar {
		p.next()
		sel.Star = true
	} else {
		cols, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		sel.Columns = cols
	}

	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	if p.peek().Type != TokenIdent {
		return nil, fmt.Errorf("%w: expected table name, got %q", ErrParse, p.peek().Literal)
	}
	sel.Table = p.next().Literal

	if p.isKeyword("WHERE") {
		p.next()
		where, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		sel.Where = where
	}

	if p.isKeyword("ORDER") {
		ob, err := p.parseOrderBy()
		if err != nil {
			return nil, err
		}
		sel.OrderBy = ob
	}

	if p.isKeyword("LIMIT") {
		p.next()
		n, err := p.parseIntLiteralValue()
		if err != nil {
			return nil, err
		}
		sel.Limit = &n
	}

	if p.isKeyword("OFFSET") {
		p.next()
		n, err := p.parseIntLiteralValue()
		if err != nil {
			return nil, err
		}
		sel.Offset = &n
	}

	if p.isKeyword("FOR") {
		p.next()
		if err := p.expectKeyword("UPDATE"); err != nil {
			return nil, err
		}
		sel.ForUpdate = true
	}

	return sel, nil
}

func (p *parser) parseColumnList() ([]string, error) {
	var cols []string
	for {
		if p.peek().Type != TokenIdent {
			return nil, fmt.Errorf("%w: expected column name, got %q", ErrParse, p.peek().Literal)
		}
		cols = append(cols, p.next().Literal)
		if p.peek().Type != TokenComma {
			break
		}
		p.next()
	}
	return cols, nil
}

func (p *parser) parseOrderBy() (*OrderBy, error) {
	if err := p.expectKeyword("ORDER"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("BY"); err != nil {
		return nil, err
	}
	if p.peek().Type != TokenIdent {
		return nil, fmt.Errorf("%w: expected column in ORDER BY, got %q", ErrParse, p.peek().Literal)
	}
	ob := &OrderBy{Column: p.next().Literal}
	switch {
	case p.isKeyword("DESC"):
		p.next()
		ob.Desc = true
	case p.isKeyword("ASC"):
		p.next()
	}
	return ob, nil
}

func (p *parser) parseIntLiteralValue() (int64, error) {
	t := p.peek()
	if t.Type != TokenInt {
		return 0, fmt.Errorf("%w: expected integer, got %q", ErrParse, t.Literal)
	}
	p.next()
	n, err := strconv.ParseInt(t.Literal, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid integer %q", ErrParse, t.Literal)
	}
	return n, nil
}

// Expression grammar (lowest to highest precedence):
//
//	expr   := orExpr
//	orExpr := andExpr (OR andExpr)*
//	andExpr := factor (AND factor)*
//	factor := '(' orExpr ')' | comparison
//	comparison := operand OP operand
//	operand := column | literal
func (p *parser) parseExpr() (Expr, error) {
	return p.parseOr()
}

func (p *parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.isKeyword("OR") {
		p.next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Op: "OR", Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (Expr, error) {
	left, err := p.parseFactor()
	if err != nil {
		return nil, err
	}
	for p.isKeyword("AND") {
		p.next()
		right, err := p.parseFactor()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Op: "AND", Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) parseFactor() (Expr, error) {
	if p.peek().Type == TokenLParen {
		p.next()
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.peek().Type != TokenRParen {
			return nil, fmt.Errorf("%w: expected ), got %q", ErrParse, p.peek().Literal)
		}
		p.next()
		return inner, nil
	}
	return p.parsePredicate()
}

// parsePredicate parses a single predicate over a left operand: a comparison
// (operand OP operand) or an IS [NOT] NULL test.
func (p *parser) parsePredicate() (Expr, error) {
	left, err := p.parseOperand()
	if err != nil {
		return nil, err
	}

	if p.isKeyword("IS") {
		return p.parseIsNull(left)
	}

	if p.isKeyword("NOT") {
		p.next()
		return p.parseNegatedPredicate(left)
	}
	if p.isKeyword("BETWEEN") {
		return p.parseBetween(left, false)
	}

	if p.peek().Type != TokenOperator {
		return nil, fmt.Errorf("%w: expected comparison operator, got %q", ErrParse, p.peek().Literal)
	}
	op := p.next().Literal
	right, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	return &BinaryExpr{Op: op, Left: left, Right: right}, nil
}

// parseNegatedPredicate parses the predicate following a NOT keyword
// (NOT BETWEEN / NOT IN / NOT LIKE).
func (p *parser) parseNegatedPredicate(left Expr) (Expr, error) {
	if p.isKeyword("BETWEEN") {
		return p.parseBetween(left, true)
	}
	return nil, fmt.Errorf("%w: expected BETWEEN, IN, or LIKE after NOT, got %q", ErrParse, p.peek().Literal)
}

// parseBetween desugars "left [NOT] BETWEEN low AND high" into comparisons so
// evaluation and bound derivation need no special cases:
//
//	BETWEEN     -> (left >= low) AND (left <= high)
//	NOT BETWEEN -> (left < low) OR (left > high)
func (p *parser) parseBetween(left Expr, negate bool) (Expr, error) {
	p.next() // consume BETWEEN
	low, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("AND"); err != nil {
		return nil, err
	}
	high, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	if negate {
		return &BinaryExpr{
			Op:    "OR",
			Left:  &BinaryExpr{Op: "<", Left: left, Right: low},
			Right: &BinaryExpr{Op: ">", Left: left, Right: high},
		}, nil
	}
	return &BinaryExpr{
		Op:    "AND",
		Left:  &BinaryExpr{Op: ">=", Left: left, Right: low},
		Right: &BinaryExpr{Op: "<=", Left: left, Right: high},
	}, nil
}

func (p *parser) parseIsNull(left Expr) (Expr, error) {
	p.next() // consume IS
	negate := false
	if p.isKeyword("NOT") {
		p.next()
		negate = true
	}
	if err := p.expectKeyword("NULL"); err != nil {
		return nil, err
	}
	return &IsNullExpr{Operand: left, Negate: negate}, nil
}

func (p *parser) parseOperand() (Expr, error) {
	t := p.peek()
	switch t.Type {
	case TokenIdent:
		p.next()
		return &ColumnRef{Name: t.Literal}, nil
	case TokenInt:
		p.next()
		n, err := strconv.ParseInt(t.Literal, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid integer %q", ErrParse, t.Literal)
		}
		return &Literal{Kind: LitInt, Int: n}, nil
	case TokenFloat:
		p.next()
		f, err := strconv.ParseFloat(t.Literal, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid float %q", ErrParse, t.Literal)
		}
		return &Literal{Kind: LitFloat, Float: f}, nil
	case TokenString:
		p.next()
		return &Literal{Kind: LitString, Str: t.Literal}, nil
	case TokenBool:
		p.next()
		return &Literal{Kind: LitBool, Bool: t.Literal == "true"}, nil
	case TokenKeyword:
		if t.Literal == "NULL" {
			p.next()
			return &Literal{Kind: LitNull}, nil
		}
	}
	return nil, fmt.Errorf("%w: expected column or literal, got %q", ErrParse, t.Literal)
}
