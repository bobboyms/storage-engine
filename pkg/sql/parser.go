package sql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrParse is the sentinel wrapped by all parser errors.
var ErrParse = errors.New("sql: parse error")

// Parse lexes and parses a single SQL statement. The trailing semicolon is
// optional. Errors wrap ErrParse (and may wrap ErrLex for lexical failures).
func Parse(input string) (Statement, error) {
	stmt, _, err := parseCounting(input)
	return stmt, err
}

// parseCounting parses a statement and also reports how many "?" placeholders
// it contains, so callers binding arguments can validate the count.
func parseCounting(input string) (Statement, int, error) {
	toks, err := Lex(input)
	if err != nil {
		return nil, 0, err
	}
	p := &parser{toks: toks}
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, 0, err
	}
	// Allow a single trailing semicolon.
	if p.peek().Type == TokenSemicolon {
		p.next()
	}
	if p.peek().Type != TokenEOF {
		return nil, 0, fmt.Errorf("%w: unexpected token %q after statement", ErrParse, p.peek().Literal)
	}
	return stmt, p.params, nil
}

type parser struct {
	toks   []Token
	pos    int
	params int // number of "?" placeholders seen so far, used to assign ordinals
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

func (p *parser) expectKeywords(kws ...string) error {
	for _, kw := range kws {
		if err := p.expectKeyword(kw); err != nil {
			return err
		}
	}
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
	case "CREATE":
		return p.parseCreateTable()
	case "ALTER":
		return p.parseAlterTable()
	case "DESCRIBE", "DESC":
		return p.parseDescribe()
	default:
		return nil, fmt.Errorf("%w: unsupported statement %q", ErrParse, t.Literal)
	}
}

func (p *parser) parseCreateTable() (*CreateTableStmt, error) {
	if err := p.expectKeyword("CREATE"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	ifNotExists := false
	if p.isKeyword("IF") {
		p.next()
		if err := p.expectKeyword("NOT"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifNotExists = true
	}
	if p.peek().Type != TokenIdent {
		return nil, fmt.Errorf("%w: expected table name, got %q", ErrParse, p.peek().Literal)
	}
	stmt := &CreateTableStmt{Table: p.next().Literal, IfNotExists: ifNotExists}

	if p.peek().Type != TokenLParen {
		return nil, fmt.Errorf("%w: expected ( before column definitions, got %q", ErrParse, p.peek().Literal)
	}
	p.next()

	for {
		if p.isKeyword("INDEX") {
			idx, err := p.parseTableIndex()
			if err != nil {
				return nil, err
			}
			stmt.Indexes = append(stmt.Indexes, idx)
		} else {
			col, err := p.parseColumnDef()
			if err != nil {
				return nil, err
			}
			stmt.Columns = append(stmt.Columns, col)
		}
		if p.peek().Type != TokenComma {
			break
		}
		p.next()
	}

	if p.peek().Type != TokenRParen {
		return nil, fmt.Errorf("%w: expected ) after column definitions, got %q", ErrParse, p.peek().Literal)
	}
	p.next()
	return stmt, nil
}

// parseTableIndex parses a table-level "INDEX (col, col, ...)" clause.
func (p *parser) parseTableIndex() (IndexClause, error) {
	if err := p.expectKeyword("INDEX"); err != nil {
		return IndexClause{}, err
	}
	cols, err := p.parseParenColumnList()
	if err != nil {
		return IndexClause{}, err
	}
	if len(cols) == 0 {
		return IndexClause{}, fmt.Errorf("%w: INDEX requires at least one column", ErrParse)
	}
	return IndexClause{Columns: cols}, nil
}

func (p *parser) parseAlterTable() (*AlterTableStmt, error) {
	if err := p.expectKeyword("ALTER"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	if p.peek().Type != TokenIdent {
		return nil, fmt.Errorf("%w: expected table name, got %q", ErrParse, p.peek().Literal)
	}
	stmt := &AlterTableStmt{Table: p.next().Literal}

	switch {
	case p.isKeyword("ADD"):
		p.next()
		if p.isKeyword("COLUMN") {
			p.next()
		}
		if p.isKeyword("IF") {
			if err := p.expectKeywords("IF", "NOT", "EXISTS"); err != nil {
				return nil, err
			}
			stmt.IfExists = true
		}
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		stmt.Column = col
	case p.isKeyword("DROP"):
		p.next()
		if p.isKeyword("COLUMN") {
			p.next()
		}
		if p.isKeyword("IF") {
			if err := p.expectKeywords("IF", "EXISTS"); err != nil {
				return nil, err
			}
			stmt.IfExists = true
		}
		if p.peek().Type != TokenIdent {
			return nil, fmt.Errorf("%w: expected column name, got %q", ErrParse, p.peek().Literal)
		}
		stmt.Drop = true
		stmt.Column = ColumnDef{Name: p.next().Literal}
	default:
		return nil, fmt.Errorf("%w: expected ADD or DROP, got %q", ErrParse, p.peek().Literal)
	}
	return stmt, nil
}

func (p *parser) parseDescribe() (*DescribeStmt, error) {
	// The dispatching keyword is DESCRIBE or its DESC alias.
	p.next()
	if p.peek().Type != TokenIdent {
		return nil, fmt.Errorf("%w: expected table name, got %q", ErrParse, p.peek().Literal)
	}
	return &DescribeStmt{Table: p.next().Literal}, nil
}

func (p *parser) parseColumnDef() (ColumnDef, error) {
	if p.peek().Type != TokenIdent {
		return ColumnDef{}, fmt.Errorf("%w: expected column name, got %q", ErrParse, p.peek().Literal)
	}
	col := ColumnDef{Name: p.next().Literal}

	if p.peek().Type != TokenIdent {
		return ColumnDef{}, fmt.Errorf("%w: expected a type for column %q, got %q", ErrParse, col.Name, p.peek().Literal)
	}
	typeName := p.next().Literal
	dt, ok := dataTypeForName(typeName)
	if !ok {
		return ColumnDef{}, fmt.Errorf("%w: unknown type %q for column %q", ErrParse, typeName, col.Name)
	}
	col.Type = dt

	// Optional column constraints: PRIMARY KEY and/or INDEX.
	for {
		switch {
		case p.isKeyword("PRIMARY"):
			p.next()
			if err := p.expectKeyword("KEY"); err != nil {
				return ColumnDef{}, err
			}
			col.Primary = true
		case p.isKeyword("INDEX"):
			p.next()
			col.Index = true
		default:
			return col, nil
		}
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
	switch operand.(type) {
	case *Literal, *Placeholder:
		return operand, nil
	default:
		return nil, fmt.Errorf("%w: expected a literal value, got %s", ErrParse, operand.String())
	}
}

func (p *parser) parseSelect() (*SelectStmt, error) {
	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}
	sel := &SelectStmt{}

	items, err := p.parseSelectItems()
	if err != nil {
		return nil, err
	}
	sel.Items = items

	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	table, sub, alias, err := p.parseFromSource()
	if err != nil {
		return nil, err
	}
	sel.Table, sel.Subquery, sel.Alias = table, sub, alias

	joins, err := p.parseJoins()
	if err != nil {
		return nil, err
	}
	sel.Joins = joins

	if p.isKeyword("WHERE") {
		p.next()
		where, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		sel.Where = where
	}

	if p.isKeyword("GROUP") {
		cols, err := p.parseGroupBy()
		if err != nil {
			return nil, err
		}
		sel.GroupBy = cols
	}

	if p.isKeyword("HAVING") {
		p.next()
		having, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		sel.Having = having
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

var aggregateFuncs = map[string]struct{}{
	"COUNT": {}, "SUM": {}, "AVG": {}, "MIN": {}, "MAX": {},
}

// parseColumnRef parses a column reference, optionally qualified by a table
// name or alias: "col" or "alias.col".
func (p *parser) parseColumnRef() (*ColumnRef, error) {
	if p.peek().Type != TokenIdent {
		return nil, fmt.Errorf("%w: expected column name, got %q", ErrParse, p.peek().Literal)
	}
	first := p.next().Literal
	if p.peek().Type != TokenDot {
		return &ColumnRef{Name: first}, nil
	}
	p.next() // consume "."
	if p.peek().Type != TokenIdent {
		return nil, fmt.Errorf("%w: expected column name after %q., got %q", ErrParse, first, p.peek().Literal)
	}
	return &ColumnRef{Qualifier: first, Name: p.next().Literal}, nil
}

func (p *parser) parseSelectItems() ([]SelectItem, error) {
	var items []SelectItem
	for {
		item, err := p.parseSelectItem()
		if err != nil {
			return nil, err
		}
		items = append(items, item)
		if p.peek().Type != TokenComma {
			break
		}
		p.next()
	}
	return items, nil
}

func (p *parser) parseSelectItem() (SelectItem, error) {
	if p.peek().Type == TokenStar {
		p.next()
		return SelectItem{Star: true}, nil
	}

	// Aggregate call: an identifier immediately followed by "(".
	if p.peek().Type == TokenIdent && p.toks[p.pos+1].Type == TokenLParen {
		if _, ok := aggregateFuncs[strings.ToUpper(p.peek().Literal)]; ok {
			agg, err := p.parseAggregate()
			if err != nil {
				return SelectItem{}, err
			}
			return SelectItem{Agg: agg, Alias: p.parseOptionalAlias()}, nil
		}
	}

	if p.peek().Type != TokenIdent {
		return SelectItem{}, fmt.Errorf("%w: expected a column or aggregate, got %q", ErrParse, p.peek().Literal)
	}
	col, err := p.parseColumnRef()
	if err != nil {
		return SelectItem{}, err
	}
	return SelectItem{Column: col, Alias: p.parseOptionalAlias()}, nil
}

func (p *parser) parseAggregate() (*AggregateCall, error) {
	fn := strings.ToUpper(p.next().Literal)
	p.next() // consume "("
	agg := &AggregateCall{Func: fn}

	if p.peek().Type == TokenStar {
		p.next()
		if fn != "COUNT" {
			return nil, fmt.Errorf("%w: %s(*) is not allowed; only COUNT(*)", ErrParse, fn)
		}
		agg.Star = true
	} else {
		if p.isKeyword("DISTINCT") {
			p.next()
			agg.Distinct = true
		}
		col, err := p.parseColumnRef()
		if err != nil {
			return nil, err
		}
		agg.Column = col
	}

	if p.peek().Type != TokenRParen {
		return nil, fmt.Errorf("%w: expected ) to close %s(, got %q", ErrParse, fn, p.peek().Literal)
	}
	p.next()
	return agg, nil
}

// parseOptionalAlias consumes an optional "AS name" or a bare identifier alias.
func (p *parser) parseOptionalAlias() string {
	if p.isKeyword("AS") {
		p.next()
		if p.peek().Type == TokenIdent {
			return p.next().Literal
		}
		return ""
	}
	if p.peek().Type == TokenIdent {
		return p.next().Literal
	}
	return ""
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

// parseFromSource parses a FROM/JOIN source: a table name (with optional alias)
// or a parenthesized subquery (which requires an alias). It returns the table
// name, the derived subquery (nil for a table), and the alias.
func (p *parser) parseFromSource() (string, *SelectStmt, string, error) {
	if p.peek().Type == TokenLParen {
		p.next()
		sub, err := p.parseSelect()
		if err != nil {
			return "", nil, "", err
		}
		if p.peek().Type != TokenRParen {
			return "", nil, "", fmt.Errorf("%w: expected ) to close subquery, got %q", ErrParse, p.peek().Literal)
		}
		p.next()
		alias := p.parseOptionalAlias()
		if alias == "" {
			return "", nil, "", fmt.Errorf("%w: derived table in FROM requires an alias", ErrParse)
		}
		return "", sub, alias, nil
	}
	if p.peek().Type != TokenIdent {
		return "", nil, "", fmt.Errorf("%w: expected table name, got %q", ErrParse, p.peek().Literal)
	}
	table := p.next().Literal
	alias := p.parseOptionalAlias()
	if alias == "" {
		alias = table
	}
	return table, nil, alias, nil
}

// parseJoins parses zero or more JOIN clauses: [INNER | LEFT [OUTER]] JOIN
// source [AS] alias ON <predicate>.
func (p *parser) parseJoins() ([]JoinClause, error) {
	var joins []JoinClause
	for {
		left := false
		switch {
		case p.isKeyword("INNER"):
			p.next()
		case p.isKeyword("LEFT"):
			p.next()
			if p.isKeyword("OUTER") {
				p.next()
			}
			left = true
		case p.isKeyword("JOIN"):
			// bare JOIN == INNER
		default:
			return joins, nil
		}
		if err := p.expectKeyword("JOIN"); err != nil {
			return nil, err
		}
		table, sub, alias, err := p.parseFromSource()
		if err != nil {
			return nil, err
		}
		jc := JoinClause{Table: table, Subquery: sub, Alias: alias, Left: left}
		if err := p.expectKeyword("ON"); err != nil {
			return nil, err
		}
		on, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		jc.On = on
		joins = append(joins, jc)
	}
}

func (p *parser) parseGroupBy() ([]string, error) {
	if err := p.expectKeyword("GROUP"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("BY"); err != nil {
		return nil, err
	}
	var cols []string
	for {
		col, err := p.parseColumnRef()
		if err != nil {
			return nil, err
		}
		cols = append(cols, col.String())
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
	col, err := p.parseColumnRef()
	if err != nil {
		return nil, err
	}
	ob := &OrderBy{Column: col.String()}
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
	if p.isKeyword("EXISTS") {
		return p.parseExists(false)
	}
	if p.isKeyword("NOT") && p.toks[p.pos+1].Type == TokenKeyword && p.toks[p.pos+1].Literal == "EXISTS" {
		p.next() // NOT
		return p.parseExists(true)
	}
	// A "(" introduces either a grouped boolean expression or, when followed
	// by SELECT, a scalar subquery used as a comparison operand.
	if p.peek().Type == TokenLParen && !p.lparenStartsSubquery() {
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

// lparenStartsSubquery reports whether the upcoming "(" begins a SELECT.
func (p *parser) lparenStartsSubquery() bool {
	return p.peek().Type == TokenLParen &&
		p.toks[p.pos+1].Type == TokenKeyword && p.toks[p.pos+1].Literal == "SELECT"
}

func (p *parser) parseExists(negate bool) (Expr, error) {
	if err := p.expectKeyword("EXISTS"); err != nil {
		return nil, err
	}
	sub, err := p.parseParenSelect()
	if err != nil {
		return nil, err
	}
	return &ExistsExpr{Select: sub, Negate: negate}, nil
}

// parseParenSelect parses "( SELECT ... )".
func (p *parser) parseParenSelect() (*SelectStmt, error) {
	if p.peek().Type != TokenLParen {
		return nil, fmt.Errorf("%w: expected ( before subquery, got %q", ErrParse, p.peek().Literal)
	}
	p.next()
	sub, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	if p.peek().Type != TokenRParen {
		return nil, fmt.Errorf("%w: expected ) to close subquery, got %q", ErrParse, p.peek().Literal)
	}
	p.next()
	return sub, nil
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
	if p.isKeyword("IN") {
		return p.parseIn(left, false)
	}
	if p.isKeyword("LIKE") {
		p.next()
		return p.parseLike(left, false)
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
	if p.isKeyword("IN") {
		return p.parseIn(left, true)
	}
	if p.isKeyword("LIKE") {
		p.next()
		return p.parseLike(left, true)
	}
	return nil, fmt.Errorf("%w: expected BETWEEN, IN, or LIKE after NOT, got %q", ErrParse, p.peek().Literal)
}

// parseLike parses "left [NOT] LIKE pattern". The LIKE keyword has already been
// consumed by the caller. It produces a BinaryExpr whose operator the evaluator
// recognizes (LIKE / NOT LIKE).
func (p *parser) parseLike(left Expr, negate bool) (Expr, error) {
	pattern, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	op := "LIKE"
	if negate {
		op = "NOT LIKE"
	}
	return &BinaryExpr{Op: op, Left: left, Right: pattern}, nil
}

// parseIn desugars "left [NOT] IN (v1, v2, ...)" into a chain of equality
// comparisons joined by OR (or inequalities joined by AND for NOT IN), so
// evaluation needs no special case. The value list must be non-empty.
func (p *parser) parseIn(left Expr, negate bool) (Expr, error) {
	p.next() // consume IN
	if p.peek().Type != TokenLParen {
		return nil, fmt.Errorf("%w: expected ( after IN, got %q", ErrParse, p.peek().Literal)
	}
	// IN (SELECT ...) — subquery membership.
	if p.toks[p.pos+1].Type == TokenKeyword && p.toks[p.pos+1].Literal == "SELECT" {
		sub, err := p.parseParenSelect()
		if err != nil {
			return nil, err
		}
		return &InSubqueryExpr{Operand: left, Select: sub, Negate: negate}, nil
	}
	p.next()

	var items []Expr
	for {
		item, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		items = append(items, item)
		if p.peek().Type != TokenComma {
			break
		}
		p.next()
	}
	if p.peek().Type != TokenRParen {
		return nil, fmt.Errorf("%w: expected ) after IN list, got %q", ErrParse, p.peek().Literal)
	}
	p.next()

	cmpOp, joinOp := "=", "OR"
	if negate {
		cmpOp, joinOp = "<>", "AND"
	}
	expr := Expr(&BinaryExpr{Op: cmpOp, Left: left, Right: items[0]})
	for _, item := range items[1:] {
		expr = &BinaryExpr{
			Op:    joinOp,
			Left:  expr,
			Right: &BinaryExpr{Op: cmpOp, Left: left, Right: item},
		}
	}
	return expr, nil
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
	if t.Type == TokenLParen && p.lparenStartsSubquery() {
		sub, err := p.parseParenSelect()
		if err != nil {
			return nil, err
		}
		return &ScalarSubquery{Select: sub}, nil
	}
	switch t.Type {
	case TokenIdent:
		// Aggregate call used as an operand (e.g. in HAVING): COUNT(*) > 1.
		if p.toks[p.pos+1].Type == TokenLParen {
			if _, ok := aggregateFuncs[strings.ToUpper(t.Literal)]; ok {
				agg, err := p.parseAggregate()
				if err != nil {
					return nil, err
				}
				return &AggregateExpr{Call: agg}, nil
			}
		}
		return p.parseColumnRef()
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
	case TokenPlaceholder:
		p.next()
		ord := p.params
		p.params++
		return &Placeholder{Ordinal: ord}, nil
	case TokenKeyword:
		if t.Literal == "NULL" {
			p.next()
			return &Literal{Kind: LitNull}, nil
		}
	}
	return nil, fmt.Errorf("%w: expected column or literal, got %q", ErrParse, t.Literal)
}
