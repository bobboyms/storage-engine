package sql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/bobboyms/storage-engine/pkg/types"
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
		return p.parseSelectStatement()
	case "INSERT":
		return p.parseInsert()
	case "UPDATE":
		return p.parseUpdate()
	case "DELETE":
		return p.parseDelete()
	case "CREATE":
		// CREATE TABLE vs CREATE [UNIQUE] INDEX, decided by lookahead.
		if next := p.toks[p.pos+1]; next.Type == TokenKeyword && (next.Literal == "INDEX" || next.Literal == "UNIQUE") {
			return p.parseCreateIndex()
		}
		return p.parseCreateTable()
	case "ALTER":
		return p.parseAlterTable()
	case "DROP":
		if next := p.toks[p.pos+1]; next.Type == TokenKeyword && next.Literal == "INDEX" {
			return p.parseDropIndex()
		}
		return p.parseDropTable()
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
		switch {
		case p.isKeyword("INDEX") || p.isKeyword("UNIQUE"):
			idx, err := p.parseTableIndex()
			if err != nil {
				return nil, err
			}
			stmt.Indexes = append(stmt.Indexes, idx)
		case p.isKeyword("CHECK"):
			chk, err := p.parseCheckClause()
			if err != nil {
				return nil, err
			}
			stmt.Checks = append(stmt.Checks, chk)
		case p.isKeyword("FOREIGN"):
			fk, err := p.parseForeignKeyClause()
			if err != nil {
				return nil, err
			}
			stmt.ForeignKeys = append(stmt.ForeignKeys, fk)
		default:
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

// parseCheckClause parses "CHECK ( expr )". The CHECK keyword has not been
// consumed yet.
func (p *parser) parseCheckClause() (Expr, error) {
	if err := p.expectKeyword("CHECK"); err != nil {
		return nil, err
	}
	if p.peek().Type != TokenLParen {
		return nil, fmt.Errorf("%w: expected ( after CHECK, got %q", ErrParse, p.peek().Literal)
	}
	p.next()
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.peek().Type != TokenRParen {
		return nil, fmt.Errorf("%w: expected ) to close CHECK, got %q", ErrParse, p.peek().Literal)
	}
	p.next()
	return expr, nil
}

// parseForeignKeyClause parses a table-level
// "FOREIGN KEY ( col ) REFERENCES table ( col )" clause.
func (p *parser) parseForeignKeyClause() (ForeignKey, error) {
	if err := p.expectKeywords("FOREIGN", "KEY"); err != nil {
		return ForeignKey{}, err
	}
	cols, err := p.parseParenColumnList()
	if err != nil {
		return ForeignKey{}, err
	}
	if len(cols) != 1 {
		return ForeignKey{}, fmt.Errorf("%w: FOREIGN KEY supports exactly one column, got %d", ErrParse, len(cols))
	}
	ref, err := p.parseReferences()
	if err != nil {
		return ForeignKey{}, err
	}
	ref.Column = cols[0]
	return ref, nil
}

// parseReferences parses "REFERENCES table ( col )", returning a ForeignKey
// whose Column field the caller fills in.
func (p *parser) parseReferences() (ForeignKey, error) {
	if err := p.expectKeyword("REFERENCES"); err != nil {
		return ForeignKey{}, err
	}
	if p.peek().Type != TokenIdent {
		return ForeignKey{}, fmt.Errorf("%w: expected referenced table name, got %q", ErrParse, p.peek().Literal)
	}
	table := p.next().Literal
	cols, err := p.parseParenColumnList()
	if err != nil {
		return ForeignKey{}, err
	}
	if len(cols) != 1 {
		return ForeignKey{}, fmt.Errorf("%w: REFERENCES supports exactly one column, got %d", ErrParse, len(cols))
	}
	return ForeignKey{RefTable: table, RefColumn: cols[0]}, nil
}

// parseTableIndex parses a table-level "INDEX (cols...)" or "UNIQUE (cols...)"
// clause.
func (p *parser) parseTableIndex() (IndexClause, error) {
	unique := false
	switch {
	case p.isKeyword("UNIQUE"):
		p.next()
		unique = true
	default:
		if err := p.expectKeyword("INDEX"); err != nil {
			return IndexClause{}, err
		}
	}
	cols, err := p.parseParenColumnList()
	if err != nil {
		return IndexClause{}, err
	}
	if len(cols) == 0 {
		return IndexClause{}, fmt.Errorf("%w: index requires at least one column", ErrParse)
	}
	return IndexClause{Columns: cols, Unique: unique}, nil
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
	case p.isKeyword("RENAME"):
		p.next()
		if p.isKeyword("COLUMN") {
			p.next()
		}
		if p.peek().Type != TokenIdent {
			return nil, fmt.Errorf("%w: expected column name, got %q", ErrParse, p.peek().Literal)
		}
		old := p.next().Literal
		if err := p.expectKeyword("TO"); err != nil {
			return nil, err
		}
		if p.peek().Type != TokenIdent {
			return nil, fmt.Errorf("%w: expected new column name, got %q", ErrParse, p.peek().Literal)
		}
		stmt.Rename = true
		stmt.Column = ColumnDef{Name: old}
		stmt.NewName = p.next().Literal
	default:
		return nil, fmt.Errorf("%w: expected ADD, DROP, or RENAME, got %q", ErrParse, p.peek().Literal)
	}
	return stmt, nil
}

func (p *parser) parseCreateIndex() (*CreateIndexStmt, error) {
	if err := p.expectKeyword("CREATE"); err != nil {
		return nil, err
	}
	stmt := &CreateIndexStmt{}
	if p.isKeyword("UNIQUE") {
		p.next()
		stmt.Unique = true
	}
	if err := p.expectKeyword("INDEX"); err != nil {
		return nil, err
	}
	if p.isKeyword("IF") {
		if err := p.expectKeywords("IF", "NOT", "EXISTS"); err != nil {
			return nil, err
		}
		stmt.IfNotExists = true
	}
	if p.peek().Type == TokenIdent {
		stmt.Name = p.next().Literal
	}
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	if p.peek().Type != TokenIdent {
		return nil, fmt.Errorf("%w: expected table name, got %q", ErrParse, p.peek().Literal)
	}
	stmt.Table = p.next().Literal
	cols, err := p.parseParenColumnList()
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("%w: index requires at least one column", ErrParse)
	}
	stmt.Columns = cols
	return stmt, nil
}

func (p *parser) parseDropIndex() (*DropIndexStmt, error) {
	if err := p.expectKeywords("DROP", "INDEX"); err != nil {
		return nil, err
	}
	stmt := &DropIndexStmt{}
	if p.isKeyword("IF") {
		if err := p.expectKeywords("IF", "EXISTS"); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}
	if p.peek().Type != TokenIdent {
		return nil, fmt.Errorf("%w: expected index name, got %q", ErrParse, p.peek().Literal)
	}
	stmt.Name = p.next().Literal
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	if p.peek().Type != TokenIdent {
		return nil, fmt.Errorf("%w: expected table name, got %q", ErrParse, p.peek().Literal)
	}
	stmt.Table = p.next().Literal
	return stmt, nil
}

func (p *parser) parseDropTable() (*DropTableStmt, error) {
	if err := p.expectKeyword("DROP"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	stmt := &DropTableStmt{}
	if p.isKeyword("IF") {
		if err := p.expectKeywords("IF", "EXISTS"); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}
	if p.peek().Type != TokenIdent {
		return nil, fmt.Errorf("%w: expected table name, got %q", ErrParse, p.peek().Literal)
	}
	stmt.Table = p.next().Literal
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

	// Optional column constraints: PRIMARY KEY, INDEX, UNIQUE, NOT NULL,
	// and/or DEFAULT <literal>.
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
		case p.isKeyword("UNIQUE"):
			p.next()
			col.Unique = true
		case p.isKeyword("NOT"):
			p.next()
			if err := p.expectKeyword("NULL"); err != nil {
				return ColumnDef{}, err
			}
			col.NotNull = true
		case p.isKeyword("DEFAULT"):
			p.next()
			operand, err := p.parseOperand()
			if err != nil {
				return ColumnDef{}, err
			}
			lit, ok := operand.(*Literal)
			if !ok {
				return ColumnDef{}, fmt.Errorf("%w: DEFAULT for column %q must be a literal, got %s", ErrParse, col.Name, operand.String())
			}
			col.Default = lit
		case p.isKeyword("CHECK"):
			chk, err := p.parseCheckClause()
			if err != nil {
				return ColumnDef{}, err
			}
			col.Check = chk
		case p.isKeyword("REFERENCES"):
			ref, err := p.parseReferences()
			if err != nil {
				return ColumnDef{}, err
			}
			ref.Column = col.Name
			col.References = &ref
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
	for {
		vals, err := p.parseParenLiteralList()
		if err != nil {
			return nil, err
		}
		ins.Rows = append(ins.Rows, vals)
		if p.peek().Type != TokenComma {
			break
		}
		p.next()
	}
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
		val, err := p.parseOperand()
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

// parseSelectStatement parses a top-level SELECT and any trailing UNION [ALL]
// SELECT clauses, building a left-associative chain of SetOpStmt nodes. A query
// with no UNION returns the bare *SelectStmt.
func (p *parser) parseSelectStatement() (Statement, error) {
	left, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	var stmt Statement = left
	for p.isKeyword("UNION") {
		p.next()
		all := false
		if p.isKeyword("ALL") {
			p.next()
			all = true
		}
		right, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		stmt = &SetOpStmt{Left: stmt, Right: right, All: all}
	}
	return stmt, nil
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

// rankingFuncs are window-only functions that take no arguments and must be
// followed by an OVER clause.
var rankingFuncs = map[string]struct{}{
	"ROW_NUMBER": {}, "RANK": {}, "DENSE_RANK": {},
}

// parseFuncSelectItem parses a function-call projection item. isRank marks a
// no-argument ranking function. It parses "fn(args)" then, if OVER follows,
// produces a window item; otherwise it produces an aggregate item (and rejects
// a ranking function used without OVER).
func (p *parser) parseFuncSelectItem(isRank bool) (SelectItem, error) {
	fn := strings.ToUpper(p.next().Literal)
	p.next() // consume "("
	call := &AggregateCall{Func: fn}

	switch {
	case isRank:
		// Ranking functions take no arguments: expect an immediate ")".
	case p.peek().Type == TokenStar:
		p.next()
		if fn != "COUNT" {
			return SelectItem{}, fmt.Errorf("%w: %s(*) is not allowed; only COUNT(*)", ErrParse, fn)
		}
		call.Star = true
	default:
		if p.isKeyword("DISTINCT") {
			p.next()
			call.Distinct = true
		}
		col, err := p.parseColumnRef()
		if err != nil {
			return SelectItem{}, err
		}
		call.Column = col
	}

	if p.peek().Type != TokenRParen {
		return SelectItem{}, fmt.Errorf("%w: expected ) to close %s(, got %q", ErrParse, fn, p.peek().Literal)
	}
	p.next()

	if p.isKeyword("OVER") {
		win, err := p.parseOver(call)
		if err != nil {
			return SelectItem{}, err
		}
		return SelectItem{Window: win, Alias: p.parseOptionalAlias()}, nil
	}
	if isRank {
		return SelectItem{}, fmt.Errorf("%w: %s requires an OVER clause", ErrParse, fn)
	}
	return SelectItem{Agg: call, Alias: p.parseOptionalAlias()}, nil
}

// parseOver parses an OVER (PARTITION BY ... ORDER BY ...) clause, building a
// WindowCall from the already-parsed function call. Both PARTITION BY and
// ORDER BY are optional. DISTINCT aggregates are not supported as windows.
func (p *parser) parseOver(call *AggregateCall) (*WindowCall, error) {
	if call.Distinct {
		return nil, fmt.Errorf("%w: DISTINCT is not supported in window functions", ErrParse)
	}
	if err := p.expectKeyword("OVER"); err != nil {
		return nil, err
	}
	if p.peek().Type != TokenLParen {
		return nil, fmt.Errorf("%w: expected ( after OVER, got %q", ErrParse, p.peek().Literal)
	}
	p.next()

	win := &WindowCall{Func: call.Func, Star: call.Star, Arg: call.Column}
	if p.isKeyword("PARTITION") {
		p.next()
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			col, err := p.parseColumnRef()
			if err != nil {
				return nil, err
			}
			win.Partition = append(win.Partition, col.String())
			if p.peek().Type != TokenComma {
				break
			}
			p.next()
		}
	}
	if p.isKeyword("ORDER") {
		ob, err := p.parseOrderBy()
		if err != nil {
			return nil, err
		}
		win.Order = ob
	}
	if p.peek().Type != TokenRParen {
		return nil, fmt.Errorf("%w: expected ) to close OVER(, got %q", ErrParse, p.peek().Literal)
	}
	p.next()
	return win, nil
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

	// Function call: an identifier immediately followed by "(". It is either an
	// aggregate (possibly with a trailing OVER for a window) or a ranking window
	// function (ROW_NUMBER/RANK/DENSE_RANK, which require OVER).
	if p.peek().Type == TokenIdent && p.toks[p.pos+1].Type == TokenLParen {
		up := strings.ToUpper(p.peek().Literal)
		_, isAgg := aggregateFuncs[up]
		_, isRank := rankingFuncs[up]
		if isAgg || isRank {
			return p.parseFuncSelectItem(isRank)
		}
	}

	// Anything else is a value expression. A bare column reference keeps the
	// dedicated Column form so existing projection paths stay unchanged.
	expr, err := p.parseOperand()
	if err != nil {
		return SelectItem{}, err
	}
	if col, ok := expr.(*ColumnRef); ok {
		return SelectItem{Column: col, Alias: p.parseOptionalAlias()}, nil
	}
	return SelectItem{Expr: expr, Alias: p.parseOptionalAlias()}, nil
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
	// A "(" introduces a grouped boolean expression, a scalar subquery used as
	// a comparison operand, or a parenthesized arithmetic operand like
	// (a - 5) * 2 > 10. Try the boolean grouping first and rewind on failure so
	// the predicate path can consume the "(" as part of a value expression.
	if p.peek().Type == TokenLParen && !p.lparenStartsSubquery() {
		savedPos, savedParams := p.pos, p.params
		p.next()
		inner, err := p.parseOr()
		if err == nil && p.peek().Type == TokenRParen {
			p.next()
			return inner, nil
		}
		p.pos, p.params = savedPos, savedParams
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

// parseTypedLiteral parses a typed string literal whose type is named by the
// uppercased prefix keyword (DATE, DECIMAL/NUMERIC, UUID). The boolean reports
// whether prefix is a recognized typed-literal keyword; when false the caller
// falls back to treating the identifier as a column reference. The string token
// is consumed only when the prefix matches.
func (p *parser) parseTypedLiteral(prefix string) (Expr, bool, error) {
	str := p.toks[p.pos+1].Literal
	switch prefix {
	case "DATE":
		k, err := types.ParseDateOnly(str)
		if err != nil {
			return nil, true, fmt.Errorf("%w: invalid DATE literal %q: %v", ErrParse, str, err)
		}
		p.next()
		p.next()
		return &Literal{Kind: LitDate, Date: k}, true, nil
	case "DECIMAL", "NUMERIC":
		k, err := types.ParseDecimal(str)
		if err != nil {
			return nil, true, fmt.Errorf("%w: invalid DECIMAL literal %q: %v", ErrParse, str, err)
		}
		p.next()
		p.next()
		return &Literal{Kind: LitDecimal, Dec: k}, true, nil
	case "UUID":
		k, err := types.ParseUUID(str)
		if err != nil {
			return nil, true, fmt.Errorf("%w: invalid UUID literal %q: %v", ErrParse, str, err)
		}
		p.next()
		p.next()
		return &Literal{Kind: LitUUID, UUID: k}, true, nil
	default:
		return nil, false, nil
	}
}

// parseOperand parses a value expression with the usual precedence:
//
//	additive       := multiplicative (('+' | '-') multiplicative)*
//	multiplicative := unary (('*' | '/' | '%') unary)*
//	unary          := ['-'] primary
//	primary        := literal | column | function | CASE | aggregate
//	                | '(' additive ')' | '(' SELECT ... ')'
func (p *parser) parseOperand() (Expr, error) {
	return p.parseAdditive()
}

func (p *parser) parseAdditive() (Expr, error) {
	left, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}
	for p.peek().Type == TokenOperator && (p.peek().Literal == "+" || p.peek().Literal == "-") {
		op := p.next().Literal
		right, err := p.parseMultiplicative()
		if err != nil {
			return nil, err
		}
		left = &ArithExpr{Op: op, Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) parseMultiplicative() (Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for {
		var op string
		switch {
		case p.peek().Type == TokenStar:
			op = "*"
		case p.peek().Type == TokenOperator && (p.peek().Literal == "/" || p.peek().Literal == "%"):
			op = p.peek().Literal
		default:
			return left, nil
		}
		p.next()
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &ArithExpr{Op: op, Left: left, Right: right}
	}
}

// parseUnary handles a leading unary minus. A negated numeric literal folds
// into the literal itself (so DEFAULT -5 stays a plain literal); any other
// operand desugars to (0 - operand).
func (p *parser) parseUnary() (Expr, error) {
	if p.peek().Type == TokenOperator && p.peek().Literal == "-" {
		p.next()
		operand, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		if lit, ok := operand.(*Literal); ok {
			switch lit.Kind {
			case LitInt:
				return &Literal{Kind: LitInt, Int: -lit.Int}, nil
			case LitFloat:
				return &Literal{Kind: LitFloat, Float: -lit.Float}, nil
			}
		}
		return &ArithExpr{Op: "-", Left: &Literal{Kind: LitInt}, Right: operand}, nil
	}
	return p.parsePrimaryOperand()
}

// scalarFuncs are the supported scalar functions usable in value expressions.
var scalarFuncs = map[string]struct{}{
	"UPPER": {}, "LOWER": {}, "LENGTH": {}, "ABS": {}, "COALESCE": {},
}

// parseScalarFunc parses "fn(arg, ...)" for a recognized scalar function. The
// function-name identifier has not been consumed yet.
func (p *parser) parseScalarFunc() (Expr, error) {
	fn := strings.ToUpper(p.next().Literal)
	p.next() // consume "("
	call := &FuncCall{Name: fn}
	for {
		arg, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		call.Args = append(call.Args, arg)
		if p.peek().Type != TokenComma {
			break
		}
		p.next()
	}
	if p.peek().Type != TokenRParen {
		return nil, fmt.Errorf("%w: expected ) to close %s(, got %q", ErrParse, fn, p.peek().Literal)
	}
	p.next()
	return call, nil
}

// parseCase parses a searched CASE expression:
// CASE WHEN cond THEN result [WHEN ...] [ELSE result] END.
func (p *parser) parseCase() (Expr, error) {
	if err := p.expectKeyword("CASE"); err != nil {
		return nil, err
	}
	ce := &CaseExpr{}
	for p.isKeyword("WHEN") {
		p.next()
		cond, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("THEN"); err != nil {
			return nil, err
		}
		then, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		ce.Whens = append(ce.Whens, WhenClause{Cond: cond, Then: then})
	}
	if len(ce.Whens) == 0 {
		return nil, fmt.Errorf("%w: CASE requires at least one WHEN clause", ErrParse)
	}
	if p.isKeyword("ELSE") {
		p.next()
		els, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		ce.Else = els
	}
	if err := p.expectKeyword("END"); err != nil {
		return nil, err
	}
	return ce, nil
}

func (p *parser) parsePrimaryOperand() (Expr, error) {
	t := p.peek()
	if t.Type == TokenLParen {
		if p.lparenStartsSubquery() {
			sub, err := p.parseParenSelect()
			if err != nil {
				return nil, err
			}
			return &ScalarSubquery{Select: sub}, nil
		}
		// Parenthesized value expression: (age - 5) * 2.
		p.next()
		inner, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		if p.peek().Type != TokenRParen {
			return nil, fmt.Errorf("%w: expected ) to close expression, got %q", ErrParse, p.peek().Literal)
		}
		p.next()
		return inner, nil
	}
	switch t.Type {
	case TokenIdent:
		// Typed literal prefix (DATE/DECIMAL/NUMERIC/UUID) followed by a string:
		// DATE '2024-01-15', DECIMAL '1.50', UUID 'xxxx-...'.
		if p.toks[p.pos+1].Type == TokenString {
			if lit, ok, err := p.parseTypedLiteral(strings.ToUpper(t.Literal)); ok {
				return lit, err
			}
		}
		// Aggregate call used as an operand (e.g. in HAVING): COUNT(*) > 1.
		if p.toks[p.pos+1].Type == TokenLParen {
			if _, ok := aggregateFuncs[strings.ToUpper(t.Literal)]; ok {
				agg, err := p.parseAggregate()
				if err != nil {
					return nil, err
				}
				return &AggregateExpr{Call: agg}, nil
			}
			if _, ok := scalarFuncs[strings.ToUpper(t.Literal)]; ok {
				return p.parseScalarFunc()
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
		switch t.Literal {
		case "NULL":
			p.next()
			return &Literal{Kind: LitNull}, nil
		case "CASE":
			return p.parseCase()
		}
	}
	return nil, fmt.Errorf("%w: expected column or literal, got %q", ErrParse, t.Literal)
}
