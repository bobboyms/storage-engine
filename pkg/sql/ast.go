package sql

import (
	"fmt"
	"strconv"
	"strings"
)

// Statement is the root AST node produced by Parse.
type Statement interface {
	stmtNode()
}

// SelectStmt represents a single-table SELECT query.
type SelectStmt struct {
	Items   []SelectItem // projection list
	Table   string
	Alias   string       // table alias (defaults to Table when omitted)
	Joins   []JoinClause // JOINed tables, in order
	Where   Expr         // nil when no WHERE clause
	GroupBy []string     // grouping columns, empty when no GROUP BY
	Having  Expr         // nil when no HAVING clause
	OrderBy *OrderBy     // nil when no ORDER BY clause
	Limit   *int64       // nil when no LIMIT clause
	Offset  *int64       // nil when no OFFSET clause
	// ForUpdate is set by a trailing FOR UPDATE clause; it requests row
	// locks on the matched rows and is only meaningful inside a transaction.
	ForUpdate bool
}

func (*SelectStmt) stmtNode() {}

// SelectItem is one entry in a SELECT projection list: a star, a bare column,
// or an aggregate call, optionally renamed with AS.
type SelectItem struct {
	Star   bool
	Column *ColumnRef     // bare column projection (nil otherwise)
	Agg    *AggregateCall // aggregate projection (nil otherwise)
	Alias  string         // output name override, empty if none
}

// AggregateCall is an aggregate function application in a projection.
type AggregateCall struct {
	Func     string     // COUNT, SUM, AVG, MIN, MAX
	Star     bool       // COUNT(*)
	Distinct bool       // COUNT(DISTINCT col) and similar
	Column   *ColumnRef // argument column (nil for COUNT(*))
}

// OutputName returns the result-set column name for an item.
func (it SelectItem) OutputName() string {
	if it.Alias != "" {
		return it.Alias
	}
	switch {
	case it.Column != nil:
		return it.Column.Name
	case it.Agg != nil:
		return it.Agg.canonicalName()
	default:
		return "*"
	}
}

func (a AggregateCall) canonicalName() string {
	fn := strings.ToLower(a.Func)
	if a.Star {
		return fn + "(*)"
	}
	arg := a.Column.String()
	if a.Distinct {
		arg = "distinct " + arg
	}
	return fn + "(" + arg + ")"
}

// OrderBy describes an ORDER BY clause over a single column.
type OrderBy struct {
	Column string
	Desc   bool
}

// JoinClause is a single JOIN: the joined table, its alias, the ON predicate,
// and whether it is a LEFT (outer) join (false means INNER).
type JoinClause struct {
	Table string
	Alias string
	On    Expr
	Left  bool
}

// InsertStmt represents INSERT INTO table (cols...) VALUES (vals...).
type InsertStmt struct {
	Table   string
	Columns []string
	Values  []Expr // one literal per column, positionally aligned
}

func (*InsertStmt) stmtNode() {}

// Assignment is a single "column = value" pair in an UPDATE SET clause.
type Assignment struct {
	Column string
	Value  Expr
}

// UpdateStmt represents UPDATE table SET assignments... [WHERE expr].
type UpdateStmt struct {
	Table       string
	Assignments []Assignment
	Where       Expr // nil when no WHERE clause
}

func (*UpdateStmt) stmtNode() {}

// DeleteStmt represents DELETE FROM table [WHERE expr].
type DeleteStmt struct {
	Table string
	Where Expr // nil when no WHERE clause
}

func (*DeleteStmt) stmtNode() {}

// Expr is a WHERE-clause expression node.
type Expr interface {
	// String returns a canonical, fully parenthesized representation.
	String() string
	exprNode()
}

// ColumnRef references a column, optionally qualified by a table name or
// alias (e.g. "u.id"). Qualifier is empty for an unqualified reference.
type ColumnRef struct {
	Qualifier string
	Name      string
}

func (c *ColumnRef) String() string {
	if c.Qualifier != "" {
		return c.Qualifier + "." + c.Name
	}
	return c.Name
}
func (*ColumnRef) exprNode() {}

// LiteralKind enumerates the literal value categories.
type LiteralKind int

const (
	// LitInt is an integer literal.
	LitInt LiteralKind = iota
	// LitFloat is a floating-point literal.
	LitFloat
	// LitString is a string literal.
	LitString
	// LitBool is a boolean literal.
	LitBool
	// LitNull is the NULL literal.
	LitNull
)

// Literal is a constant value appearing in a WHERE clause.
type Literal struct {
	Kind  LiteralKind
	Int   int64
	Float float64
	Str   string
	Bool  bool
}

func (l *Literal) String() string {
	switch l.Kind {
	case LitInt:
		return strconv.FormatInt(l.Int, 10)
	case LitFloat:
		return fmt.Sprintf("%f", l.Float)
	case LitString:
		return "'" + l.Str + "'"
	case LitBool:
		return strconv.FormatBool(l.Bool)
	case LitNull:
		return "NULL"
	default:
		return "?"
	}
}

func (*Literal) exprNode() {}

// IsNullExpr is an "operand IS [NOT] NULL" predicate. Negate is true for the
// IS NOT NULL form.
type IsNullExpr struct {
	Operand Expr
	Negate  bool
}

func (e *IsNullExpr) String() string {
	if e.Negate {
		return "(" + e.Operand.String() + " IS NOT NULL)"
	}
	return "(" + e.Operand.String() + " IS NULL)"
}

func (*IsNullExpr) exprNode() {}

// AggregateExpr wraps an aggregate call so it can appear as an operand inside a
// HAVING predicate (e.g. COUNT(*) > 1). It resolves against the per-group
// aggregate values during evaluation.
type AggregateExpr struct {
	Call *AggregateCall
}

func (e *AggregateExpr) String() string { return e.Call.canonicalName() }
func (*AggregateExpr) exprNode()        {}

// BinaryExpr is a comparison (= <> != < <= > >=) or a logical connective
// (AND, OR) joining two sub-expressions.
type BinaryExpr struct {
	Op    string
	Left  Expr
	Right Expr
}

func (b *BinaryExpr) String() string {
	return "(" + b.Left.String() + " " + b.Op + " " + b.Right.String() + ")"
}

func (*BinaryExpr) exprNode() {}
