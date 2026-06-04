package sql

import (
	"fmt"
	"strconv"
)

// Statement is the root AST node produced by Parse.
type Statement interface {
	stmtNode()
}

// SelectStmt represents a single-table SELECT query.
type SelectStmt struct {
	Star    bool     // true for "SELECT *"
	Columns []string // projected column names when Star is false
	Table   string
	Where   Expr     // nil when no WHERE clause
	OrderBy *OrderBy // nil when no ORDER BY clause
	Limit   *int64   // nil when no LIMIT clause
	Offset  *int64   // nil when no OFFSET clause
	// ForUpdate is set by a trailing FOR UPDATE clause; it requests row
	// locks on the matched rows and is only meaningful inside a transaction.
	ForUpdate bool
}

func (*SelectStmt) stmtNode() {}

// OrderBy describes an ORDER BY clause over a single column.
type OrderBy struct {
	Column string
	Desc   bool
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

// ColumnRef references a column by name.
type ColumnRef struct {
	Name string
}

func (c *ColumnRef) String() string { return c.Name }
func (*ColumnRef) exprNode()        {}

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
