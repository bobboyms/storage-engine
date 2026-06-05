package sql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bobboyms/storage-engine/pkg/storage"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// Statement is the root AST node produced by Parse.
type Statement interface {
	stmtNode()
}

// SelectStmt represents a single-table SELECT query.
type SelectStmt struct {
	Items    []SelectItem // projection list
	Table    string       // base table name (empty when Subquery is set)
	Subquery *SelectStmt  // derived table in FROM (nil for a plain table)
	Alias    string       // table/derived-table alias (defaults to Table)
	Joins    []JoinClause // JOINed sources, in order
	Where    Expr         // nil when no WHERE clause
	GroupBy  []string     // grouping columns, empty when no GROUP BY
	Having   Expr         // nil when no HAVING clause
	OrderBy  *OrderBy     // nil when no ORDER BY clause
	Limit    *int64       // nil when no LIMIT clause
	Offset   *int64       // nil when no OFFSET clause
	// ForUpdate is set by a trailing FOR UPDATE clause; it requests row
	// locks on the matched rows and is only meaningful inside a transaction.
	ForUpdate bool
}

func (*SelectStmt) stmtNode() {}

// SetOpStmt is a set operation combining a left query with a right SELECT. The
// only operation currently supported is UNION; All distinguishes UNION ALL
// (keep duplicates) from UNION (eliminate duplicate result rows). Left is a
// *SelectStmt or a nested *SetOpStmt, so chained unions are left-associative.
type SetOpStmt struct {
	Left  Statement
	Right *SelectStmt
	All   bool
}

func (*SetOpStmt) stmtNode() {}

// SelectItem is one entry in a SELECT projection list: a star, a bare column,
// or an aggregate call, optionally renamed with AS.
type SelectItem struct {
	Star   bool
	Column *ColumnRef     // bare column projection (nil otherwise)
	Agg    *AggregateCall // aggregate projection (nil otherwise)
	Window *WindowCall    // window-function projection (nil otherwise)
	Alias  string         // output name override, empty if none
}

// WindowCall is a window-function application in a projection:
// func(arg) OVER (PARTITION BY ... ORDER BY ...). Func is one of the ranking
// functions (ROW_NUMBER, RANK, DENSE_RANK) or an aggregate (SUM, COUNT, AVG,
// MIN, MAX) evaluated over the window frame. Star marks COUNT(*); Arg is the
// argument column for aggregates (nil for ranking functions and COUNT(*)).
type WindowCall struct {
	Func      string
	Star      bool
	Arg       *ColumnRef
	Partition []string // PARTITION BY columns (canonical strings), empty if none
	Order     *OrderBy // ORDER BY within the window, nil if none
}

// canonicalName returns the default result-set column name for a window call,
// e.g. "row_number()", "sum(age)", or "count(*)".
func (w *WindowCall) canonicalName() string {
	fn := strings.ToLower(w.Func)
	switch {
	case w.Star:
		return fn + "(*)"
	case w.Arg != nil:
		return fn + "(" + w.Arg.String() + ")"
	default:
		return fn + "()"
	}
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
	case it.Window != nil:
		return it.Window.canonicalName()
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

// JoinClause is a single JOIN: the joined source (a table or a derived
// subquery), its alias, the ON predicate, and whether it is a LEFT (outer)
// join (false means INNER).
type JoinClause struct {
	Table    string
	Subquery *SelectStmt // derived table (nil for a plain table)
	Alias    string
	On       Expr
	Left     bool
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

// ColumnDef is a column definition in CREATE TABLE: its name, engine data type,
// and whether it is the primary key or a secondary index.
type ColumnDef struct {
	Name    string
	Type    storage.DataType
	Primary bool
	Index   bool
	Unique  bool
}

// IndexClause is a table-level index definition in CREATE TABLE, e.g.
// INDEX (a, b) or UNIQUE (a, b). A single column produces a regular
// single-column index; two or more produce a composite index. Unique marks a
// UNIQUE constraint.
type IndexClause struct {
	Columns []string
	Unique  bool
}

// CreateTableStmt represents CREATE TABLE [IF NOT EXISTS] name (column defs...,
// table-level index clauses...).
type CreateTableStmt struct {
	Table   string
	Columns []ColumnDef
	Indexes []IndexClause
	// IfNotExists makes execution a silent no-op when a table with the same
	// name already exists, instead of returning ErrDuplicateTable.
	IfNotExists bool
}

func (*CreateTableStmt) stmtNode() {}

// AlterTableStmt represents ALTER TABLE name ADD/DROP COLUMN. Drop selects the
// action: false adds Column (a full definition, optionally a secondary index);
// true drops the column named by Column.Name.
type AlterTableStmt struct {
	Table  string
	Drop   bool
	Column ColumnDef
	// IfExists guards the column operation so it can run idempotently: ADD
	// COLUMN IF NOT EXISTS is a silent no-op when the column already exists, and
	// DROP COLUMN IF EXISTS is a silent no-op when the column is absent.
	IfExists bool
}

func (*AlterTableStmt) stmtNode() {}

// noop reports whether the guarded ALTER should be skipped for schema: an ADD
// whose column already exists, or a DROP whose column is missing. It is false
// for an unguarded statement, which must surface the conflict as an error.
func (s *AlterTableStmt) noop(schema TableSchema) bool {
	if !s.IfExists {
		return false
	}
	_, exists := schema.Column(s.Column.Name)
	if s.Drop {
		return !exists
	}
	return exists
}

// DescribeStmt represents DESCRIBE name (or its DESC alias): a read-only
// introspection statement that returns the columns of a table.
type DescribeStmt struct {
	Table string
}

func (*DescribeStmt) stmtNode() {}

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
	// LitUUID is a 16-byte UUID literal.
	LitUUID
	// LitDate is a calendar-date literal (DATE 'YYYY-MM-DD').
	LitDate
	// LitDecimal is an exact-precision decimal literal (DECIMAL '1.50').
	LitDecimal
)

// Literal is a constant value appearing in a WHERE clause.
type Literal struct {
	Kind  LiteralKind
	Int   int64
	Float float64
	Str   string
	Bool  bool
	UUID  types.UUIDKey
	Date  types.DateOnlyKey
	Dec   types.DecimalKey
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
	case LitUUID:
		return "UUID '" + l.UUID.String() + "'"
	case LitDate:
		return "DATE '" + l.Date.String() + "'"
	case LitDecimal:
		return "DECIMAL '" + l.Dec.String() + "'"
	default:
		return "?"
	}
}

func (*Literal) exprNode() {}

// Placeholder is an unbound positional bind parameter ("?"). Ordinal is its
// zero-based position in the statement, assigned left to right at parse time.
// Binding replaces every Placeholder with a Literal before execution, so the
// executor never sees a Placeholder.
type Placeholder struct {
	Ordinal int
}

func (*Placeholder) String() string { return "?" }
func (*Placeholder) exprNode()      {}

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

// ScalarSubquery is a parenthesized SELECT used as a value operand; it must
// yield a single row and column at evaluation time.
type ScalarSubquery struct {
	Select *SelectStmt
}

func (e *ScalarSubquery) String() string { return "(scalar subquery)" }
func (*ScalarSubquery) exprNode()        {}

// InSubqueryExpr is "operand [NOT] IN (SELECT ...)".
type InSubqueryExpr struct {
	Operand Expr
	Select  *SelectStmt
	Negate  bool
}

func (e *InSubqueryExpr) String() string {
	op := "IN"
	if e.Negate {
		op = "NOT IN"
	}
	return "(" + e.Operand.String() + " " + op + " (subquery))"
}
func (*InSubqueryExpr) exprNode() {}

// ExistsExpr is "[NOT] EXISTS (SELECT ...)".
type ExistsExpr struct {
	Select *SelectStmt
	Negate bool
}

func (e *ExistsExpr) String() string {
	if e.Negate {
		return "(NOT EXISTS (subquery))"
	}
	return "(EXISTS (subquery))"
}
func (*ExistsExpr) exprNode() {}

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
