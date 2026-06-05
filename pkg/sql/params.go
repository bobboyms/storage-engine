package sql

import (
	"errors"
	"fmt"

	"github.com/bobboyms/storage-engine/pkg/types"
)

// ErrBind is the sentinel wrapped when bind parameters cannot be applied: a
// wrong number of arguments or an argument whose Go type has no SQL literal.
var ErrBind = errors.New("sql: parameter bind error")

// parseBound parses a statement and binds the positional "?" parameters from
// args, returning a statement whose placeholders have been replaced by
// literals. It fails when the number of arguments does not match the number of
// placeholders. With no placeholders and no args it is equivalent to Parse.
func parseBound(input string, args []any) (Statement, error) {
	stmt, n, err := parseCounting(input)
	if err != nil {
		return nil, err
	}
	if n != len(args) {
		return nil, fmt.Errorf("%w: statement has %d placeholder(s) but %d argument(s) were given", ErrBind, n, len(args))
	}
	if n == 0 {
		return stmt, nil
	}
	if err := bindStatement(stmt, args); err != nil {
		return nil, err
	}
	return stmt, nil
}

// bindStatement replaces every Placeholder reachable from stmt with the literal
// built from the matching argument.
func bindStatement(stmt Statement, args []any) error {
	switch s := stmt.(type) {
	case *InsertStmt:
		return bindExprs(s.Values, args)
	case *UpdateStmt:
		for i := range s.Assignments {
			bound, err := bindExpr(s.Assignments[i].Value, args)
			if err != nil {
				return err
			}
			s.Assignments[i].Value = bound
		}
		return bindWhere(&s.Where, args)
	case *DeleteStmt:
		return bindWhere(&s.Where, args)
	case *SelectStmt:
		return bindSelect(s, args)
	default:
		// CREATE TABLE, ALTER TABLE, and DESCRIBE carry no value expressions.
		return nil
	}
}

// bindSelect binds placeholders in every expression a SELECT can hold: the
// WHERE and HAVING predicates, each JOIN ON condition, and any nested
// subqueries (derived FROM tables and predicate subqueries).
func bindSelect(sel *SelectStmt, args []any) error {
	if sel == nil {
		return nil
	}
	if err := bindWhere(&sel.Where, args); err != nil {
		return err
	}
	if err := bindWhere(&sel.Having, args); err != nil {
		return err
	}
	for i := range sel.Joins {
		if err := bindWhere(&sel.Joins[i].On, args); err != nil {
			return err
		}
		if err := bindSelect(sel.Joins[i].Subquery, args); err != nil {
			return err
		}
	}
	return bindSelect(sel.Subquery, args)
}

// bindWhere binds a nullable expression slot in place.
func bindWhere(slot *Expr, args []any) error {
	if *slot == nil {
		return nil
	}
	bound, err := bindExpr(*slot, args)
	if err != nil {
		return err
	}
	*slot = bound
	return nil
}

func bindExprs(exprs []Expr, args []any) error {
	for i := range exprs {
		bound, err := bindExpr(exprs[i], args)
		if err != nil {
			return err
		}
		exprs[i] = bound
	}
	return nil
}

// bindExpr returns e with every Placeholder in its subtree replaced by a
// Literal. Container nodes are mutated in place and returned; a Placeholder
// node is replaced by the literal for its ordinal.
func bindExpr(e Expr, args []any) (Expr, error) {
	switch n := e.(type) {
	case *Placeholder:
		return literalFromArg(n.Ordinal, args)
	case *BinaryExpr:
		left, err := bindExpr(n.Left, args)
		if err != nil {
			return nil, err
		}
		right, err := bindExpr(n.Right, args)
		if err != nil {
			return nil, err
		}
		n.Left, n.Right = left, right
		return n, nil
	case *IsNullExpr:
		operand, err := bindExpr(n.Operand, args)
		if err != nil {
			return nil, err
		}
		n.Operand = operand
		return n, nil
	case *InSubqueryExpr:
		operand, err := bindExpr(n.Operand, args)
		if err != nil {
			return nil, err
		}
		n.Operand = operand
		return n, bindSelect(n.Select, args)
	case *ScalarSubquery:
		return n, bindSelect(n.Select, args)
	case *ExistsExpr:
		return n, bindSelect(n.Select, args)
	default:
		// Literal, ColumnRef, AggregateExpr: no placeholders to replace.
		return e, nil
	}
}

// literalFromArg converts the argument at ordinal into a Literal. The argument
// types map to the literal kinds the value layer understands; an unsupported
// type is an error.
func literalFromArg(ordinal int, args []any) (*Literal, error) {
	if ordinal < 0 || ordinal >= len(args) {
		return nil, fmt.Errorf("%w: placeholder ordinal %d out of range", ErrBind, ordinal)
	}
	switch v := args[ordinal].(type) {
	case nil:
		return &Literal{Kind: LitNull}, nil
	case bool:
		return &Literal{Kind: LitBool, Bool: v}, nil
	case string:
		return &Literal{Kind: LitString, Str: v}, nil
	case int:
		return &Literal{Kind: LitInt, Int: int64(v)}, nil
	case int32:
		return &Literal{Kind: LitInt, Int: int64(v)}, nil
	case int64:
		return &Literal{Kind: LitInt, Int: v}, nil
	case float32:
		return &Literal{Kind: LitFloat, Float: float64(v)}, nil
	case float64:
		return &Literal{Kind: LitFloat, Float: v}, nil
	case types.UUIDKey:
		return &Literal{Kind: LitUUID, UUID: v}, nil
	case *types.UUIDKey:
		if v == nil {
			return &Literal{Kind: LitNull}, nil
		}
		return &Literal{Kind: LitUUID, UUID: *v}, nil
	case [16]byte:
		return &Literal{Kind: LitUUID, UUID: types.UUIDKey(v)}, nil
	case []byte:
		k, err := types.UUIDKeyFromBytes(v)
		if err != nil {
			return nil, fmt.Errorf("%w: argument %d: %v", ErrBind, ordinal, err)
		}
		return &Literal{Kind: LitUUID, UUID: k}, nil
	default:
		return nil, fmt.Errorf("%w: argument %d has unsupported type %T", ErrBind, ordinal, v)
	}
}
