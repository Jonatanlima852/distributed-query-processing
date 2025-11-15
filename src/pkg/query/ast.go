package query

import (
	"fmt"

	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
)

// Statement represents a query that can be executed by the system.
type Statement interface {
	statement()
}

// SelectStatement models a SELECT query with the clauses suported by the MVP.
type SelectStatement struct {
	Distinct bool
	Columns  []SelectItem
	From     []TableReference
	Where    Expression
	GroupBy  []Expression
	OrderBy  []OrderExpression
	Limit    *int64
}

func (*SelectStatement) statement() {}

// SelectItem represents one entry in the SELECT list.
type SelectItem struct {
	Expr     Expression
	Alias    string
	Wildcard *Wildcard
}

// TableReference represents a FROM entry with optional joins.
type TableReference struct {
	Name  string
	Alias string
	Joins []JoinClause
}

// JoinType enumerates the supported join types.
type JoinType string

const (
	JoinTypeInner JoinType = "INNER"
	JoinTypeLeft  JoinType = "LEFT"
	JoinTypeRight JoinType = "RIGHT"
	JoinTypeFull  JoinType = "FULL"
	JoinTypeCross JoinType = "CROSS"
)

// JoinClause models a JOIN on a table reference.
type JoinClause struct {
	Type      JoinType
	Table     string
	Alias     string
	Condition Expression
}

// OrderExpression represents an ORDER BY entry.
type OrderExpression struct {
	Expr      Expression
	Direction SortDirection
}

// SortDirection enumerates sort orders.
type SortDirection string

const (
	SortAsc  SortDirection = "ASC"
	SortDesc SortDirection = "DESC"
)

// Expression is the root interface for all parsed expressions.
type Expression interface {
	expression()
	fmt.Stringer
}

// ColumnRef is a reference to table.column.
type ColumnRef struct {
	Table string
	Name  string
}

func (ColumnRef) expression() {}

func (c ColumnRef) String() string {
	if c.Table != "" {
		return fmt.Sprintf("%s.%s", c.Table, c.Name)
	}
	return c.Name
}

// Wildcard represents "*" or "table.*".
type Wildcard struct {
	Table string
}

func (Wildcard) expression() {}

func (w Wildcard) String() string {
	if w.Table == "" {
		return "*"
	}
	return fmt.Sprintf("%s.*", w.Table)
}

// Literal is a typed literal value.
type Literal struct {
	Value columnar.Value
}

func (Literal) expression() {}

func (l Literal) String() string {
	return l.Value.String()
}

// NullLiteral represents NULL.
type NullLiteral struct{}

func (NullLiteral) expression() {}

func (NullLiteral) String() string { return "NULL" }

// BinaryExpr is an infix expression (a op b).
type BinaryExpr struct {
	Left     Expression
	Operator string
	Right    Expression
}

func (BinaryExpr) expression() {}

func (b BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", b.Left, b.Operator, b.Right)
}

// UnaryExpr is a prefix operator expression (NOT a, -a).
type UnaryExpr struct {
	Operator string
	Expr     Expression
}

func (UnaryExpr) expression() {}

func (u UnaryExpr) String() string {
	return fmt.Sprintf("(%s %s)", u.Operator, u.Expr)
}

// FunctionCall models scalar/aggregate function invocations.
type FunctionCall struct {
	Name     string
	Args     []Expression
	Distinct bool
}

func (FunctionCall) expression() {}

func (f FunctionCall) String() string {
	distinct := ""
	if f.Distinct {
		distinct = "DISTINCT "
	}
	return fmt.Sprintf("%s(%s%s)", f.Name, distinct, joinExpressions(f.Args))
}

// Parameter represents a named parameter (e.g., :country).
type Parameter struct {
	Name string
}

func (Parameter) expression() {}

func (p Parameter) String() string {
	return ":" + p.Name
}

// BetweenExpr models "expr BETWEEN lower AND upper".
type BetweenExpr struct {
	Expr  Expression
	Lower Expression
	Upper Expression
	Not   bool
}

func (BetweenExpr) expression() {}

func (b BetweenExpr) String() string {
	not := ""
	if b.Not {
		not = "NOT "
	}
	return fmt.Sprintf("(%s %sBETWEEN %s AND %s)", b.Expr, not, b.Lower, b.Upper)
}

func joinExpressions(exprs []Expression) string {
	if len(exprs) == 0 {
		return ""
	}
	s := exprs[0].String()
	for i := 1; i < len(exprs); i++ {
		s += ", " + exprs[i].String()
	}
	return s
}
