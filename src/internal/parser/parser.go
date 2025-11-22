package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
	"github.com/Jonatan852/distributed-query-processing/pkg/query"
	"github.com/xwb1989/sqlparser"
)

// Parse converts a SQL string into a SelectStatement AST using sqlparser.
func Parse(sql string) (*query.SelectStatement, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("erro ao parsear SQL: %w", err)
	}

	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("apenas SELECT é suportado")
	}

	return convertSelect(selectStmt)
}

func convertSelect(stmt *sqlparser.Select) (*query.SelectStatement, error) {
	result := &query.SelectStatement{
		Distinct: stmt.Distinct != "",
	}

	// Converter SELECT columns
	columns, err := convertSelectExprs(stmt.SelectExprs)
	if err != nil {
		return nil, err
	}
	result.Columns = columns

	// Converter FROM
	if stmt.From != nil && len(stmt.From) > 0 {
		from, err := convertTableExprs(stmt.From)
		if err != nil {
			return nil, err
		}
		result.From = from
	}

	// Converter WHERE
	if stmt.Where != nil {
		where, err := convertExpr(stmt.Where.Expr)
		if err != nil {
			return nil, err
		}
		result.Where = where
	}

	// Converter GROUP BY
	if stmt.GroupBy != nil {
		groupBy, err := convertExprs(stmt.GroupBy)
		if err != nil {
			return nil, err
		}
		result.GroupBy = groupBy
	}

	// Converter ORDER BY
	if stmt.OrderBy != nil {
		orderBy, err := convertOrderBy(stmt.OrderBy)
		if err != nil {
			return nil, err
		}
		result.OrderBy = orderBy
	}

	// Converter LIMIT
	if stmt.Limit != nil {
		if stmt.Limit.Rowcount != nil {
			limitVal, err := convertLimitValue(stmt.Limit.Rowcount)
			if err != nil {
				return nil, err
			}
			result.Limit = &limitVal
		}
	}

	return result, nil
}

func convertSelectExprs(exprs sqlparser.SelectExprs) ([]query.SelectItem, error) {
	result := make([]query.SelectItem, 0, len(exprs))
	for _, expr := range exprs {
		item, err := convertSelectExpr(expr)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, nil
}

func convertSelectExpr(expr sqlparser.SelectExpr) (query.SelectItem, error) {
	item := query.SelectItem{}

	switch e := expr.(type) {
	case *sqlparser.StarExpr:
		wildcard := &query.Wildcard{}
		if e.TableName.Name.String() != "" {
			wildcard.Table = e.TableName.Name.String()
		}
		item.Wildcard = wildcard

	case *sqlparser.AliasedExpr:
		// Converter expressão
		convertedExpr, err := convertExpr(e.Expr)
		if err != nil {
			return query.SelectItem{}, err
		}
		item.Expr = convertedExpr

		// Converter alias
		if e.As.String() != "" {
			item.Alias = e.As.String()
		}

	default:
		// Tentar converter como expressão genérica (apenas se for Expr)
		if exprExpr, ok := expr.(sqlparser.Expr); ok {
			convertedExpr, err := convertExpr(exprExpr)
			if err != nil {
				return query.SelectItem{}, fmt.Errorf("tipo de expressão SELECT não suportado: %T", expr)
			}
			item.Expr = convertedExpr
		} else {
			// Se não for Expr nem StarExpr nem AliasedExpr, não suportamos
			return query.SelectItem{}, fmt.Errorf("tipo de expressão SELECT não suportado: %T", expr)
		}
	}

	return item, nil
}

func convertTableExprs(exprs sqlparser.TableExprs) ([]query.TableReference, error) {
	result := make([]query.TableReference, 0, len(exprs))
	for _, expr := range exprs {
		ref, err := convertTableExpr(expr)
		if err != nil {
			return nil, err
		}
		result = append(result, ref)
	}
	return result, nil
}

func convertTableExpr(expr sqlparser.TableExpr) (query.TableReference, error) {
	switch e := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		ref := query.TableReference{}

		// Converter nome da tabela
		switch t := e.Expr.(type) {
		case sqlparser.TableName:
			ref.Name = t.Name.String()
		default:
			return query.TableReference{}, fmt.Errorf("tipo de tabela não suportado: %T", e.Expr)
		}

		// Converter alias
		if e.As.String() != "" {
			ref.Alias = e.As.String()
		}

		return ref, nil

	case *sqlparser.JoinTableExpr:
		// JOIN explícito - processar left primeiro
		left, err := convertTableExpr(e.LeftExpr)
		if err != nil {
			return query.TableReference{}, err
		}

		// Converter join
		join, err := convertJoinExpr(e)
		if err != nil {
			return query.TableReference{}, err
		}

		// Processar right side para obter nome da tabela
		if rightAliased, ok := e.RightExpr.(*sqlparser.AliasedTableExpr); ok {
			if tn, ok := rightAliased.Expr.(sqlparser.TableName); ok {
				join.Table = tn.Name.String()
				if rightAliased.As.String() != "" {
					join.Alias = rightAliased.As.String()
				}
			}
		}

		left.Joins = append(left.Joins, join)
		return left, nil

	default:
		return query.TableReference{}, fmt.Errorf("tipo de expressão de tabela não suportado: %T", expr)
	}
}

func convertJoinExpr(join *sqlparser.JoinTableExpr) (query.JoinClause, error) {
	clause := query.JoinClause{}

	// Converter tipo de join
	joinStr := strings.ToUpper(join.Join)
	switch {
	case joinStr == "LEFT JOIN" || joinStr == "LEFT OUTER JOIN":
		clause.Type = query.JoinTypeLeft
	case joinStr == "RIGHT JOIN" || joinStr == "RIGHT OUTER JOIN":
		clause.Type = query.JoinTypeRight
	case joinStr == "FULL JOIN" || joinStr == "FULL OUTER JOIN":
		clause.Type = query.JoinTypeFull
	case joinStr == "CROSS JOIN":
		clause.Type = query.JoinTypeCross
	default:
		clause.Type = query.JoinTypeInner
	}

	// Converter condição ON
	if join.Condition.On != nil {
		cond, err := convertExpr(join.Condition.On)
		if err != nil {
			return query.JoinClause{}, err
		}
		clause.Condition = cond
	}

	return clause, nil
}

func convertOrderBy(orderBy sqlparser.OrderBy) ([]query.OrderExpression, error) {
	result := make([]query.OrderExpression, 0, len(orderBy))
	for _, order := range orderBy {
		expr, err := convertExpr(order.Expr)
		if err != nil {
			return nil, err
		}
		direction := query.SortAsc
		if strings.ToUpper(order.Direction) == "DESC" {
			direction = query.SortDesc
		}
		result = append(result, query.OrderExpression{
			Expr:      expr,
			Direction: direction,
		})
	}
	return result, nil
}

func convertLimitValue(expr sqlparser.Expr) (int64, error) {
	switch e := expr.(type) {
	case *sqlparser.SQLVal:
		if e.Type == sqlparser.IntVal {
			val, err := strconv.ParseInt(string(e.Val), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("valor LIMIT inválido: %w", err)
			}
			return val, nil
		}
		return 0, fmt.Errorf("LIMIT deve ser um inteiro")
	default:
		return 0, fmt.Errorf("tipo de LIMIT não suportado: %T", expr)
	}
}

func convertExprs(exprs []sqlparser.Expr) ([]query.Expression, error) {
	result := make([]query.Expression, 0, len(exprs))
	for _, expr := range exprs {
		converted, err := convertExpr(expr)
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	return result, nil
}

func convertExpr(expr sqlparser.Expr) (query.Expression, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *sqlparser.ColName:
		// Referência de coluna
		ref := query.ColumnRef{
			Name: e.Name.String(),
		}
		if !e.Qualifier.IsEmpty() {
			ref.Table = e.Qualifier.Name.String()
		}
		return ref, nil

	case sqlparser.ValTuple:
		// Tupla de valores - não suportado como expressão
		return nil, fmt.Errorf("tuplas não são suportadas em expressões")

	case *sqlparser.SQLVal:
		// Valor literal
		return convertLiteral(e)

	case *sqlparser.NullVal:
		// NULL
		return query.NullLiteral{}, nil

	case *sqlparser.BinaryExpr:
		// Expressão binária (a op b)
		left, err := convertExpr(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := convertExpr(e.Right)
		if err != nil {
			return nil, err
		}
		return query.BinaryExpr{
			Left:     left,
			Operator: e.Operator,
			Right:    right,
		}, nil

	case *sqlparser.UnaryExpr:
		// Expressão unária (NOT, -)
		expr, err := convertExpr(e.Expr)
		if err != nil {
			return nil, err
		}
		return query.UnaryExpr{
			Operator: e.Operator,
			Expr:     expr,
		}, nil

	case *sqlparser.FuncExpr:
		// Chamada de função
		name := e.Name.String()
		args := make([]query.Expression, 0, len(e.Exprs))
		for _, arg := range e.Exprs {
			if argExpr, ok := arg.(sqlparser.Expr); ok {
				converted, err := convertExpr(argExpr)
				if err != nil {
					return nil, err
				}
				args = append(args, converted)
			} else if star, ok := arg.(*sqlparser.StarExpr); ok {
				// COUNT(*) - usar wildcard
				wildcard := query.Wildcard{}
				if !star.TableName.IsEmpty() {
					wildcard.Table = star.TableName.Name.String()
				}
				args = append(args, wildcard)
			} else {
				return nil, fmt.Errorf("tipo de argumento de função não suportado: %T", arg)
			}
		}
		return query.FunctionCall{
			Name:     strings.ToUpper(name),
			Args:     args,
			Distinct: e.Distinct,
		}, nil

	case *sqlparser.ComparisonExpr:
		// Comparação (a op b)
		left, err := convertExpr(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := convertExpr(e.Right)
		if err != nil {
			return nil, err
		}
		return query.BinaryExpr{
			Left:     left,
			Operator: e.Operator,
			Right:    right,
		}, nil

	case *sqlparser.RangeCond:
		// BETWEEN
		expr, err := convertExpr(e.Left)
		if err != nil {
			return nil, err
		}
		lower, err := convertExpr(e.From)
		if err != nil {
			return nil, err
		}
		upper, err := convertExpr(e.To)
		if err != nil {
			return nil, err
		}
		return query.BetweenExpr{
			Expr:  expr,
			Lower: lower,
			Upper: upper,
			Not:   e.Operator == sqlparser.NotBetweenStr,
		}, nil

	case *sqlparser.IsExpr:
		// IS NULL, IS TRUE, etc.
		left, err := convertExpr(e.Expr)
		if err != nil {
			return nil, err
		}
		var right query.Expression
		op := "IS"
		switch strings.ToUpper(e.Operator) {
		case "IS NULL":
			right = query.NullLiteral{}
		case "IS NOT NULL":
			op = "IS NOT"
			right = query.NullLiteral{}
		case "IS TRUE":
			right = query.Literal{Value: columnar.NewBoolValue(true)}
		case "IS FALSE":
			right = query.Literal{Value: columnar.NewBoolValue(false)}
		default:
			return nil, fmt.Errorf("IS %s não suportado", e.Operator)
		}
		return query.BinaryExpr{
			Left:     left,
			Operator: op,
			Right:    right,
		}, nil

	case *sqlparser.AndExpr:
		// AND
		left, err := convertExpr(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := convertExpr(e.Right)
		if err != nil {
			return nil, err
		}
		return query.BinaryExpr{
			Left:     left,
			Operator: "AND",
			Right:    right,
		}, nil

	case *sqlparser.OrExpr:
		// OR
		left, err := convertExpr(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := convertExpr(e.Right)
		if err != nil {
			return nil, err
		}
		return query.BinaryExpr{
			Left:     left,
			Operator: "OR",
			Right:    right,
		}, nil

	case *sqlparser.ParenExpr:
		// Parênteses - apenas converter o conteúdo
		return convertExpr(e.Expr)

	default:
		return nil, fmt.Errorf("tipo de expressão não suportado: %T", expr)
	}
}

func convertLiteral(val *sqlparser.SQLVal) (query.Expression, error) {
	switch val.Type {
	case sqlparser.StrVal:
		return query.Literal{Value: columnar.NewStringValue(string(val.Val))}, nil

	case sqlparser.IntVal:
		intVal, err := strconv.ParseInt(string(val.Val), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("valor inteiro inválido: %w", err)
		}
		return query.Literal{Value: columnar.NewIntValue(intVal)}, nil

	case sqlparser.FloatVal:
		floatVal, err := strconv.ParseFloat(string(val.Val), 64)
		if err != nil {
			return nil, fmt.Errorf("valor float inválido: %w", err)
		}
		return query.Literal{Value: columnar.NewFloatValue(floatVal)}, nil

	case sqlparser.HexVal:
		// Hexadecimal - converter para int
		intVal, err := strconv.ParseInt(strings.TrimPrefix(string(val.Val), "0x"), 16, 64)
		if err != nil {
			return nil, fmt.Errorf("valor hexadecimal inválido: %w", err)
		}
		return query.Literal{Value: columnar.NewIntValue(intVal)}, nil

	default:
		return nil, fmt.Errorf("tipo de literal não suportado: %v", val.Type)
	}
}
