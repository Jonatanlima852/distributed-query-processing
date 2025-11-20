package parser

import (
    "fmt"

    "github.com/Jonatan852/distributed-query-processing/pkg/query"
    "github.com/xwb1989/sqlparser"
)

// Parse converte uma string SQL em uma SelectStatement mínima.
// Nesta fase inicial garantimos apenas o fluxo e validações básicas.
func Parse(sql string) (*query.SelectStatement, error) {
    if sql == "" {
        return nil, fmt.Errorf("sql vazio")
    }

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

// convertSelect mantém apenas marcações principais por enquanto.
func convertSelect(stmt *sqlparser.Select) (*query.SelectStatement, error) {
    if stmt == nil {
        return nil, fmt.Errorf("select inválido")
    }

    result := &query.SelectStatement{Distinct: stmt.Distinct != ""}

    // Demais seções serão preenchidas nas próximas iterações.
    if len(stmt.From) == 0 {
        return nil, fmt.Errorf("cláusula FROM obrigatória")
    }

    return result, nil
}

// Demais auxiliares retornam erro explícito até a implementação completa.
func convertExpr(sqlparser.Expr) (query.Expression, error) {
    return query.Expression{}, fmt.Errorf("conversão de expressão não implementada")
}

func convertSelectExprs(sqlparser.SelectExprs) ([]query.SelectItem, error) {
    return nil, fmt.Errorf("select exprs não implementado")
}

func convertTableExprs(sqlparser.TableExprs) ([]query.TableReference, error) {
    return nil, fmt.Errorf("table exprs não implementado")
}

func convertOrderBy(sqlparser.OrderBy) ([]query.OrderBy, error) {
    return nil, fmt.Errorf("order by não implementado")
}

func convertLimitValue(node sqlparser.Expr) (int64, error) {
    return 0, fmt.Errorf("limit não implementado")
}
