package parser

import (
	"testing"

	"github.com/Jonatan852/distributed-query-processing/pkg/query"
)

func TestParseSelectStatement(t *testing.T) {
	sql := `
		SELECT DISTINCT e.user_id, COUNT(*) AS total, u.country
		FROM events e
		LEFT JOIN users u ON e.user_id = u.id
		WHERE e.ts >= '2025-01-01' AND u.country <> 'BR'
		GROUP BY e.user_id, u.country
		ORDER BY total DESC
		LIMIT 100
	`
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if !stmt.Distinct {
		t.Fatalf("expected DISTINCT flag")
	}
	if len(stmt.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(stmt.Columns))
	}
	first, ok := stmt.Columns[0].Expr.(query.ColumnRef)
	if !ok || first.Name != "user_id" || first.Table != "e" {
		t.Fatalf("unexpected first column %+v", stmt.Columns[0].Expr)
	}
	fn, ok := stmt.Columns[1].Expr.(query.FunctionCall)
	if !ok || fn.Name != "COUNT" || len(fn.Args) != 1 {
		t.Fatalf("expected COUNT function, got %+v", stmt.Columns[1].Expr)
	}
	if len(stmt.From) != 1 {
		t.Fatalf("expected single from entry")
	}
	if len(stmt.From[0].Joins) != 1 || stmt.From[0].Joins[0].Type != query.JoinTypeLeft {
		t.Fatalf("expected LEFT JOIN, got %+v", stmt.From[0].Joins)
	}
	if stmt.Where == nil {
		t.Fatalf("expected WHERE clause")
	}
	if len(stmt.GroupBy) != 2 {
		t.Fatalf("expected two GROUP BY expressions")
	}
	if len(stmt.OrderBy) != 1 || stmt.OrderBy[0].Direction != query.SortDesc {
		t.Fatalf("unexpected ORDER BY content")
	}
	if stmt.Limit == nil || *stmt.Limit != 100 {
		t.Fatalf("expected LIMIT 100, got %v", stmt.Limit)
	}
}
