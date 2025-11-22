package planner

import (
	"testing"

	"github.com/Jonatan852/distributed-query-processing/internal/storage"
	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
	"github.com/Jonatan852/distributed-query-processing/pkg/query"
)

type mockMetadata struct {
	tables map[string]storage.TableSchema
}

func (m mockMetadata) Table(name string) (storage.TableSchema, error) {
	if schema, ok := m.tables[name]; ok {
		return schema, nil
	}
	return storage.TableSchema{}, storage.ErrTableNotFound
}

func TestPlannerBuildsPlan(t *testing.T) {
	stmt := &query.SelectStatement{
		Columns: []query.SelectItem{
			{Expr: query.ColumnRef{Table: "e", Name: "user_id"}},
			{Expr: query.FunctionCall{Name: "COUNT", Args: []query.Expression{query.Wildcard{}}}, Alias: "total"},
		},
		From: []query.TableReference{
			{
				Name:  "events",
				Alias: "e",
				Joins: []query.JoinClause{
					{
						Type:      query.JoinTypeLeft,
						Table:     "users",
						Alias:     "u",
						Condition: query.BinaryExpr{Left: query.ColumnRef{Table: "e", Name: "user_id"}, Operator: "=", Right: query.ColumnRef{Table: "u", Name: "id"}},
					},
				},
			},
		},
		Where: query.BinaryExpr{
			Left:     query.ColumnRef{Table: "u", Name: "country"},
			Operator: "=",
			Right:    query.Literal{Value: columnar.NewStringValue("BR")},
		},
		GroupBy: []query.Expression{
			query.ColumnRef{Table: "e", Name: "user_id"},
		},
		OrderBy: []query.OrderExpression{
			{Expr: query.ColumnRef{Name: "total"}, Direction: query.SortDesc},
		},
	}

	metadata := mockMetadata{
		tables: map[string]storage.TableSchema{
			"events": {
				Name: "events",
				Columns: []storage.ColumnSchema{
					{Name: "user_id", Type: columnar.TypeInt},
					{Name: "value", Type: columnar.TypeFloat},
				},
			},
			"users": {
				Name: "users",
				Columns: []storage.ColumnSchema{
					{Name: "id", Type: columnar.TypeInt},
					{Name: "country", Type: columnar.TypeString},
				},
			},
		},
	}

	planner := New(metadata)
	plan, err := planner.Build(stmt)
	if err != nil {
		t.Fatalf("planner falhou: %v", err)
	}
	if plan.Root == nil {
		t.Fatalf("plano sem nó raiz")
	}
	if plan.Root.Children[0].Type != query.PlanNodeLimit && plan.Root.Children[0].Type != query.PlanNodeSort && plan.Root.Children[0].Type != query.PlanNodeAggregate {
		t.Fatalf("plano parece incompleto, obtido: %s", plan.Root.Children[0].Type)
	}
}
