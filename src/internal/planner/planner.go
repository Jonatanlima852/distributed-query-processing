package planner

import (
	"fmt"
	"strings"

	"github.com/Jonatan852/distributed-query-processing/internal/storage"
	"github.com/Jonatan852/distributed-query-processing/pkg/query"
)

// MetadataProvider descreve o mínimo necessário para consultar schemas.
type MetadataProvider interface {
	Table(name string) (storage.TableSchema, error)
}

// Planner transforma uma AST (SelectStatement) em um plano físico distribuído.
type Planner struct {
	metadata MetadataProvider
}

// New cria um planner usando o provedor de metadata informado.
func New(metadata MetadataProvider) *Planner {
	return &Planner{metadata: metadata}
}

// Build gera o plano físico distribuído para a query.
func (p *Planner) Build(stmt *query.SelectStatement) (*query.PhysicalPlan, error) {
	if stmt == nil {
		return nil, fmt.Errorf("select statement não pode ser nulo")
	}
	if len(stmt.From) == 0 {
		return nil, fmt.Errorf("cláusula FROM obrigatória")
	}

	tablePredicates, globalPredicates := p.splitPredicates(stmt)
	root, err := p.buildFromTree(stmt.From, tablePredicates)
	if err != nil {
		return nil, err
	}

	if len(globalPredicates) > 0 {
		filter := query.NewPlanNode(query.PlanNodeFilter)
		filter.Properties["predicates"] = expressionsToStrings(globalPredicates)
		filter.AddChild(root)
		root = filter
	}

	projectionInfo := buildProjectionSpecs(stmt.Columns)
	if len(projectionInfo) > 0 && !allWildcards(projectionInfo) {
		project := query.NewPlanNode(query.PlanNodeProject)
		project.Properties["items"] = projectionInfo
		project.AddChild(root)
		root = project
	}

	if needsAggregation(stmt) {
		root = p.buildAggregation(root, stmt)
	}

	if len(stmt.OrderBy) > 0 {
		sortNode := query.NewPlanNode(query.PlanNodeSort)
		sortNode.Properties["keys"] = orderExpressionsToSpec(stmt.OrderBy)
		sortNode.AddChild(root)
		root = sortNode
	}

	if stmt.Limit != nil {
		limitNode := query.NewPlanNode(query.PlanNodeLimit)
		limitNode.Properties["count"] = *stmt.Limit
		limitNode.AddChild(root)
		root = limitNode
	}

	final := query.NewPlanNode(query.PlanNodeRoot)
	final.AddChild(root)
	return &query.PhysicalPlan{Root: final}, nil
}

func (p *Planner) buildFromTree(from []query.TableReference, tablePredicates map[string][]query.Expression) (*query.PlanNode, error) {
	var root *query.PlanNode
	for idx, tableRef := range from {
		subPlan, err := p.buildTableNode(tableRef, tablePredicates)
		if err != nil {
			return nil, err
		}
		if idx == 0 {
			root = subPlan
			continue
		}
		// Implicit CROSS JOIN caso o usuário tenha listado múltiplas tabelas separados por vírgula.
		root = buildJoinNode(root, subPlan, query.JoinTypeCross, nil)
	}
	return root, nil
}

func (p *Planner) buildTableNode(ref query.TableReference, tablePredicates map[string][]query.Expression) (*query.PlanNode, error) {
	schema, err := p.metadata.Table(ref.Name)
	if err != nil {
		return nil, err
	}
	scan := query.NewPlanNode(query.PlanNodeScan)
	scan.Properties["table"] = ref.Name
	alias := ref.Alias
	if alias == "" {
		alias = ref.Name
	}
	scan.Properties["alias"] = alias
	scan.Properties["columns"] = schema.ColumnNames()

	if preds, ok := tablePredicates[strings.ToLower(alias)]; ok && len(preds) > 0 {
		filter := query.NewPlanNode(query.PlanNodeFilter)
		filter.Properties["predicates"] = expressionsToStrings(preds)
		filter.AddChild(scan)
		scan = filter
	}

	root := scan
	for _, join := range ref.Joins {
		rightSchema, err := p.metadata.Table(join.Table)
		if err != nil {
			return nil, err
		}
		rightScan := query.NewPlanNode(query.PlanNodeScan)
		rightScan.Properties["table"] = join.Table
		alias := join.Alias
		if alias == "" {
			alias = join.Table
		}
		rightScan.Properties["alias"] = alias
		rightScan.Properties["columns"] = rightSchema.ColumnNames()

		if preds, ok := tablePredicates[strings.ToLower(alias)]; ok && len(preds) > 0 {
			filter := query.NewPlanNode(query.PlanNodeFilter)
			filter.Properties["predicates"] = expressionsToStrings(preds)
			filter.AddChild(rightScan)
			rightScan = filter
		}
		root = buildJoinNode(root, rightScan, join.Type, join.Condition)
	}
	return root, nil
}

func (p *Planner) splitPredicates(stmt *query.SelectStatement) (map[string][]query.Expression, []query.Expression) {
	result := map[string][]query.Expression{}
	global := []query.Expression{}
	if stmt.Where == nil {
		return result, global
	}
	conjuncts := splitConjuncts(stmt.Where)
	for _, predicate := range conjuncts {
		tables := referencedTables(predicate)
		if len(tables) == 1 {
			var alias string
			for tbl := range tables {
				alias = tbl
			}
			alias = strings.ToLower(alias)
			result[alias] = append(result[alias], predicate)
		} else {
			global = append(global, predicate)
		}
	}
	return result, global
}

func buildJoinNode(left, right *query.PlanNode, typ query.JoinType, cond query.Expression) *query.PlanNode {
	joinNode := query.NewPlanNode(query.PlanNodeJoin)
	joinNode.Properties["type"] = typ
	if cond != nil {
		joinNode.Properties["condition"] = cond.String()
	}
	joinNode.AddChild(left)
	joinNode.AddChild(right)
	return joinNode
}

func expressionsToStrings(exprs []query.Expression) []string {
	out := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		out = append(out, expr.String())
	}
	return out
}

func splitConjuncts(expr query.Expression) []query.Expression {
	if expr == nil {
		return nil
	}
	if bin, ok := expr.(query.BinaryExpr); ok && strings.EqualFold(bin.Operator, "AND") {
		left := splitConjuncts(bin.Left)
		right := splitConjuncts(bin.Right)
		return append(left, right...)
	}
	return []query.Expression{expr}
}

func referencedTables(expr query.Expression) map[string]struct{} {
	res := map[string]struct{}{}
	walkExpression(expr, func(e query.Expression) {
		if col, ok := e.(query.ColumnRef); ok {
			tbl := strings.ToLower(col.Table)
			if tbl != "" {
				res[tbl] = struct{}{}
			}
		}
	})
	return res
}

func walkExpression(expr query.Expression, fn func(query.Expression)) {
	if expr == nil {
		return
	}
	fn(expr)
	switch e := expr.(type) {
	case query.BinaryExpr:
		walkExpression(e.Left, fn)
		walkExpression(e.Right, fn)
	case query.UnaryExpr:
		walkExpression(e.Expr, fn)
	case query.BetweenExpr:
		walkExpression(e.Expr, fn)
		walkExpression(e.Lower, fn)
		walkExpression(e.Upper, fn)
	case query.FunctionCall:
		for _, arg := range e.Args {
			walkExpression(arg, fn)
		}
	}
}

func buildProjectionSpecs(items []query.SelectItem) []ProjectionSpec {
	result := make([]ProjectionSpec, 0, len(items))
	for _, item := range items {
		if item.Wildcard != nil {
			result = append(result, ProjectionSpec{Expr: item.Wildcard.String(), Alias: item.Alias, Wildcard: true})
			continue
		}
		result = append(result, ProjectionSpec{
			Expr:  item.Expr.String(),
			Alias: item.Alias,
		})
	}
	return result
}

func allWildcards(projections []ProjectionSpec) bool {
	if len(projections) == 0 {
		return false
	}
	for _, proj := range projections {
		if !proj.Wildcard {
			return false
		}
	}
	return true
}

func needsAggregation(stmt *query.SelectStatement) bool {
	if len(stmt.GroupBy) > 0 {
		return true
	}
	for _, item := range stmt.Columns {
		if _, ok := item.Expr.(query.FunctionCall); ok {
			return true
		}
	}
	return false
}

func (p *Planner) buildAggregation(child *query.PlanNode, stmt *query.SelectStatement) *query.PlanNode {
	localAgg := query.NewPlanNode(query.PlanNodeAggregate)
	localAgg.Properties["stage"] = "LOCAL"
	localAgg.Properties["groupKeys"] = expressionsToStrings(stmt.GroupBy)
	localAgg.Properties["aggregates"] = aggregateSpecs(stmt.Columns)
	localAgg.AddChild(child)

	exchange := query.NewPlanNode(query.PlanNodeExchange)
	exchange.Properties["mode"] = "SHUFFLE"
	exchange.AddChild(localAgg)

	globalAgg := query.NewPlanNode(query.PlanNodeAggregate)
	globalAgg.Properties["stage"] = "GLOBAL"
	globalAgg.Properties["groupKeys"] = expressionsToStrings(stmt.GroupBy)
	globalAgg.Properties["aggregates"] = aggregateSpecs(stmt.Columns)
	globalAgg.AddChild(exchange)
	return globalAgg
}

func aggregateSpecs(items []query.SelectItem) []AggregateSpec {
	result := []AggregateSpec{}
	for _, item := range items {
		fn, ok := item.Expr.(query.FunctionCall)
		if !ok {
			continue
		}
		spec := AggregateSpec{
			Func:     strings.ToUpper(fn.Name),
			Expr:     joinExpressions(fn.Args),
			Alias:    item.Alias,
			Distinct: fn.Distinct,
		}
		result = append(result, spec)
	}
	return result
}

func joinExpressions(exprs []query.Expression) string {
	strs := make([]string, 0, len(exprs))
	for _, e := range exprs {
		strs = append(strs, e.String())
	}
	return strings.Join(strs, ", ")
}

func orderExpressionsToSpec(items []query.OrderExpression) []SortSpec {
	result := make([]SortSpec, 0, len(items))
	for _, item := range items {
		result = append(result, SortSpec{
			Expr:      item.Expr.String(),
			Direction: item.Direction,
		})
	}
	return result
}

// ProjectionSpec descreve cada item de projeção para documentar a saída.
type ProjectionSpec struct {
	Expr     string `json:"expr"`
	Alias    string `json:"alias,omitempty"`
	Wildcard bool   `json:"wildcard,omitempty"`
}

// AggregateSpec explica a função agregadora escolhida.
type AggregateSpec struct {
	Func     string `json:"func"`
	Expr     string `json:"expr"`
	Alias    string `json:"alias,omitempty"`
	Distinct bool   `json:"distinct,omitempty"`
}

// SortSpec define a ordenação aplicada.
type SortSpec struct {
	Expr      string              `json:"expr"`
	Direction query.SortDirection `json:"direction"`
}
