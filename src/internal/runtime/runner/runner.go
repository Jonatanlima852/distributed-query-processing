package runner

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Jonatan852/distributed-query-processing/internal/storage"
	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
	"github.com/Jonatan852/distributed-query-processing/pkg/query"
)

// Runner executa consultas SELECT diretamente sobre o storage local.
type Runner struct {
	engine *storage.Engine
}

// New cria um novo runner usando o storage informado.
func New(engine *storage.Engine) *Runner {
	return &Runner{engine: engine}
}

// Execute processa um SelectStatement e retorna linhas em formato map[string]interface{}.
func (r *Runner) Execute(stmt *query.SelectStatement) ([]map[string]interface{}, error) {
	if stmt == nil {
		return nil, fmt.Errorf("runner: statement vazio")
	}
	if len(stmt.From) != 1 {
		return nil, fmt.Errorf("runner: apenas uma tabela é suportada neste MVP")
	}
	if len(stmt.GroupBy) > 0 {
		return nil, fmt.Errorf("runner: GROUP BY ainda não suportado")
	}
	for _, item := range stmt.Columns {
		if _, ok := item.Expr.(query.FunctionCall); ok {
			return nil, fmt.Errorf("runner: funções agregadas ainda não suportadas")
		}
	}

	tableRef := stmt.From[0]
	tableName := tableRef.Name
	schema, err := r.engine.Table(tableName)
	if err != nil {
		return nil, err
	}
	columns := schema.ColumnNames()

	batches, err := r.engine.Scan(tableName, storage.ScanOptions{Columns: columns})
	if err != nil {
		return nil, err
	}
	alias := tableRef.Alias
	if alias == "" {
		alias = tableName
	}

	var rows []map[string]interface{}
	for _, batch := range batches {
		for i := 0; i < batch.RowCount; i++ {
			ctx, err := newRowContext(batch.Columns, columns, i, alias)
			if err != nil {
				return nil, err
			}
			pass, err := evaluateBoolean(stmt.Where, ctx, alias)
			if err != nil {
				return nil, err
			}
			if !pass {
				continue
			}
			record, err := buildProjection(stmt.Columns, ctx)
			if err != nil {
				return nil, err
			}
			rows = append(rows, record)
			if stmt.Limit != nil && int64(len(rows)) >= *stmt.Limit {
				return applyOrder(rows, stmt.OrderBy), nil
			}
		}
	}
	rows = applyOrder(rows, stmt.OrderBy)
	return rows, nil
}

func applyOrder(rows []map[string]interface{}, order []query.OrderExpression) []map[string]interface{} {
	if len(order) == 0 || len(rows) <= 1 {
		return rows
	}
	sort.SliceStable(rows, func(i, j int) bool {
		for _, item := range order {
			col, ok := item.Expr.(query.ColumnRef)
			if !ok {
				continue
			}
			l := rows[i][outputColumnName(col)]
			r := rows[j][outputColumnName(col)]
			cmp := compareInterfaces(l, r)
			if cmp == 0 {
				continue
			}
			if item.Direction == query.SortDesc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
	return rows
}

func compareInterfaces(left, right interface{}) int {
	switch l := left.(type) {
	case int64:
		r := right.(int64)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case float64:
		r := right.(float64)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case string:
		r := right.(string)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case bool:
		r := right.(bool)
		if l == r {
			return 0
		}
		if !l && r {
			return -1
		}
		return 1
	default:
		return 0
	}
}

func buildProjection(items []query.SelectItem, ctx rowContext) (map[string]interface{}, error) {
	result := map[string]interface{}{}
	if len(items) == 0 {
		items = append(items, query.SelectItem{Wildcard: &query.Wildcard{}})
	}
	for _, item := range items {
		if item.Wildcard != nil {
			for _, name := range ctx.order {
				result[name] = valueToInterface(ctx.values[strings.ToLower(name)])
			}
			continue
		}
		colRef, ok := item.Expr.(query.ColumnRef)
		if !ok {
			return nil, fmt.Errorf("runner: apenas projeções de colunas são suportadas no momento")
		}
		value, err := ctx.getColumn(colRef)
		if err != nil {
			return nil, err
		}
		key := item.Alias
		if key == "" {
			key = outputColumnName(colRef)
		}
		result[key] = valueToInterface(value)
	}
	return result, nil
}

func outputColumnName(col query.ColumnRef) string {
	if col.Name != "" {
		return col.Name
	}
	return col.String()
}

func valueToInterface(v columnar.Value) interface{} {
	switch v.Type {
	case columnar.TypeInt:
		i, _ := v.AsInt()
		return i
	case columnar.TypeFloat:
		f, _ := v.AsFloat()
		return f
	case columnar.TypeString:
		s, _ := v.AsString()
		return s
	case columnar.TypeBool:
		b, _ := v.AsBool()
		return b
	default:
		return nil
	}
}

type rowContext struct {
	values map[string]columnar.Value
	order  []string
	alias  string
}

func newRowContext(cols map[string]*columnar.Column, order []string, index int, alias string) (rowContext, error) {
	values := make(map[string]columnar.Value, len(order))
	for _, name := range order {
		col := cols[name]
		if col == nil {
			continue
		}
		val, err := col.Get(index)
		if err != nil {
			return rowContext{}, err
		}
		values[strings.ToLower(name)] = val
	}
	return rowContext{values: values, order: order, alias: strings.ToLower(alias)}, nil
}

func (rc rowContext) getColumn(col query.ColumnRef) (columnar.Value, error) {
	if col.Table != "" && !strings.EqualFold(col.Table, rc.alias) {
		return columnar.Value{}, fmt.Errorf("tabela %s não disponível nesta linha", col.Table)
	}
	val, ok := rc.values[strings.ToLower(col.Name)]
	if !ok {
		return columnar.Value{}, fmt.Errorf("coluna %s não encontrada", col.Name)
	}
	return val, nil
}

func evaluateBoolean(expr query.Expression, ctx rowContext, alias string) (bool, error) {
	if expr == nil {
		return true, nil
	}
	switch e := expr.(type) {
	case query.BinaryExpr:
		op := strings.ToUpper(e.Operator)
		switch op {
		case "AND":
			left, err := evaluateBoolean(e.Left, ctx, alias)
			if err != nil {
				return false, err
			}
			if !left {
				return false, nil
			}
			return evaluateBoolean(e.Right, ctx, alias)
		case "OR":
			left, err := evaluateBoolean(e.Left, ctx, alias)
			if err != nil {
				return false, err
			}
			if left {
				return true, nil
			}
			return evaluateBoolean(e.Right, ctx, alias)
		default:
			leftVal, err := evaluateValue(e.Left, ctx, alias)
			if err != nil {
				return false, err
			}
			rightVal, err := evaluateValue(e.Right, ctx, alias)
			if err != nil {
				return false, err
			}
			cmp, err := compareValues(leftVal, rightVal)
			if err != nil {
				return false, err
			}
			switch op {
			case "=", "==":
				return cmp == 0, nil
			case "!=", "<>":
				return cmp != 0, nil
			case "<":
				return cmp < 0, nil
			case "<=":
				return cmp <= 0, nil
			case ">":
				return cmp > 0, nil
			case ">=":
				return cmp >= 0, nil
			default:
				return false, fmt.Errorf("operador %s não suportado", e.Operator)
			}
		}
	case query.UnaryExpr:
		if strings.EqualFold(e.Operator, "NOT") {
			val, err := evaluateBoolean(e.Expr, ctx, alias)
			return !val, err
		}
		return false, fmt.Errorf("operador unário %s não suportado", e.Operator)
	default:
		val, err := evaluateValue(expr, ctx, alias)
		if err != nil {
			return false, err
		}
		boolVal, err := valueToBool(val)
		return boolVal, err
	}
}

func evaluateValue(expr query.Expression, ctx rowContext, alias string) (columnar.Value, error) {
	switch e := expr.(type) {
	case query.ColumnRef:
		return ctx.getColumn(e)
	case query.Literal:
		return e.Value, nil
	case query.BinaryExpr:
		return columnar.Value{}, fmt.Errorf("expressões aritméticas ainda não suportadas")
	default:
		return columnar.Value{}, fmt.Errorf("expressão %T não suportada", expr)
	}
}

func compareValues(left, right columnar.Value) (int, error) {
	if left.Type == right.Type {
		switch left.Type {
		case columnar.TypeInt:
			l, _ := left.AsInt()
			r, _ := right.AsInt()
			switch {
			case l < r:
				return -1, nil
			case l > r:
				return 1, nil
			default:
				return 0, nil
			}
		case columnar.TypeFloat:
			l, _ := left.AsFloat()
			r, _ := right.AsFloat()
			switch {
			case l < r:
				return -1, nil
			case l > r:
				return 1, nil
			default:
				return 0, nil
			}
		case columnar.TypeString:
			l, _ := left.AsString()
			r, _ := right.AsString()
			switch {
			case l < r:
				return -1, nil
			case l > r:
				return 1, nil
			default:
				return 0, nil
			}
		case columnar.TypeBool:
			l, _ := left.AsBool()
			r, _ := right.AsBool()
			if l == r {
				return 0, nil
			}
			if !l && r {
				return -1, nil
			}
			return 1, nil
		default:
			return 0, fmt.Errorf("tipo %v não suportado em comparação", left.Type)
		}
	}
	// Comparar INT com FLOAT convertendo para float64
	if left.Type == columnar.TypeInt && right.Type == columnar.TypeFloat {
		l, _ := left.AsInt()
		r, _ := right.AsFloat()
		return compareFloat(float64(l), r), nil
	}
	if left.Type == columnar.TypeFloat && right.Type == columnar.TypeInt {
		l, _ := left.AsFloat()
		r, _ := right.AsInt()
		return compareFloat(l, float64(r)), nil
	}
	return 0, fmt.Errorf("tipos incompatíveis (%v vs %v)", left.Type, right.Type)
}

func compareFloat(left, right float64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func valueToBool(value columnar.Value) (bool, error) {
	switch value.Type {
	case columnar.TypeBool:
		return value.AsBool()
	default:
		return false, fmt.Errorf("valor %v não pode ser interpretado como boolean", value.Type)
	}
}
