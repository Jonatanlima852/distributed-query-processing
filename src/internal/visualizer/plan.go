package visualizer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Jonatan852/distributed-query-processing/pkg/query"
)

// PlanToJSON devolve o plano físico em formato JSON legível.
func PlanToJSON(plan *query.PhysicalPlan) ([]byte, error) {
	if plan == nil || plan.Root == nil {
		return nil, fmt.Errorf("plano vazio")
	}
	return json.MarshalIndent(plan, "", "  ")
}

// PlanToDOT gera um grafo DOT simples para usar com Graphviz.
func PlanToDOT(plan *query.PhysicalPlan) (string, error) {
	if plan == nil || plan.Root == nil {
		return "", fmt.Errorf("plano vazio")
	}
	var buf bytes.Buffer
	buf.WriteString("digraph Plan {\n")
	buf.WriteString("  rankdir=TB;\n")
	traverse(plan.Root, func(node *query.PlanNode) {
		label := fmt.Sprintf("%s\\n%s", node.ID, node.Type)
		if len(node.Properties) > 0 {
			label = fmt.Sprintf("%s\\n%s", label, formatProperties(node.Properties))
		}
		buf.WriteString(fmt.Sprintf("  \"%s\" [label=\"%s\", shape=box];\n", node.ID, label))
		for _, child := range node.Children {
			buf.WriteString(fmt.Sprintf("  \"%s\" -> \"%s\";\n", node.ID, child.ID))
		}
	})
	buf.WriteString("}\n")
	return buf.String(), nil
}

func traverse(node *query.PlanNode, fn func(*query.PlanNode)) {
	if node == nil {
		return
	}
	fn(node)
	for _, child := range node.Children {
		traverse(child, fn)
	}
}

func formatProperties(props map[string]interface{}) string {
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", key, props[key]))
	}
	return strings.Join(parts, "\\n")
}
