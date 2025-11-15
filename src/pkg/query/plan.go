package query

import (
	"fmt"
	"sync/atomic"
)

// PlanNodeType identifica o operador físico executado.
type PlanNodeType string

const (
	PlanNodeScan      PlanNodeType = "SCAN"
	PlanNodeFilter    PlanNodeType = "FILTER"
	PlanNodeProject   PlanNodeType = "PROJECT"
	PlanNodeAggregate PlanNodeType = "AGGREGATE"
	PlanNodeExchange  PlanNodeType = "EXCHANGE"
	PlanNodeJoin      PlanNodeType = "JOIN"
	PlanNodeSort      PlanNodeType = "SORT"
	PlanNodeLimit     PlanNodeType = "LIMIT"
	PlanNodeRoot      PlanNodeType = "ROOT"
	PlanNodeUnknown   PlanNodeType = "UNKNOWN"
)

var planNodeCounter atomic.Int64

// PhysicalPlan representa a árvore de operadores distribuídos gerada pelo planner.
type PhysicalPlan struct {
	Root *PlanNode
}

// PlanNode é um nó simples com filhos e propriedades específicas do operador.
type PlanNode struct {
	ID         string                 `json:"id"`
	Type       PlanNodeType           `json:"type"`
	Children   []*PlanNode            `json:"children,omitempty"`
	Properties map[string]interface{} `json:"properties,omitempty"`
	Stats      map[string]interface{} `json:"stats,omitempty"`
}

// NewPlanNode cria um nó com ID único e tipo informado.
func NewPlanNode(typ PlanNodeType) *PlanNode {
	id := planNodeCounter.Add(1)
	return &PlanNode{
		ID:         fmt.Sprintf("node-%03d", id),
		Type:       typ,
		Children:   []*PlanNode{},
		Properties: map[string]interface{}{},
		Stats:      map[string]interface{}{},
	}
}

// AddChild anexa um filho à lista do nó.
func (n *PlanNode) AddChild(child *PlanNode) {
	n.Children = append(n.Children, child)
}
