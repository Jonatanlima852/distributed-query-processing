package distributed

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jonatan852/distributed-query-processing/pkg/query"
)

// Coordinator gerencia workers e execução de planos distribuídos.
type Coordinator struct {
	mu       sync.Mutex
	workers  map[string]WorkerClient
	queries  map[string]*queryState
	taskSeq  int64
	querySeq int64
}

type queryState struct {
	ID          string
	Status      QueryStatus
	Plan        *query.PhysicalPlan
	Results     []TaskResult
	Error       error
	SubmittedAt time.Time
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		workers: map[string]WorkerClient{},
		queries: map[string]*queryState{},
	}
}

// Register adiciona/atualiza um worker disponível.
func (c *Coordinator) Register(worker WorkerClient) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workers[worker.ID()] = worker
}

// Deregister remove workers inativos.
func (c *Coordinator) Deregister(workerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.workers, workerID)
}

// Submit inicia a execução distribuída.
func (c *Coordinator) Submit(plan *query.PhysicalPlan) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.workers) == 0 {
		return "", errors.New("nenhum worker registrado")
	}
	if plan == nil || plan.Root == nil {
		return "", fmt.Errorf("plano inválido")
	}
	c.querySeq++
	id := fmt.Sprintf("q-%04d", c.querySeq)
	state := &queryState{
		ID:          id,
		Status:      StatusPending,
		Plan:        plan,
		SubmittedAt: time.Now(),
	}
	c.queries[id] = state
	go c.execute(state)
	return id, nil
}

func (c *Coordinator) execute(state *queryState) {
	state.Status = StatusRunning
	fragments := collectFragments(state.Plan.Root)
	workers := c.snapshotWorkers()
	if len(workers) == 0 {
		state.Status = StatusFailed
		state.Error = errors.New("nenhum worker disponível")
		return
	}
	var wg sync.WaitGroup
	results := make([]TaskResult, len(fragments))
	for i, fragment := range fragments {
		wg.Add(1)
		worker := workers[i%len(workers)]
		taskID := fmt.Sprintf("%s-task-%d", state.ID, i+1)
		req := TaskRequest{
			QueryID:  state.ID,
			TaskID:   taskID,
			Fragment: fragment,
		}
		go func(idx int, w WorkerClient, tr TaskRequest) {
			defer wg.Done()
			results[idx] = w.Execute(tr)
		}(i, worker, req)
	}
	wg.Wait()
	state.Results = results
	for _, res := range results {
		if res.Error != "" {
			state.Status = StatusFailed
			state.Error = errors.New(res.Error)
			return
		}
	}
	state.Status = StatusSuccess
}

func (c *Coordinator) snapshotWorkers() []WorkerClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	list := make([]WorkerClient, 0, len(c.workers))
	for _, worker := range c.workers {
		list = append(list, worker)
	}
	return list
}

// QueryStatus retorna o status atual de uma query enviada.
func (c *Coordinator) QueryStatus(id string) (QueryStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, ok := c.queries[id]
	if !ok {
		return "", fmt.Errorf("query %s não encontrada", id)
	}
	return state.Status, nil
}

// QueryResults devolve o detalhamento dos tasks.
func (c *Coordinator) QueryResults(id string) ([]TaskResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, ok := c.queries[id]
	if !ok {
		return nil, fmt.Errorf("query %s não encontrada", id)
	}
	return state.Results, nil
}

// QueryPlan devolve o plano físico utilizado na execução.
func (c *Coordinator) QueryPlan(id string) (*query.PhysicalPlan, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, ok := c.queries[id]
	if !ok {
		return nil, fmt.Errorf("query %s não encontrada", id)
	}
	return state.Plan, nil
}

func collectFragments(node *query.PlanNode) []*query.PlanNode {
	if node == nil {
		return nil
	}
	var fragments []*query.PlanNode
	if node.Type == query.PlanNodeScan {
		fragments = append(fragments, node)
	}
	for _, child := range node.Children {
		fragments = append(fragments, collectFragments(child)...)
	}
	return fragments
}
