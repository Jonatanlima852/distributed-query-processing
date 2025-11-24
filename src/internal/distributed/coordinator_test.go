package distributed

import (
	"testing"
	"time"

	"github.com/Jonatan852/distributed-query-processing/pkg/query"
)

func TestCoordinatorSubmitAndStatus(t *testing.T) {
	root := query.NewPlanNode(query.PlanNodeRoot)
	scan := query.NewPlanNode(query.PlanNodeScan)
	root.AddChild(scan)
	plan := &query.PhysicalPlan{Root: root}

	coord := NewCoordinator()
	worker := NewLocalWorker("worker-1", func(req TaskRequest) TaskResult {
		time.Sleep(10 * time.Millisecond)
		return TaskResult{Rows: 10, Duration: 10 * time.Millisecond}
	})
	coord.Register(worker)

	id, err := coord.Submit(plan)
	if err != nil {
		t.Fatalf("submit falhou: %v", err)
	}

	waitForStatus(t, coord, id, StatusSuccess, 2*time.Second)
	results, err := coord.QueryResults(id)
	if err != nil {
		t.Fatalf("erro consultando resultados: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("esperava 1 task, obteve %d", len(results))
	}
}

func TestCoordinatorHandlesWorkerError(t *testing.T) {
	root := query.NewPlanNode(query.PlanNodeRoot)
	scan := query.NewPlanNode(query.PlanNodeScan)
	root.AddChild(scan)
	plan := &query.PhysicalPlan{Root: root}

	coord := NewCoordinator()
	worker := NewLocalWorker("w-err", func(req TaskRequest) TaskResult {
		return TaskResult{Error: "falha simulada"}
	})
	coord.Register(worker)

	id, err := coord.Submit(plan)
	if err != nil {
		t.Fatalf("submit falhou: %v", err)
	}
	waitForStatus(t, coord, id, StatusFailed, 2*time.Second)
}

func waitForStatus(t *testing.T, coord *Coordinator, id string, desired QueryStatus, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		status, err := coord.QueryStatus(id)
		if err != nil {
			t.Fatalf("falha consultando status: %v", err)
		}
		if status == desired {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("status não atingiu %s dentro do timeout", desired)
}
