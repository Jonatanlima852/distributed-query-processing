package distributed

import (
	"time"

	"github.com/Jonatan852/distributed-query-processing/pkg/query"
)

// QueryStatus representa o ciclo de vida de uma query distribuída.
type QueryStatus string

const (
	StatusPending QueryStatus = "PENDING"
	StatusRunning QueryStatus = "RUNNING"
	StatusSuccess QueryStatus = "SUCCESS"
	StatusFailed  QueryStatus = "FAILED"
)

// TaskRequest contém a fatia do plano que um worker deve executar.
type TaskRequest struct {
	QueryID  string
	TaskID   string
	Fragment *query.PlanNode
}

// TaskResult descreve métricas e possíveis erros de um task executado pelo worker.
type TaskResult struct {
	TaskID   string        `json:"taskId"`
	WorkerID string        `json:"workerId"`
	Rows     int           `json:"rows"`
	Duration time.Duration `json:"duration"`
	Error    string        `json:"error,omitempty"`
}

// WorkerClient representa um worker conectado ao coordinator.
type WorkerClient interface {
	ID() string
	Heartbeat() time.Time
	Execute(TaskRequest) TaskResult
}
