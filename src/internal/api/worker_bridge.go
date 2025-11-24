package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
	"time"

	"github.com/Jonatan852/distributed-query-processing/internal/distributed"
)

// workerBridge encapsula o protocolo long-poll usado pelos workers remotos.
type workerBridge struct {
	id       string
	secret   string
	taskCh   chan distributed.TaskRequest
	resultCh chan distributed.TaskResult
	lastBeat atomicPointerTime
	timeout  time.Duration
}

func newWorkerBridge(id string, timeout time.Duration) *workerBridge {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	return &workerBridge{
		id:       id,
		secret:   randomSecret(),
		taskCh:   make(chan distributed.TaskRequest),
		resultCh: make(chan distributed.TaskResult),
		timeout:  timeout,
	}
}

func (w *workerBridge) ID() string {
	return w.id
}

func (w *workerBridge) Heartbeat() time.Time {
	return w.lastBeat.Load()
}

func (w *workerBridge) Execute(task distributed.TaskRequest) distributed.TaskResult {
	select {
	case w.taskCh <- task:
	case <-time.After(w.timeout):
		return distributed.TaskResult{
			TaskID:   task.TaskID,
			WorkerID: w.id,
			Error:    "timeout enviando task para worker",
		}
	}
	select {
	case result := <-w.resultCh:
		if result.TaskID == "" {
			result.TaskID = task.TaskID
		}
		if result.WorkerID == "" {
			result.WorkerID = w.id
		}
		return result
	case <-time.After(w.timeout):
		return distributed.TaskResult{
			TaskID:   task.TaskID,
			WorkerID: w.id,
			Error:    "timeout aguardando resultado do worker",
		}
	}
}

func (w *workerBridge) waitTask(ctx context.Context) (distributed.TaskRequest, bool) {
	select {
	case task := <-w.taskCh:
		return task, true
	case <-ctx.Done():
		return distributed.TaskRequest{}, false
	}
}

func (w *workerBridge) deliverResult(result distributed.TaskResult) bool {
	select {
	case w.resultCh <- result:
		return true
	case <-time.After(5 * time.Second):
		return false
	}
}

func (w *workerBridge) validateSecret(secret string) bool {
	return secret == w.secret
}

func (w *workerBridge) updateHeartbeat() {
	w.lastBeat.Store(time.Now())
}

func randomSecret() string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}

type atomicPointerTime struct {
	value atomic.Value
}

func (a *atomicPointerTime) Load() time.Time {
	if v := a.value.Load(); v != nil {
		if t, ok := v.(time.Time); ok {
			return t
		}
	}
	return time.Time{}
}

func (a *atomicPointerTime) Store(t time.Time) {
	a.value.Store(t)
}
